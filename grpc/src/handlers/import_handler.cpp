#include "server/conversions.h"
#include "server/service_impl.h"

#include "storage/bpo_storage/bpo_storage.h"
#include "storage/cas/cas.h"
#include "utils/uuid/uuid.h"

#include <bsoncxx/builder/basic/array.hpp>
#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/builder/basic/kvp.hpp>
#include <bsoncxx/document/value.hpp>
#include <bsoncxx/json.hpp>
#include <bsoncxx/types.hpp>

#include <exception>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace geogit_server {

namespace {

using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::make_document;
using bsoncxx::builder::basic::sub_document;

std::string element_to_string(const bsoncxx::document::element& element) {
    switch (element.type()) {
        case bsoncxx::type::k_string:
            return std::string(element.get_string().value);
        case bsoncxx::type::k_int32:
            return std::to_string(element.get_int32().value);
        case bsoncxx::type::k_int64:
            return std::to_string(element.get_int64().value);
        case bsoncxx::type::k_double:
            return std::to_string(element.get_double().value);
        default:
            return "";
    }
}

std::string derive_object_id(const bsoncxx::document::view& feature, bool generate) {
    if (!generate) {
        if (feature["id"]) {
            std::string id = element_to_string(feature["id"]);
            if (!id.empty()) {
                return id;
            }
        }
        if (feature["properties"] && feature["properties"].type() == bsoncxx::type::k_document) {
            auto properties = feature["properties"].get_document().view();
            if (properties["id"]) {
                std::string id = element_to_string(properties["id"]);
                if (!id.empty()) {
                    return id;
                }
            }
        }
    }
    return geoversion::utils::generate_uuid();
}

} // namespace

grpc::Status GeoGitServiceImpl::ImportFeatures(grpc::ServerContext* /*context*/,
                                               const geogit::ImportFeaturesRequest* request,
                                               geogit::VersionResponse* response) {
    std::lock_guard<std::mutex> lock(mutex_);

    bsoncxx::document::value parsed = make_document();
    try {
        parsed = bsoncxx::from_json(request->geojson());
    } catch (const std::exception& e) {
        return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                            std::string("Invalid GeoJSON: ") + e.what());
    }

    auto view = parsed.view();
    if (!view["features"] || view["features"].type() != bsoncxx::type::k_array) {
        return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                            "GeoJSON must be a FeatureCollection with a 'features' array");
    }

    try {
        std::unordered_map<std::string, std::string> objects;
        std::vector<geoversion::storage::BPO> batch;
        const std::size_t flush_at = 10000;
        auto flush = [&]() {
            if (!batch.empty()) {
                cas_.store_many(batch);
                batch.clear();
            }
        };

        for (auto&& element : view["features"].get_array().value) {
            if (element.type() != bsoncxx::type::k_document) {
                continue;
            }
            auto feature = element.get_document().value;

            if (!feature["geometry"] || feature["geometry"].type() != bsoncxx::type::k_document) {
                continue;
            }
            auto geometry = feature["geometry"].get_document().value;

            bsoncxx::document::value empty_properties = make_document();
            bsoncxx::document::view properties = empty_properties.view();
            if (feature["properties"] &&
                feature["properties"].type() == bsoncxx::type::k_document) {
                properties = feature["properties"].get_document().value;
            }

            std::string object_id = derive_object_id(feature, request->generate_object_ids());

            geoversion::storage::BPO bpo("", geometry, properties);
            std::string hash = cas_.compute_hash(bpo);
            bpo.set_hash(hash);
            objects[object_id] = hash;
            batch.push_back(std::move(bpo));
            if (batch.size() >= flush_at) {
                flush();
            }
        }
        flush();

        std::vector<std::string> parents(request->parent_version_ids().begin(),
                                         request->parent_version_ids().end());
        std::string version_id = versions_.create_version(request->situation_id(), objects, parents,
                                                          request->message(), request->author());

        if (!request->advance_branch_id().empty()) {
            branches_.advance(request->advance_branch_id(), version_id);
        }

        auto version = versions_.get_version(version_id);
        if (!version) {
            return grpc::Status(grpc::StatusCode::INTERNAL, "Created version not found");
        }
        fill_version(*version, response->mutable_version());
        return grpc::Status::OK;
    } catch (const std::exception& e) {
        return grpc::Status(grpc::StatusCode::INTERNAL, e.what());
    }
}

grpc::Status GeoGitServiceImpl::ExportFeatures(grpc::ServerContext* /*context*/,
                                               const geogit::ExportFeaturesRequest* request,
                                               geogit::ExportFeaturesResponse* response) {
    std::lock_guard<std::mutex> lock(mutex_);
    try {
        auto objects = versions_.checkout(request->version_id());

        bsoncxx::builder::basic::array features;
        for (const auto& bpo : objects) {
            features.append([&](sub_document feature) {
                feature.append(kvp("type", "Feature"));
                auto object_id = bpo.get_object_id();
                if (object_id) {
                    feature.append(kvp("id", *object_id));
                }
                feature.append(kvp("geometry", bsoncxx::types::b_document{bpo.get_geometry()}));
                feature.append(kvp("properties", bsoncxx::types::b_document{bpo.get_attributes()}));
            });
        }

        auto collection =
            make_document(kvp("type", "FeatureCollection"), kvp("features", features));
        response->set_geojson(bsoncxx::to_json(collection.view()));
        return grpc::Status::OK;
    } catch (const std::exception& e) {
        return grpc::Status(grpc::StatusCode::INTERNAL, e.what());
    }
}

} // namespace geogit_server
