#include "server/conversions.h"
#include "server/service_impl.h"

#include "storage/bpo_storage/bpo_storage.h"
#include "storage/cas/cas.h"

#include <bsoncxx/document/value.hpp>
#include <bsoncxx/json.hpp>

#include <exception>
#include <mutex>
#include <string>

namespace geogit_server {

grpc::Status GeoGitServiceImpl::StoreBPO(grpc::ServerContext* /*context*/,
                                         const geogit::StoreBPORequest* request,
                                         geogit::StoreBPOResponse* response) {
    std::lock_guard<std::mutex> lock(mutex_);

    bsoncxx::document::value geometry = bsoncxx::from_json("{}");
    bsoncxx::document::value attributes = bsoncxx::from_json("{}");
    try {
        geometry = bsoncxx::from_json(request->geometry_json());
        std::string attributes_json =
            request->attributes_json().empty() ? "{}" : request->attributes_json();
        attributes = bsoncxx::from_json(attributes_json);
    } catch (const std::exception& e) {
        return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                            std::string("Invalid GeoJSON: ") + e.what());
    }

    try {
        geoversion::storage::BPO bpo("", geometry.view(), attributes.view());
        std::string hash = cas_.compute_hash(bpo);
        bpo.set_hash(hash);
        cas_.store(bpo);
        response->set_hash(hash);
        return grpc::Status::OK;
    } catch (const std::exception& e) {
        return grpc::Status(grpc::StatusCode::INTERNAL, e.what());
    }
}

grpc::Status GeoGitServiceImpl::GetBPO(grpc::ServerContext* /*context*/,
                                       const geogit::GetBPORequest* request,
                                       geogit::BPOResponse* response) {
    std::lock_guard<std::mutex> lock(mutex_);
    try {
        auto bpo = cas_.retrieve(request->hash());
        if (!bpo) {
            return grpc::Status(grpc::StatusCode::NOT_FOUND, "BPO not found");
        }
        fill_bpo(*bpo, response->mutable_bpo());
        return grpc::Status::OK;
    } catch (const std::exception& e) {
        return grpc::Status(grpc::StatusCode::INTERNAL, e.what());
    }
}

grpc::Status GeoGitServiceImpl::FindInBBox(grpc::ServerContext* /*context*/,
                                           const geogit::FindInBBoxRequest* request,
                                           grpc::ServerWriter<geogit::BPOResponse>* writer) {
    std::lock_guard<std::mutex> lock(mutex_);
    try {
        auto results = cas_.find_in_bbox(request->min_lon(), request->min_lat(), request->max_lon(),
                                         request->max_lat());
        for (const auto& bpo : results) {
            geogit::BPOResponse response;
            fill_bpo(*bpo, response.mutable_bpo());
            writer->Write(response);
        }
        return grpc::Status::OK;
    } catch (const std::exception& e) {
        return grpc::Status(grpc::StatusCode::INTERNAL, e.what());
    }
}

} // namespace geogit_server
