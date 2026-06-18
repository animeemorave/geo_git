#include "server/conversions.h"
#include "server/service_impl.h"

#include <exception>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace geogit_server {

grpc::Status GeoGitServiceImpl::CreateVersion(grpc::ServerContext* /*context*/,
                                              const geogit::CreateVersionRequest* request,
                                              geogit::VersionResponse* response) {
    std::lock_guard<std::mutex> lock(mutex_);
    try {
        std::unordered_map<std::string, std::string> objects;
        for (const auto& entry : request->objects()) {
            objects.emplace(entry.first, entry.second);
        }
        std::vector<std::string> parents(request->parent_version_ids().begin(),
                                         request->parent_version_ids().end());

        std::string id = versions_.create_version(request->situation_id(), objects, parents,
                                                  request->message(), request->author());
        auto version = versions_.get_version(id);
        if (!version) {
            return grpc::Status(grpc::StatusCode::INTERNAL, "Created version not found");
        }
        fill_version(*version, response->mutable_version());
        return grpc::Status::OK;
    } catch (const std::exception& e) {
        return grpc::Status(grpc::StatusCode::INTERNAL, e.what());
    }
}

grpc::Status GeoGitServiceImpl::GetVersion(grpc::ServerContext* /*context*/,
                                           const geogit::GetVersionRequest* request,
                                           geogit::VersionResponse* response) {
    std::lock_guard<std::mutex> lock(mutex_);
    try {
        auto version = versions_.get_version(request->version_id());
        if (!version) {
            return grpc::Status(grpc::StatusCode::NOT_FOUND, "Version not found");
        }
        fill_version(*version, response->mutable_version());
        return grpc::Status::OK;
    } catch (const std::exception& e) {
        return grpc::Status(grpc::StatusCode::INTERNAL, e.what());
    }
}

grpc::Status GeoGitServiceImpl::ListVersions(grpc::ServerContext* /*context*/,
                                             const geogit::ListVersionsRequest* request,
                                             geogit::ListVersionsResponse* response) {
    std::lock_guard<std::mutex> lock(mutex_);
    try {
        for (const auto& meta : versions_.list_versions(request->situation_id())) {
            fill_version_meta(meta, response->add_versions());
        }
        return grpc::Status::OK;
    } catch (const std::exception& e) {
        return grpc::Status(grpc::StatusCode::INTERNAL, e.what());
    }
}

grpc::Status GeoGitServiceImpl::Checkout(grpc::ServerContext* /*context*/,
                                         const geogit::CheckoutRequest* request,
                                         grpc::ServerWriter<geogit::BPOResponse>* writer) {
    std::lock_guard<std::mutex> lock(mutex_);
    try {
        for (const auto& bpo : versions_.checkout(request->version_id())) {
            geogit::BPOResponse response;
            fill_bpo(bpo, response.mutable_bpo());
            writer->Write(response);
        }
        return grpc::Status::OK;
    } catch (const std::exception& e) {
        return grpc::Status(grpc::StatusCode::INTERNAL, e.what());
    }
}

} // namespace geogit_server
