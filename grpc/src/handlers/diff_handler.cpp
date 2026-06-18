#include "server/conversions.h"
#include "server/service_impl.h"

#include "diff/diff_engine.h"
#include "diff/status_engine.h"
#include "storage/delta_storage/delta_storage.h"

#include <exception>
#include <mutex>
#include <string>
#include <unordered_map>

namespace geogit_server {

grpc::Status GeoGitServiceImpl::ComputeDiff(grpc::ServerContext* /*context*/,
                                            const geogit::ComputeDiffRequest* request,
                                            geogit::DiffResponse* response) {
    std::lock_guard<std::mutex> lock(mutex_);
    try {
        geoversion::diff::DiffEngine engine(versions_, deltas_);
        if (request->entity_resolution()) {
            engine.set_entity_matcher(cas_, matcher_);
        }
        auto result = engine.compute(request->from_version_id(), request->to_version_id());
        fill_diff(result, response);
        return grpc::Status::OK;
    } catch (const std::exception& e) {
        return grpc::Status(grpc::StatusCode::NOT_FOUND, e.what());
    }
}

grpc::Status GeoGitServiceImpl::GetStatus(grpc::ServerContext* /*context*/,
                                          const geogit::GetStatusRequest* request,
                                          geogit::DiffResponse* response) {
    std::lock_guard<std::mutex> lock(mutex_);
    try {
        geoversion::diff::StatusEngine engine(branches_, versions_);
        std::unordered_map<std::string, std::string> proposed;
        for (const auto& entry : request->proposed_objects()) {
            proposed.emplace(entry.first, entry.second);
        }
        auto result = engine.compute_status(request->branch_id(), proposed);
        fill_diff(result, response);
        return grpc::Status::OK;
    } catch (const std::exception& e) {
        return grpc::Status(grpc::StatusCode::NOT_FOUND, e.what());
    }
}

grpc::Status GeoGitServiceImpl::GetDelta(grpc::ServerContext* /*context*/,
                                         const geogit::GetDeltaRequest* request,
                                         geogit::DiffResponse* response) {
    std::lock_guard<std::mutex> lock(mutex_);
    try {
        auto delta = deltas_.find(request->from_version_id(), request->to_version_id());
        if (!delta) {
            return grpc::Status(grpc::StatusCode::NOT_FOUND, "Delta not cached");
        }
        fill_diff(*delta, response);
        return grpc::Status::OK;
    } catch (const std::exception& e) {
        return grpc::Status(grpc::StatusCode::INTERNAL, e.what());
    }
}

} // namespace geogit_server
