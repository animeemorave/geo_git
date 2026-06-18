#include "server/service_impl.h"

#include "merge/merge_engine.h"

#include <exception>
#include <mutex>

namespace geogit_server {

namespace {

geoversion::merge::MergeStrategy to_strategy(geogit::MergeStrategy strategy) {
    switch (strategy) {
        case geogit::OURS:
            return geoversion::merge::MergeStrategy::Ours;
        case geogit::THEIRS:
            return geoversion::merge::MergeStrategy::Theirs;
        default:
            return geoversion::merge::MergeStrategy::Manual;
    }
}

geogit::ConflictType to_conflict_type(geoversion::merge::ConflictType type) {
    switch (type) {
        case geoversion::merge::ConflictType::ModifiedDeleted:
            return geogit::MODIFIED_DELETED;
        case geoversion::merge::ConflictType::BothAdded:
            return geogit::BOTH_ADDED;
        default:
            return geogit::BOTH_MODIFIED;
    }
}

} // namespace

grpc::Status GeoGitServiceImpl::Merge(grpc::ServerContext* /*context*/,
                                      const geogit::MergeRequest* request,
                                      geogit::MergeResponse* response) {
    std::lock_guard<std::mutex> lock(mutex_);
    try {
        geoversion::merge::MergeEngine engine(versions_);
        auto result = engine.merge(request->base_version_id(), request->ours_version_id(),
                                   request->theirs_version_id(), to_strategy(request->strategy()));

        auto* merged = response->mutable_merged();
        for (const auto& entry : result.merged) {
            (*merged)[entry.first] = entry.second;
        }
        for (const auto& conflict : result.conflicts) {
            auto* proto_conflict = response->add_conflicts();
            proto_conflict->set_object_id(conflict.object_id);
            proto_conflict->set_base_hash(conflict.base_hash);
            proto_conflict->set_ours_hash(conflict.ours_hash);
            proto_conflict->set_theirs_hash(conflict.theirs_hash);
            proto_conflict->set_type(to_conflict_type(conflict.type));
        }
        return grpc::Status::OK;
    } catch (const std::exception& e) {
        return grpc::Status(grpc::StatusCode::NOT_FOUND, e.what());
    }
}

} // namespace geogit_server
