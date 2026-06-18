#include "server/conversions.h"
#include "server/service_impl.h"

#include <exception>
#include <mutex>
#include <optional>
#include <string>

namespace geogit_server {

grpc::Status GeoGitServiceImpl::CreateBranch(grpc::ServerContext* /*context*/,
                                             const geogit::CreateBranchRequest* request,
                                             geogit::BranchResponse* response) {
    std::lock_guard<std::mutex> lock(mutex_);
    try {
        std::optional<std::string> head;
        if (!request->head_version_id().empty()) {
            head = request->head_version_id();
        }
        std::string id = branches_.create(request->situation_id(), request->name(), head);
        auto branch = branches_.get(id);
        if (!branch) {
            return grpc::Status(grpc::StatusCode::INTERNAL, "Created branch not found");
        }
        fill_branch(*branch, response->mutable_branch());
        return grpc::Status::OK;
    } catch (const std::exception& e) {
        return grpc::Status(grpc::StatusCode::INTERNAL, e.what());
    }
}

grpc::Status GeoGitServiceImpl::GetBranch(grpc::ServerContext* /*context*/,
                                          const geogit::GetBranchRequest* request,
                                          geogit::BranchResponse* response) {
    std::lock_guard<std::mutex> lock(mutex_);
    try {
        auto branch = branches_.get(request->branch_id());
        if (!branch) {
            return grpc::Status(grpc::StatusCode::NOT_FOUND, "Branch not found");
        }
        fill_branch(*branch, response->mutable_branch());
        return grpc::Status::OK;
    } catch (const std::exception& e) {
        return grpc::Status(grpc::StatusCode::INTERNAL, e.what());
    }
}

grpc::Status GeoGitServiceImpl::GetBranchByName(grpc::ServerContext* /*context*/,
                                                const geogit::GetBranchByNameRequest* request,
                                                geogit::BranchResponse* response) {
    std::lock_guard<std::mutex> lock(mutex_);
    try {
        auto branch = branches_.get_by_name(request->situation_id(), request->name());
        if (!branch) {
            return grpc::Status(grpc::StatusCode::NOT_FOUND, "Branch not found");
        }
        fill_branch(*branch, response->mutable_branch());
        return grpc::Status::OK;
    } catch (const std::exception& e) {
        return grpc::Status(grpc::StatusCode::INTERNAL, e.what());
    }
}

grpc::Status GeoGitServiceImpl::ListBranches(grpc::ServerContext* /*context*/,
                                             const geogit::ListBranchesRequest* request,
                                             geogit::ListBranchesResponse* response) {
    std::lock_guard<std::mutex> lock(mutex_);
    try {
        for (const auto& branch : branches_.list(request->situation_id())) {
            fill_branch(branch, response->add_branches());
        }
        return grpc::Status::OK;
    } catch (const std::exception& e) {
        return grpc::Status(grpc::StatusCode::INTERNAL, e.what());
    }
}

grpc::Status GeoGitServiceImpl::AdvanceBranch(grpc::ServerContext* /*context*/,
                                              const geogit::AdvanceBranchRequest* request,
                                              geogit::BranchResponse* response) {
    std::lock_guard<std::mutex> lock(mutex_);
    try {
        branches_.advance(request->branch_id(), request->new_version_id());
        auto branch = branches_.get(request->branch_id());
        if (!branch) {
            return grpc::Status(grpc::StatusCode::NOT_FOUND, "Branch not found");
        }
        fill_branch(*branch, response->mutable_branch());
        return grpc::Status::OK;
    } catch (const std::exception& e) {
        return grpc::Status(grpc::StatusCode::INTERNAL, e.what());
    }
}

grpc::Status GeoGitServiceImpl::DeleteBranch(grpc::ServerContext* /*context*/,
                                             const geogit::DeleteBranchRequest* request,
                                             google::protobuf::Empty* /*response*/) {
    std::lock_guard<std::mutex> lock(mutex_);
    try {
        branches_.remove(request->branch_id());
        return grpc::Status::OK;
    } catch (const std::exception& e) {
        return grpc::Status(grpc::StatusCode::INTERNAL, e.what());
    }
}

} // namespace geogit_server
