#include "server/conversions.h"
#include "server/service_impl.h"

#include <exception>
#include <mutex>

namespace geogit_server {

grpc::Status GeoGitServiceImpl::CreateSituation(grpc::ServerContext* /*context*/,
                                                const geogit::CreateSituationRequest* request,
                                                geogit::SituationResponse* response) {
    std::lock_guard<std::mutex> lock(mutex_);
    try {
        std::string id = situations_.create(request->name(), request->description());
        auto situation = situations_.get(id);
        if (!situation) {
            return grpc::Status(grpc::StatusCode::INTERNAL, "Created situation not found");
        }
        fill_situation(*situation, response->mutable_situation());
        return grpc::Status::OK;
    } catch (const std::exception& e) {
        return grpc::Status(grpc::StatusCode::INTERNAL, e.what());
    }
}

grpc::Status GeoGitServiceImpl::GetSituation(grpc::ServerContext* /*context*/,
                                             const geogit::GetSituationRequest* request,
                                             geogit::SituationResponse* response) {
    std::lock_guard<std::mutex> lock(mutex_);
    try {
        auto situation = situations_.get(request->situation_id());
        if (!situation) {
            return grpc::Status(grpc::StatusCode::NOT_FOUND, "Situation not found");
        }
        fill_situation(*situation, response->mutable_situation());
        return grpc::Status::OK;
    } catch (const std::exception& e) {
        return grpc::Status(grpc::StatusCode::INTERNAL, e.what());
    }
}

grpc::Status GeoGitServiceImpl::ListSituations(grpc::ServerContext* /*context*/,
                                               const geogit::ListSituationsRequest* /*request*/,
                                               geogit::ListSituationsResponse* response) {
    std::lock_guard<std::mutex> lock(mutex_);
    try {
        for (const auto& situation : situations_.list()) {
            fill_situation(situation, response->add_situations());
        }
        return grpc::Status::OK;
    } catch (const std::exception& e) {
        return grpc::Status(grpc::StatusCode::INTERNAL, e.what());
    }
}

grpc::Status GeoGitServiceImpl::UpdateSituation(grpc::ServerContext* /*context*/,
                                                const geogit::UpdateSituationRequest* request,
                                                geogit::SituationResponse* response) {
    std::lock_guard<std::mutex> lock(mutex_);
    try {
        situations_.update(request->situation_id(), request->name(), request->description());
        auto situation = situations_.get(request->situation_id());
        if (!situation) {
            return grpc::Status(grpc::StatusCode::NOT_FOUND, "Situation not found");
        }
        fill_situation(*situation, response->mutable_situation());
        return grpc::Status::OK;
    } catch (const std::exception& e) {
        return grpc::Status(grpc::StatusCode::INTERNAL, e.what());
    }
}

grpc::Status GeoGitServiceImpl::DeleteSituation(grpc::ServerContext* /*context*/,
                                                const geogit::DeleteSituationRequest* request,
                                                google::protobuf::Empty* /*response*/) {
    std::lock_guard<std::mutex> lock(mutex_);
    try {
        situations_.remove(request->situation_id());
        return grpc::Status::OK;
    } catch (const std::exception& e) {
        return grpc::Status(grpc::StatusCode::INTERNAL, e.what());
    }
}

} // namespace geogit_server
