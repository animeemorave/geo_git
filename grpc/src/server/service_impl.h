#pragma once

#include "geo_git.grpc.pb.h"

#include "diff/matchers/adaptive_matcher.h"
#include "storage/branch_storage/branch_storage.h"
#include "storage/cas/cas.h"
#include "storage/delta_storage/delta_storage.h"
#include "storage/mongodb_connection/mongodb_connection.h"
#include "storage/situation_storage/situation_storage.h"
#include "storage/version_storage/version_storage.h"

#include <google/protobuf/empty.pb.h>
#include <grpcpp/grpcpp.h>

#include <mutex>

namespace geogit_server {

class GeoGitServiceImpl final : public geogit::GeoGit::Service {
public:
    explicit GeoGitServiceImpl(geoversion::storage::MongoDBConnection& connection);

    grpc::Status CreateSituation(grpc::ServerContext* context,
                                 const geogit::CreateSituationRequest* request,
                                 geogit::SituationResponse* response) override;
    grpc::Status GetSituation(grpc::ServerContext* context,
                              const geogit::GetSituationRequest* request,
                              geogit::SituationResponse* response) override;
    grpc::Status ListSituations(grpc::ServerContext* context,
                                const geogit::ListSituationsRequest* request,
                                geogit::ListSituationsResponse* response) override;
    grpc::Status UpdateSituation(grpc::ServerContext* context,
                                 const geogit::UpdateSituationRequest* request,
                                 geogit::SituationResponse* response) override;
    grpc::Status DeleteSituation(grpc::ServerContext* context,
                                 const geogit::DeleteSituationRequest* request,
                                 google::protobuf::Empty* response) override;

    grpc::Status CreateBranch(grpc::ServerContext* context,
                              const geogit::CreateBranchRequest* request,
                              geogit::BranchResponse* response) override;
    grpc::Status GetBranch(grpc::ServerContext* context, const geogit::GetBranchRequest* request,
                           geogit::BranchResponse* response) override;
    grpc::Status GetBranchByName(grpc::ServerContext* context,
                                 const geogit::GetBranchByNameRequest* request,
                                 geogit::BranchResponse* response) override;
    grpc::Status ListBranches(grpc::ServerContext* context,
                              const geogit::ListBranchesRequest* request,
                              geogit::ListBranchesResponse* response) override;
    grpc::Status AdvanceBranch(grpc::ServerContext* context,
                               const geogit::AdvanceBranchRequest* request,
                               geogit::BranchResponse* response) override;
    grpc::Status DeleteBranch(grpc::ServerContext* context,
                              const geogit::DeleteBranchRequest* request,
                              google::protobuf::Empty* response) override;

    grpc::Status CreateVersion(grpc::ServerContext* context,
                               const geogit::CreateVersionRequest* request,
                               geogit::VersionResponse* response) override;
    grpc::Status GetVersion(grpc::ServerContext* context, const geogit::GetVersionRequest* request,
                            geogit::VersionResponse* response) override;
    grpc::Status ListVersions(grpc::ServerContext* context,
                              const geogit::ListVersionsRequest* request,
                              geogit::ListVersionsResponse* response) override;
    grpc::Status Checkout(grpc::ServerContext* context, const geogit::CheckoutRequest* request,
                          grpc::ServerWriter<geogit::BPOResponse>* writer) override;

    grpc::Status StoreBPO(grpc::ServerContext* context, const geogit::StoreBPORequest* request,
                          geogit::StoreBPOResponse* response) override;
    grpc::Status GetBPO(grpc::ServerContext* context, const geogit::GetBPORequest* request,
                        geogit::BPOResponse* response) override;
    grpc::Status FindInBBox(grpc::ServerContext* context, const geogit::FindInBBoxRequest* request,
                            grpc::ServerWriter<geogit::BPOResponse>* writer) override;

    grpc::Status GetStatus(grpc::ServerContext* context, const geogit::GetStatusRequest* request,
                           geogit::DiffResponse* response) override;
    grpc::Status ComputeDiff(grpc::ServerContext* context,
                             const geogit::ComputeDiffRequest* request,
                             geogit::DiffResponse* response) override;
    grpc::Status GetDelta(grpc::ServerContext* context, const geogit::GetDeltaRequest* request,
                          geogit::DiffResponse* response) override;

    grpc::Status Merge(grpc::ServerContext* context, const geogit::MergeRequest* request,
                       geogit::MergeResponse* response) override;

private:
    geoversion::storage::MongoDBConnection& connection_;
    geoversion::storage::CAS cas_;
    geoversion::storage::SituationStorage situations_;
    geoversion::storage::BranchStorage branches_;
    geoversion::storage::VersionStorage versions_;
    geoversion::storage::DeltaStorage deltas_;
    geoversion::diff::AdaptiveMatcher matcher_;
    std::mutex mutex_;
};

} // namespace geogit_server
