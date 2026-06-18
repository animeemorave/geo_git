#pragma once

#include "geo_git.grpc.pb.h"

#include <grpcpp/grpcpp.h>

#include <memory>
#include <string>

namespace geogit_cli {

class GeoGitClient {
public:
    explicit GeoGitClient(const std::string& target);

    geogit::GeoGit::Stub* stub();

private:
    std::shared_ptr<grpc::Channel> channel_;
    std::unique_ptr<geogit::GeoGit::Stub> stub_;
};

} // namespace geogit_cli
