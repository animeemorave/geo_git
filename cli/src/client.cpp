#include "client.h"

namespace geogit_cli {

GeoGitClient::GeoGitClient(const std::string& target)
    : channel_(grpc::CreateChannel(target, grpc::InsecureChannelCredentials())),
      stub_(geogit::GeoGit::NewStub(channel_)) {}

geogit::GeoGit::Stub* GeoGitClient::stub() {
    return stub_.get();
}

} // namespace geogit_cli
