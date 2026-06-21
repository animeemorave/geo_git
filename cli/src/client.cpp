#include "client.h"

namespace geogit_cli {

namespace {

std::shared_ptr<grpc::Channel> make_channel(const std::string& target) {
    grpc::ChannelArguments args;
    args.SetMaxReceiveMessageSize(-1);
    args.SetMaxSendMessageSize(-1);
    return grpc::CreateCustomChannel(target, grpc::InsecureChannelCredentials(), args);
}

} // namespace

GeoGitClient::GeoGitClient(const std::string& target)
    : channel_(make_channel(target)), stub_(geogit::GeoGit::NewStub(channel_)) {}

geogit::GeoGit::Stub* GeoGitClient::stub() {
    return stub_.get();
}

} // namespace geogit_cli
