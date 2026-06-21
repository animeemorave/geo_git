#include "grpc_server.h"

#include "service_impl.h"
#include "storage/mongodb_connection/mongodb_connection.h"
#include "utils/logger/logger.h"

#include <grpcpp/grpcpp.h>

#include <memory>
#include <utility>

namespace geogit_server {

GrpcServer::GrpcServer(std::string mongo_uri, std::string listen_address)
    : mongo_uri_(std::move(mongo_uri)), listen_address_(std::move(listen_address)) {}

void GrpcServer::run() {
    geoversion::storage::MongoDBConnection connection(mongo_uri_, "geoversion");
    connection.initialize_database();

    GeoGitServiceImpl service(connection);

    grpc::ServerBuilder builder;
    builder.SetMaxReceiveMessageSize(-1);
    builder.SetMaxSendMessageSize(-1);
    builder.AddListeningPort(listen_address_, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);

    std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
    if (!server) {
        geoversion::utils::Logger::error("Failed to start gRPC server on " + listen_address_);
        return;
    }

    geoversion::utils::Logger::info("gRPC server listening on " + listen_address_);
    server->Wait();
}

} // namespace geogit_server
