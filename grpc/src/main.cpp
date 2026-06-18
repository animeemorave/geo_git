#include "server/grpc_server.h"
#include "utils/logger/logger.h"

#include <cstdlib>
#include <exception>
#include <string>

namespace {

std::string env_or(const char* name, const std::string& fallback) {
    const char* value = std::getenv(name);
    if (value && value[0] != '\0') {
        return std::string(value);
    }
    return fallback;
}

} // namespace

int main() {
    std::string mongo_uri = env_or("MONGODB_URI", "mongodb://localhost:27017");
    std::string port = env_or("GRPC_PORT", "50051");
    std::string address = "0.0.0.0:" + port;

    try {
        geogit_server::GrpcServer server(mongo_uri, address);
        server.run();
    } catch (const std::exception& e) {
        geoversion::utils::Logger::error(std::string("Server error: ") + e.what());
        return 1;
    }

    return 0;
}
