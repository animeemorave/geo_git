#pragma once

#include <string>

namespace geogit_server {

class GrpcServer {
public:
    GrpcServer(std::string mongo_uri, std::string listen_address);

    void run();

private:
    std::string mongo_uri_;
    std::string listen_address_;
};

} // namespace geogit_server
