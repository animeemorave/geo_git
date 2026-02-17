#pragma once

#include <mongocxx/client.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/pool.hpp>
#include <mongocxx/uri.hpp>
#include <bsoncxx/builder/stream/document.hpp>
#include <bsoncxx/document/view.hpp>
#include <memory>
#include <string>

namespace geoversion {
namespace storage {

class MongoDBConnection {
public:
    explicit MongoDBConnection(
        const std::string& connection_string = "mongodb://localhost:27017",
        const std::string& database_name = "geoversion"
    );

    ~MongoDBConnection();

    MongoDBConnection(const MongoDBConnection&) = delete;
    MongoDBConnection& operator=(const MongoDBConnection&) = delete;

    MongoDBConnection(MongoDBConnection&&) = default;
    MongoDBConnection& operator=(MongoDBConnection&&) = default;

    mongocxx::database get_database();

    mongocxx::collection get_bpo_cas_collection();

    mongocxx::collection get_situations_collection();

    mongocxx::collection get_situation_versions_collection();

    mongocxx::collection get_version_deltas_collection();

    bool is_initialized();

    bool initialize_database();

    bool test_connection();

private:
    std::string connection_string_;
    std::string database_name_;
    std::unique_ptr<mongocxx::client> client_;
    mongocxx::database database_;

    void create_geospatial_indexes();
};

}
}
