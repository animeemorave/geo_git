#include "mongodb_connection.h"
#include <mongocxx/options/index.hpp>
#include <bsoncxx/builder/stream/document.hpp>
#include <bsoncxx/builder/stream/array.hpp>
#include <bsoncxx/json.hpp>
#include <iostream>

namespace geoversion {
namespace storage {

MongoDBConnection::MongoDBConnection(
    const std::string& connection_string,
    const std::string& database_name
) : connection_string_(connection_string),
    database_name_(database_name),
    instance_{}
{
    try {
        mongocxx::uri uri(connection_string_);
        client_ = std::make_unique<mongocxx::client>(uri);
        database_ = client_->database(database_name_);
        
        std::cout << "Connected to MongoDB: " << connection_string_ << std::endl;
        std::cout << "Using database: " << database_name_ << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "Failed to connect to MongoDB: " << e.what() << std::endl;
        throw;
    }
}

MongoDBConnection::~MongoDBConnection() = default;

mongocxx::database MongoDBConnection::get_database() {
    return database_;
}

mongocxx::collection MongoDBConnection::get_bpo_cas_collection() {
    return database_.collection("bpo_cas");
}

mongocxx::collection MongoDBConnection::get_situations_collection() {
    return database_.collection("situations");
}

mongocxx::collection MongoDBConnection::get_situation_versions_collection() {
    return database_.collection("situation_versions");
}

mongocxx::collection MongoDBConnection::get_version_deltas_collection() {
    return database_.collection("version_deltas");
}

bool MongoDBConnection::is_initialized() {
    try {
        auto collections = database_.list_collection_names();
        std::vector<std::string> required_collections = {
            "bpo_cas",
            "situations",
            "situation_versions",
            "version_deltas"
        };

        for (const auto& required : required_collections) {
            bool found = false;
            for (const auto& collection : collections) {
                if (collection == required) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                return false;
            }
        }

        auto bpo_cas = get_bpo_cas_collection();
        auto indexes = bpo_cas.list_indexes();
        bool has_geospatial_index = false;
        for (auto&& index : indexes) {
            if (index["name"] && index["name"].get_string().value == std::string("geometry_2dsphere_idx")) {
                has_geospatial_index = true;
                break;
            }
        }

        return has_geospatial_index;
    } catch (const std::exception& e) {
        std::cerr << "Error checking initialization: " << e.what() << std::endl;
        return false;
    }
}

bool MongoDBConnection::initialize_database() {
    try {
        if (is_initialized()) {
            std::cout << "Database already initialized." << std::endl;
            return true;
        }

        std::cout << "Initializing database indexes..." << std::endl;
        create_geospatial_indexes();
        
        std::cout << "Database initialization complete." << std::endl;
        return true;
    } catch (const std::exception& e) {
        std::cerr << "Failed to initialize database: " << e.what() << std::endl;
        return false;
    }
}

bool MongoDBConnection::test_connection() {
    try {
        auto admin_db = client_->database("admin");
        auto result = admin_db.run_command(
            bsoncxx::builder::stream::document{} << "ping" << 1 
            << bsoncxx::builder::stream::finalize
        );
        return true;
    } catch (const std::exception& e) {
        std::cerr << "Connection test failed: " << e.what() << std::endl;
        return false;
    }
}

void MongoDBConnection::create_geospatial_indexes() {
    try {
        auto bpo_cas = get_bpo_cas_collection();

        bsoncxx::builder::stream::document index_spec;
        index_spec << "geometry" << "2dsphere";
        
        mongocxx::options::index index_options;
        index_options.name("geometry_2dsphere_idx");
        
        try {
            bpo_cas.create_index(index_spec.view(), index_options);
        } catch (const std::exception& e) {
            std::cerr << "Index may already exist: " << e.what() << std::endl;
        }

        bsoncxx::builder::stream::document hash_index_spec;
        hash_index_spec << "hash" << 1;
        
        mongocxx::options::index hash_index_options;
        hash_index_options.name("hash_idx").unique(true);
        
        bpo_cas.create_index(
            hash_index_spec.view(),
            hash_index_options
        );

        auto situations = get_situations_collection();
        bsoncxx::builder::stream::document situation_id_index;
        situation_id_index << "situation_id" << 1;
        
        mongocxx::options::index situation_index_options;
        situation_index_options.name("situation_id_idx").unique(true);
        
        situations.create_index(
            situation_id_index.view(),
            situation_index_options
        );

        auto situation_versions = get_situation_versions_collection();
        
        bsoncxx::builder::stream::document version_id_index;
        version_id_index << "version_id" << 1;
        
        mongocxx::options::index version_index_options;
        version_index_options.name("version_id_idx").unique(true);
        
        situation_versions.create_index(
            version_id_index.view(),
            version_index_options
        );

        bsoncxx::builder::stream::document situation_versions_lookup_index;
        situation_versions_lookup_index << "situation_id" << 1 
                                        << "created_at" << -1;
        
        mongocxx::options::index lookup_index_options;
        lookup_index_options.name("situation_versions_lookup_idx");
        
        situation_versions.create_index(
            situation_versions_lookup_index.view(),
            lookup_index_options
        );

        auto version_deltas = get_version_deltas_collection();
        
        bsoncxx::builder::stream::document delta_id_index;
        delta_id_index << "delta_id" << 1;
        
        mongocxx::options::index delta_index_options;
        delta_index_options.name("delta_id_idx").unique(true);
        
        version_deltas.create_index(
            delta_id_index.view(),
            delta_index_options
        );

        bsoncxx::builder::stream::document delta_lookup_index;
        delta_lookup_index << "from_version_id" << 1 
                          << "to_version_id" << 1;
        
        mongocxx::options::index delta_lookup_options;
        delta_lookup_options.name("delta_lookup_idx");
        
        version_deltas.create_index(
            delta_lookup_index.view(),
            delta_lookup_options
        );

        std::cout << "Geospatial indexes created successfully." << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "Error creating indexes: " << e.what() << std::endl;
        throw;
    }
}

}
}
