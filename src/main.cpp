#include "storage/mongodb_connection/mongodb_connection.h"
#include "utils/logger/logger.h"
#include <iostream>

using namespace geoversion;

int main(int argc, char* argv[]) {
    utils::Logger::info("Starting GeoVersion Control System");

    std::string connection_string = "mongodb://localhost:27017";
    if (argc > 1) {
        connection_string = argv[1];
    }

    try {
        storage::MongoDBConnection mongo(connection_string);

        if (!mongo.test_connection()) {
            utils::Logger::error("Failed to connect to MongoDB");
            return 1;
        }

        utils::Logger::info("MongoDB connection successful");

        if (!mongo.is_initialized()) {
            utils::Logger::warning("Database not initialized. Please run init_mongodb.js script first.");
            utils::Logger::info("Attempting to create indexes...");
            
            if (mongo.initialize_database()) {
                utils::Logger::info("Database indexes created successfully");
            } else {
                utils::Logger::error("Failed to initialize database");
                return 1;
            }
        } else {
            utils::Logger::info("Database is initialized");
        }

        utils::Logger::info("GeoVersion Control System ready");

        return 0;
    } catch (const std::exception& e) {
        utils::Logger::error(std::string("Error: ") + e.what());
        return 1;
    }
}
