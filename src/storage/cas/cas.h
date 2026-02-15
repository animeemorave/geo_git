#pragma once

#include <mongocxx/collection.hpp>
#include <bsoncxx/document/view.hpp>
#include <bsoncxx/document/value.hpp>
#include <string>
#include <memory>
#include <vector>

namespace geoversion {
namespace storage {

class BPO;

enum class GeometryType;

class CAS {
public:
    explicit CAS(mongocxx::collection collection);

    std::string compute_hash(const bsoncxx::document::view& geometry, const bsoncxx::document::view& attributes);
    std::string compute_hash(const BPO& bpo);
    
    bool store(const BPO& bpo);
    bool store(const std::string& hash, const bsoncxx::document::view& geometry, const bsoncxx::document::view& attributes);
    
    std::unique_ptr<BPO> retrieve(const std::string& hash);
    bool exists(const std::string& hash);
    
    bool remove(const std::string& hash);
    
    std::vector<std::string> get_all_hashes();
    
    size_t count();
    
    std::vector<std::unique_ptr<BPO>> find_by_geometry_type(GeometryType type);
    std::vector<std::unique_ptr<BPO>> find_in_bbox(double min_lon, double min_lat, double max_lon, double max_lat);

private:
    mongocxx::collection collection_;
    
    std::string sha256_hash(const std::string& data);
    std::string serialize_for_hashing(const bsoncxx::document::view& geometry, const bsoncxx::document::view& attributes);
};

}
}
