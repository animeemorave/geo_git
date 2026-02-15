#include "cas.h"
#include "bpo_storage/bpo_storage.h"
#include <bsoncxx/builder/stream/document.hpp>
#include <bsoncxx/json.hpp>
#include <bsoncxx/types.hpp>
#include <openssl/sha.h>
#include <sstream>
#include <iomanip>
#include <iostream>
#include <chrono>

namespace geoversion {
namespace storage {

CAS::CAS(mongocxx::collection collection) : collection_(collection) {
}

std::string CAS::compute_hash(const bsoncxx::document::view& geometry, const bsoncxx::document::view& attributes) {
    std::string serialized = serialize_for_hashing(geometry, attributes);
    return sha256_hash(serialized);
}

std::string CAS::compute_hash(const BPO& bpo) {
    return compute_hash(bpo.get_geometry(), bpo.get_attributes());
}

bool CAS::store(const BPO& bpo) {
    std::string hash = compute_hash(bpo);
    
    if (exists(hash)) {
        return true;
    }
    
    return store(hash, bpo.get_geometry(), bpo.get_attributes());
}

bool CAS::store(const std::string& hash, const bsoncxx::document::view& geometry, const bsoncxx::document::view& attributes) {
    try {
        if (exists(hash)) {
            return true;
        }
        
        bsoncxx::builder::stream::document doc;
        doc << "hash" << hash
            << "geometry" << bsoncxx::types::b_document{bsoncxx::document::value(geometry)}
            << "attributes" << bsoncxx::types::b_document{bsoncxx::document::value(attributes)}
            << "created_at" << bsoncxx::types::b_date{std::chrono::system_clock::now()};
        
        collection_.insert_one(doc.view());
        return true;
    } catch (const std::exception& e) {
        std::cerr << "Error storing in CAS: " << e.what() << std::endl;
        return false;
    }
}

std::unique_ptr<BPO> CAS::retrieve(const std::string& hash) {
    try {
        bsoncxx::builder::stream::document filter;
        filter << "hash" << hash;
        
        auto result = collection_.find_one(filter.view());
        
        if (!result) {
            return nullptr;
        }
        
        return std::make_unique<BPO>(result->view());
    } catch (const std::exception& e) {
        std::cerr << "Error retrieving from CAS: " << e.what() << std::endl;
        return nullptr;
    }
}

bool CAS::exists(const std::string& hash) {
    try {
        bsoncxx::builder::stream::document filter;
        filter << "hash" << hash;
        
        auto result = collection_.find_one(filter.view());
        return result.has_value();
    } catch (const std::exception& e) {
        std::cerr << "Error checking CAS existence: " << e.what() << std::endl;
        return false;
    }
}

bool CAS::remove(const std::string& hash) {
    try {
        bsoncxx::builder::stream::document filter;
        filter << "hash" << hash;
        
        auto result = collection_.delete_one(filter.view());
        return result->deleted_count() > 0;
    } catch (const std::exception& e) {
        std::cerr << "Error removing from CAS: " << e.what() << std::endl;
        return false;
    }
}

std::vector<std::string> CAS::get_all_hashes() {
    std::vector<std::string> hashes;
    
    try {
        bsoncxx::builder::stream::document projection;
        projection << "hash" << 1 << "_id" << 0;
        
        mongocxx::options::find opts;
        opts.projection(projection.view());
        
        bsoncxx::builder::stream::document empty_filter;
        auto cursor = collection_.find(empty_filter.view(), opts);
        
        for (auto&& doc : cursor) {
            if (doc["hash"]) {
                hashes.push_back(doc["hash"].get_string().value.to_string());
            }
        }
    } catch (const std::exception& e) {
        std::cerr << "Error getting all hashes: " << e.what() << std::endl;
    }
    
    return hashes;
}

size_t CAS::count() {
    try {
        bsoncxx::builder::stream::document empty_filter;
        return collection_.count_documents(empty_filter.view());
    } catch (const std::exception& e) {
        std::cerr << "Error counting CAS: " << e.what() << std::endl;
        return 0;
    }
}

std::string CAS::sha256_hash(const std::string& data) {
    unsigned char hash[SHA256_DIGEST_LENGTH];
    SHA256_CTX sha256;
    SHA256_Init(&sha256);
    SHA256_Update(&sha256, data.c_str(), data.length());
    SHA256_Final(hash, &sha256);
    
    std::stringstream ss;
    for (int i = 0; i < SHA256_DIGEST_LENGTH; i++) {
        ss << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(hash[i]);
    }
    
    return ss.str();
}

std::string CAS::serialize_for_hashing(const bsoncxx::document::view& geometry, const bsoncxx::document::view& attributes) {
    std::string geometry_str = bsoncxx::to_json(geometry);
    std::string attributes_str = bsoncxx::to_json(attributes);
    
    return geometry_str + "|" + attributes_str;
}

std::vector<std::unique_ptr<BPO>> CAS::find_by_geometry_type(GeometryType type) {
    std::vector<std::unique_ptr<BPO>> results;
    
    std::string type_str;
    switch (type) {
        case GeometryType::Point:
            type_str = "Point";
            break;
        case GeometryType::LineString:
            type_str = "LineString";
            break;
        case GeometryType::Polygon:
            type_str = "Polygon";
            break;
        default:
            return results;
    }
    
    try {
        bsoncxx::builder::stream::document filter_builder;
        filter_builder << "geometry.type" << type_str;
        
        auto cursor = collection_.find(filter_builder.view());
        
        for (auto&& doc : cursor) {
            results.push_back(std::make_unique<BPO>(doc));
        }
    } catch (const std::exception& e) {
        std::cerr << "Error finding BPOs by geometry type: " << e.what() << std::endl;
    }
    
    return results;
}

std::vector<std::unique_ptr<BPO>> CAS::find_in_bbox(double min_lon, double min_lat, double max_lon, double max_lat) {
    std::vector<std::unique_ptr<BPO>> results;
    
    try {
        bsoncxx::builder::stream::document filter_builder;
        filter_builder << "geometry" << bsoncxx::builder::stream::open_document
                       << "$geoWithin" << bsoncxx::builder::stream::open_document
                       << "$geometry" << bsoncxx::builder::stream::open_document
                       << "type" << "Polygon"
                       << "coordinates" << bsoncxx::builder::stream::open_array
                       << bsoncxx::builder::stream::open_array
                       << bsoncxx::builder::stream::open_array << min_lon << min_lat << bsoncxx::builder::stream::close_array
                       << bsoncxx::builder::stream::open_array << max_lon << min_lat << bsoncxx::builder::stream::close_array
                       << bsoncxx::builder::stream::open_array << max_lon << max_lat << bsoncxx::builder::stream::close_array
                       << bsoncxx::builder::stream::open_array << min_lon << max_lat << bsoncxx::builder::stream::close_array
                       << bsoncxx::builder::stream::open_array << min_lon << min_lat << bsoncxx::builder::stream::close_array
                       << bsoncxx::builder::stream::close_array
                       << bsoncxx::builder::stream::close_document
                       << bsoncxx::builder::stream::close_document
                       << bsoncxx::builder::stream::close_document;
        
        auto cursor = collection_.find(filter_builder.view());
        
        for (auto&& doc : cursor) {
            results.push_back(std::make_unique<BPO>(doc));
        }
    } catch (const std::exception& e) {
        std::cerr << "Error finding BPOs in bbox: " << e.what() << std::endl;
    }
    
    return results;
}

}
}
