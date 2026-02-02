#include "bpo_storage.h"
#include <bsoncxx/builder/stream/array.hpp>
#include <bsoncxx/builder/stream/helpers.hpp>
#include <bsoncxx/json.hpp>
#include <bsoncxx/types.hpp>
#include <iostream>
#include <chrono>

namespace geoversion {
namespace storage {

BPO::BPO() : geometry_type_(GeometryType::Unknown) {
}

BPO::BPO(const bsoncxx::document::view& doc) {
    if (doc["hash"]) {
        hash_ = doc["hash"].get_string().value.to_string();
    }
    
    if (doc["geometry"]) {
        geometry_ = bsoncxx::document::value(doc["geometry"].get_document().value);
        geometry_type_ = parse_geometry_type(geometry_.view());
    } else {
        geometry_type_ = GeometryType::Unknown;
    }
    
    if (doc["attributes"]) {
        attributes_ = bsoncxx::document::value(doc["attributes"].get_document().value);
    }
}

BPO::BPO(const std::string& hash, const bsoncxx::document::view& geometry, const bsoncxx::document::view& attributes)
    : hash_(hash), geometry_(bsoncxx::document::value(geometry)), attributes_(bsoncxx::document::value(attributes)) {
    geometry_type_ = parse_geometry_type(geometry);
}

std::string BPO::get_hash() const {
    return hash_;
}

bsoncxx::document::view BPO::get_geometry() const {
    return geometry_.view();
}

bsoncxx::document::view BPO::get_attributes() const {
    return attributes_.view();
}

GeometryType BPO::get_geometry_type() const {
    return geometry_type_;
}

void BPO::set_hash(const std::string& hash) {
    hash_ = hash;
}

void BPO::set_geometry(const bsoncxx::document::view& geometry) {
    geometry_ = bsoncxx::document::value(geometry);
    geometry_type_ = parse_geometry_type(geometry);
}

void BPO::set_attributes(const bsoncxx::document::view& attributes) {
    attributes_ = bsoncxx::document::value(attributes);
}

bsoncxx::document::value BPO::to_bson() const {
    bsoncxx::builder::stream::document builder;
    builder << "hash" << hash_
            << "geometry" << bsoncxx::types::b_document{geometry_}
            << "attributes" << bsoncxx::types::b_document{attributes_}
            << "created_at" << bsoncxx::types::b_date{std::chrono::system_clock::now()};
    
    return builder << bsoncxx::builder::stream::finalize;
}

bool BPO::is_valid() const {
    if (hash_.empty()) {
        return false;
    }
    return validate_geometry(geometry_.view());
}

GeometryType BPO::parse_geometry_type(const bsoncxx::document::view& geometry) {
    if (!geometry["type"]) {
        return GeometryType::Unknown;
    }
    
    std::string type = geometry["type"].get_string().value.to_string();
    
    if (type == "Point") return GeometryType::Point;
    if (type == "LineString") return GeometryType::LineString;
    if (type == "Polygon") return GeometryType::Polygon;
    if (type == "MultiPoint") return GeometryType::MultiPoint;
    if (type == "MultiLineString") return GeometryType::MultiLineString;
    if (type == "MultiPolygon") return GeometryType::MultiPolygon;
    if (type == "GeometryCollection") return GeometryType::GeometryCollection;
    
    return GeometryType::Unknown;
}

bool BPO::validate_geometry(const bsoncxx::document::view& geometry) {
    return GeoJSONValidator::validate(geometry);
}

bool BPO::validate_point(const bsoncxx::document::view& geometry) {
    if (!geometry["type"] || !geometry["coordinates"]) {
        return false;
    }
    
    std::string type = geometry["type"].get_string().value.to_string();
    if (type != "Point") {
        return false;
    }
    
    auto coordinates = geometry["coordinates"].get_array().value;
    return GeoJSONValidator::validate_point_coordinates(coordinates);
}

bool BPO::validate_linestring(const bsoncxx::document::view& geometry) {
    if (!geometry["type"] || !geometry["coordinates"]) {
        return false;
    }
    
    std::string type = geometry["type"].get_string().value.to_string();
    if (type != "LineString") {
        return false;
    }
    
    auto coordinates = geometry["coordinates"].get_array().value;
    return GeoJSONValidator::validate_linestring_coordinates(coordinates);
}

bool BPO::validate_polygon(const bsoncxx::document::view& geometry) {
    if (!geometry["type"] || !geometry["coordinates"]) {
        return false;
    }
    
    std::string type = geometry["type"].get_string().value.to_string();
    if (type != "Polygon") {
        return false;
    }
    
    auto coordinates = geometry["coordinates"].get_array().value;
    return GeoJSONValidator::validate_polygon_coordinates(coordinates);
}

bool GeoJSONValidator::validate(const bsoncxx::document::view& geometry) {
    if (!geometry["type"] || !geometry["coordinates"]) {
        return false;
    }
    
    std::string type = geometry["type"].get_string().value.to_string();
    auto coordinates = geometry["coordinates"].get_array().value;
    
    if (type == "Point") {
        return validate_point_coordinates(coordinates);
    } else if (type == "LineString") {
        return validate_linestring_coordinates(coordinates);
    } else if (type == "Polygon") {
        return validate_polygon_coordinates(coordinates);
    }
    
    return false;
}

GeometryType GeoJSONValidator::get_type(const bsoncxx::document::view& geometry) {
    if (!geometry["type"]) {
        return GeometryType::Unknown;
    }
    
    std::string type = geometry["type"].get_string().value.to_string();
    
    if (type == "Point") return GeometryType::Point;
    if (type == "LineString") return GeometryType::LineString;
    if (type == "Polygon") return GeometryType::Polygon;
    if (type == "MultiPoint") return GeometryType::MultiPoint;
    if (type == "MultiLineString") return GeometryType::MultiLineString;
    if (type == "MultiPolygon") return GeometryType::MultiPolygon;
    if (type == "GeometryCollection") return GeometryType::GeometryCollection;
    
    return GeometryType::Unknown;
}

bool GeoJSONValidator::validate_coordinates(const bsoncxx::document::view& geometry) {
    if (!geometry["coordinates"]) {
        return false;
    }
    
    auto coordinates = geometry["coordinates"].get_array().value;
    std::string type = geometry["type"].get_string().value.to_string();
    
    if (type == "Point") {
        return validate_point_coordinates(coordinates);
    } else if (type == "LineString") {
        return validate_linestring_coordinates(coordinates);
    } else if (type == "Polygon") {
        return validate_polygon_coordinates(coordinates);
    }
    
    return false;
}

bool GeoJSONValidator::validate_point_coordinates(const bsoncxx::array::view& coordinates) {
    if (coordinates.length() < 2) {
        return false;
    }
    
    auto it = coordinates.begin();
    if (it == coordinates.end() || (*it).type() != bsoncxx::type::k_double) {
        return false;
    }
    ++it;
    if (it == coordinates.end() || (*it).type() != bsoncxx::type::k_double) {
        return false;
    }
    
    double lon = (*coordinates.begin()).get_double().value;
    double lat = (*(++coordinates.begin())).get_double().value;
    
    return lon >= -180.0 && lon <= 180.0 && lat >= -90.0 && lat <= 90.0;
}

bool GeoJSONValidator::validate_linestring_coordinates(const bsoncxx::array::view& coordinates) {
    if (coordinates.length() < 2) {
        return false;
    }
    
    for (auto&& point : coordinates) {
        if (point.type() != bsoncxx::type::k_array) {
            return false;
        }
        auto point_array = point.get_array().value;
        if (!validate_point_coordinates(point_array)) {
            return false;
        }
    }
    
    return true;
}

bool GeoJSONValidator::validate_polygon_coordinates(const bsoncxx::array::view& coordinates) {
    if (coordinates.length() == 0) {
        return false;
    }
    
    for (auto&& ring : coordinates) {
        if (ring.type() != bsoncxx::type::k_array) {
            return false;
        }
        auto ring_array = ring.get_array().value;
        if (ring_array.length() < 4) {
            return false;
        }
        
        for (auto&& point : ring_array) {
            if (point.type() != bsoncxx::type::k_array) {
                return false;
            }
            auto point_array = point.get_array().value;
            if (!validate_point_coordinates(point_array)) {
                return false;
            }
        }
    }
    
    return true;
}


BPOStorage::BPOStorage(mongocxx::collection collection) : collection_(collection) {
}

bool BPOStorage::save(const BPO& bpo) {
    if (!bpo.is_valid()) {
        return false;
    }
    
    try {
        auto doc = bpo.to_bson();
        collection_.insert_one(doc.view());
        return true;
    } catch (const std::exception& e) {
        std::cerr << "Error saving BPO: " << e.what() << std::endl;
        return false;
    }
}

std::unique_ptr<BPO> BPOStorage::load(const std::string& hash) {
    try {
        bsoncxx::builder::stream::document filter_builder;
        filter_builder << "hash" << hash;
        
        auto result = collection_.find_one(filter_builder.view());
        
        if (!result) {
            return nullptr;
        }
        
        return std::make_unique<BPO>(result->view());
    } catch (const std::exception& e) {
        std::cerr << "Error loading BPO: " << e.what() << std::endl;
        return nullptr;
    }
}

bool BPOStorage::exists(const std::string& hash) {
    try {
        bsoncxx::builder::stream::document filter_builder;
        filter_builder << "hash" << hash;
        
        auto result = collection_.find_one(filter_builder.view());
        return result.has_value();
    } catch (const std::exception& e) {
        std::cerr << "Error checking BPO existence: " << e.what() << std::endl;
        return false;
    }
}

bool BPOStorage::remove(const std::string& hash) {
    try {
        bsoncxx::builder::stream::document filter_builder;
        filter_builder << "hash" << hash;
        
        auto result = collection_.delete_one(filter_builder.view());
        return result->deleted_count() > 0;
    } catch (const std::exception& e) {
        std::cerr << "Error removing BPO: " << e.what() << std::endl;
        return false;
    }
}

std::vector<std::unique_ptr<BPO>> BPOStorage::find_by_geometry_type(GeometryType type) {
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

std::vector<std::unique_ptr<BPO>> BPOStorage::find_in_bbox(double min_lon, double min_lat, double max_lon, double max_lat) {
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
