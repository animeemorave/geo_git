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



}
}
