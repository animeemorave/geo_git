#pragma once

#include <bsoncxx/document/view.hpp>
#include <bsoncxx/document/value.hpp>
#include <bsoncxx/builder/stream/document.hpp>
#include <mongocxx/collection.hpp>
#include <string>
#include <vector>
#include <map>
#include <memory>

namespace geoversion {
namespace storage {

enum class GeometryType {
    Point,
    LineString,
    Polygon,
    MultiPoint,
    MultiLineString,
    MultiPolygon,
    GeometryCollection,
    Unknown
};

class BPO {
public:
    BPO();
    BPO(const bsoncxx::document::view& doc);
    BPO(const std::string& hash, const bsoncxx::document::view& geometry, const bsoncxx::document::view& attributes);

    std::string get_hash() const;
    bsoncxx::document::view get_geometry() const;
    bsoncxx::document::view get_attributes() const;
    GeometryType get_geometry_type() const;

    void set_hash(const std::string& hash);
    void set_geometry(const bsoncxx::document::view& geometry);
    void set_attributes(const bsoncxx::document::view& attributes);

    bsoncxx::document::value to_bson() const;
    bool is_valid() const;

private:
    std::string hash_;
    bsoncxx::document::value geometry_;
    bsoncxx::document::value attributes_;
    GeometryType geometry_type_;

    GeometryType parse_geometry_type(const bsoncxx::document::view& geometry);
    bool validate_geometry(const bsoncxx::document::view& geometry);
    bool validate_point(const bsoncxx::document::view& geometry);
    bool validate_linestring(const bsoncxx::document::view& geometry);
    bool validate_polygon(const bsoncxx::document::view& geometry);
};

class GeoJSONValidator {
public:
    static bool validate(const bsoncxx::document::view& geometry);
    static GeometryType get_type(const bsoncxx::document::view& geometry);
    static bool validate_coordinates(const bsoncxx::document::view& geometry);

private:
    static bool validate_point_coordinates(const bsoncxx::array::view& coordinates);
    static bool validate_linestring_coordinates(const bsoncxx::array::view& coordinates);
    static bool validate_polygon_coordinates(const bsoncxx::array::view& coordinates);
};


}
}
