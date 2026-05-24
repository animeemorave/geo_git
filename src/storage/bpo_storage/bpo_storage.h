#pragma once

#include <bsoncxx/document/view.hpp>
#include <bsoncxx/document/value.hpp>
#include <bsoncxx/builder/stream/document.hpp>
#include <mongocxx/collection.hpp>
#include <string>
#include <vector>
#include <map>
#include <memory>
#include <optional>

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
    std::optional<std::string> get_object_id() const;

    void set_hash(const std::string& hash);
    void set_geometry(const bsoncxx::document::view& geometry);
    void set_attributes(const bsoncxx::document::view& attributes);
    void set_object_id(const std::string& object_id);

    bsoncxx::document::value to_bson() const;
    bool is_valid() const;

private:
    std::string hash_;
    std::unique_ptr<bsoncxx::document::value> geometry_;
    std::unique_ptr<bsoncxx::document::value> attributes_;
    GeometryType geometry_type_;
    std::optional<std::string> object_id_;

    GeometryType parse_geometry_type(const bsoncxx::document::view& geometry);
    bool validate_geometry(const bsoncxx::document::view& geometry) const;
    bool validate_point(const bsoncxx::document::view& geometry) const;
    bool validate_linestring(const bsoncxx::document::view& geometry) const;
    bool validate_polygon(const bsoncxx::document::view& geometry) const;
};

class GeoJSONValidator {
public:
    static bool validate(const bsoncxx::document::view& geometry);
    static GeometryType get_type(const bsoncxx::document::view& geometry);
    static bool validate_coordinates(const bsoncxx::document::view& geometry);
    static bool validate_point_coordinates(const bsoncxx::array::view& coordinates);
    static bool validate_linestring_coordinates(const bsoncxx::array::view& coordinates);
    static bool validate_polygon_coordinates(const bsoncxx::array::view& coordinates);
};


}
}
