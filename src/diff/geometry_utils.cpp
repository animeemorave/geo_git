#include "geometry_utils.h"

#include <bsoncxx/array/view.hpp>
#include <bsoncxx/types.hpp>

#include <boost/geometry.hpp>
#include <boost/geometry/geometries/geometries.hpp>

#include <cmath>
#include <exception>

namespace geoversion {
namespace diff {

namespace bg = boost::geometry;

namespace {

using Point2D = bg::model::d2::point_xy<double>;
using Line2D = bg::model::linestring<Point2D>;
using Polygon2D = bg::model::polygon<Point2D>;

constexpr double kPi = 3.14159265358979323846;
constexpr double kEarthRadiusM = 6371000.0;

double element_as_double(const bsoncxx::array::element& element) {
    switch (element.type()) {
        case bsoncxx::type::k_double:
            return element.get_double().value;
        case bsoncxx::type::k_int32:
            return static_cast<double>(element.get_int32().value);
        case bsoncxx::type::k_int64:
            return static_cast<double>(element.get_int64().value);
        default:
            return 0.0;
    }
}

bool read_pair(const bsoncxx::array::view& pair, double& lon, double& lat) {
    auto it = pair.begin();
    if (it == pair.end()) {
        return false;
    }
    lon = element_as_double(*it);
    ++it;
    if (it == pair.end()) {
        return false;
    }
    lat = element_as_double(*it);
    return true;
}

RepresentativePoint from_point(const bsoncxx::array::view& coordinates) {
    RepresentativePoint point;
    point.valid = read_pair(coordinates, point.lon, point.lat);
    return point;
}

RepresentativePoint from_linestring(const bsoncxx::array::view& coordinates) {
    Line2D line;
    for (auto&& vertex : coordinates) {
        double lon = 0.0;
        double lat = 0.0;
        if (read_pair(vertex.get_array().value, lon, lat)) {
            line.push_back(Point2D(lon, lat));
        }
    }

    RepresentativePoint point;
    if (line.empty()) {
        return point;
    }

    Point2D centroid;
    try {
        bg::centroid(line, centroid);
        point.lon = bg::get<0>(centroid);
        point.lat = bg::get<1>(centroid);
    } catch (const std::exception&) {
        point.lon = bg::get<0>(line.front());
        point.lat = bg::get<1>(line.front());
    }
    point.valid = true;
    return point;
}

RepresentativePoint from_polygon(const bsoncxx::array::view& coordinates) {
    auto ring_it = coordinates.begin();
    if (ring_it == coordinates.end()) {
        return RepresentativePoint{};
    }

    Polygon2D polygon;
    for (auto&& vertex : ring_it->get_array().value) {
        double lon = 0.0;
        double lat = 0.0;
        if (read_pair(vertex.get_array().value, lon, lat)) {
            polygon.outer().push_back(Point2D(lon, lat));
        }
    }

    RepresentativePoint point;
    if (polygon.outer().empty()) {
        return point;
    }

    Point2D centroid;
    try {
        bg::centroid(polygon, centroid);
        point.lon = bg::get<0>(centroid);
        point.lat = bg::get<1>(centroid);
    } catch (const std::exception&) {
        point.lon = bg::get<0>(polygon.outer().front());
        point.lat = bg::get<1>(polygon.outer().front());
    }
    point.valid = true;
    return point;
}

} // namespace

RepresentativePoint representative_point(const bsoncxx::document::view& geometry,
                                         storage::GeometryType type) {
    if (!geometry["coordinates"]) {
        return RepresentativePoint{};
    }
    auto coordinates = geometry["coordinates"].get_array().value;

    switch (type) {
        case storage::GeometryType::Point:
            return from_point(coordinates);
        case storage::GeometryType::LineString:
            return from_linestring(coordinates);
        case storage::GeometryType::Polygon:
            return from_polygon(coordinates);
        default:
            return RepresentativePoint{};
    }
}

double haversine_meters(double lon1, double lat1, double lon2, double lat2) {
    double phi1 = lat1 * kPi / 180.0;
    double phi2 = lat2 * kPi / 180.0;
    double delta_phi = (lat2 - lat1) * kPi / 180.0;
    double delta_lambda = (lon2 - lon1) * kPi / 180.0;

    double a = std::sin(delta_phi / 2.0) * std::sin(delta_phi / 2.0) +
               std::cos(phi1) * std::cos(phi2) * std::sin(delta_lambda / 2.0) *
                   std::sin(delta_lambda / 2.0);
    double c = 2.0 * std::atan2(std::sqrt(a), std::sqrt(1.0 - a));
    return kEarthRadiusM * c;
}

} // namespace diff
} // namespace geoversion
