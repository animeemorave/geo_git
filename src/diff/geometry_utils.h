#pragma once

#include "storage/bpo_storage/bpo_storage.h"

#include <bsoncxx/document/view.hpp>

namespace geoversion {
namespace diff {

struct RepresentativePoint {
    double lon = 0.0;
    double lat = 0.0;
    bool valid = false;
};

RepresentativePoint representative_point(const bsoncxx::document::view& geometry,
                                         storage::GeometryType type);

double haversine_meters(double lon1, double lat1, double lon2, double lat2);

} // namespace diff
} // namespace geoversion
