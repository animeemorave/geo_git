#include "large_diff_matcher.h"

#include "diff/geometry_utils.h"

#include <boost/geometry.hpp>
#include <boost/geometry/geometries/geometries.hpp>
#include <boost/geometry/index/rtree.hpp>

#include <cstddef>
#include <iterator>
#include <utility>
#include <vector>

namespace geoversion {
namespace diff {

namespace bg = boost::geometry;
namespace bgi = boost::geometry::index;

namespace {

using RTreePoint = bg::model::point<double, 2, bg::cs::cartesian>;
using RTreeValue = std::pair<RTreePoint, std::size_t>;

constexpr std::size_t kNearestK = 5;

} // namespace

LargeDiffMatcher::LargeDiffMatcher(MatcherConfig config) : config_(config) {}

std::vector<DiffResult::SimilarityPair>
LargeDiffMatcher::match(const std::vector<GeoCandidate>& removed,
                        const std::vector<GeoCandidate>& added) const {
    if (removed.empty() || added.empty()) {
        return {};
    }

    bgi::rtree<RTreeValue, bgi::rstar<16>> tree;
    for (std::size_t i = 0; i < added.size(); ++i) {
        tree.insert(std::make_pair(RTreePoint(added[i].lon, added[i].lat), i));
    }

    std::vector<ScoredPair> scored;
    for (std::size_t i = 0; i < removed.size(); ++i) {
        std::vector<RTreeValue> nearest;
        tree.query(bgi::nearest(RTreePoint(removed[i].lon, removed[i].lat), kNearestK),
                   std::back_inserter(nearest));

        double epsilon = epsilon_for_type(config_, removed[i].type);
        double best_distance = 0.0;
        std::size_t best_index = 0;
        bool found = false;
        for (const auto& value : nearest) {
            std::size_t added_index = value.second;
            double distance = haversine_meters(removed[i].lon, removed[i].lat,
                                               added[added_index].lon, added[added_index].lat);
            if (distance <= epsilon && (!found || distance < best_distance)) {
                best_distance = distance;
                best_index = added_index;
                found = true;
            }
        }

        if (found) {
            ScoredPair pair;
            pair.removed_index = i;
            pair.added_index = best_index;
            pair.distance_m = best_distance;
            scored.push_back(pair);
        }
    }

    return resolve_one_to_one(removed, added, std::move(scored), config_);
}

} // namespace diff
} // namespace geoversion
