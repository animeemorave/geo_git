#include "small_diff_matcher.h"

#include "diff/geometry_utils.h"

#include <bsoncxx/builder/basic/array.hpp>
#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/builder/basic/kvp.hpp>

#include <cstddef>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace geoversion {
namespace diff {

using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::make_array;
using bsoncxx::builder::basic::make_document;

SmallDiffMatcher::SmallDiffMatcher(mongocxx::collection collection, MatcherConfig config)
    : collection_(std::move(collection)), config_(config) {}

std::vector<DiffResult::SimilarityPair>
SmallDiffMatcher::match(const std::vector<GeoCandidate>& removed,
                        const std::vector<GeoCandidate>& added) const {
    if (removed.empty() || added.empty()) {
        return {};
    }

    mongocxx::collection collection = collection_;

    std::unordered_map<std::string, std::size_t> hash_to_added;
    bsoncxx::builder::basic::array hash_array;
    for (std::size_t i = 0; i < added.size(); ++i) {
        hash_to_added.emplace(added[i].hash, i);
        hash_array.append(added[i].hash);
    }
    auto hashes = hash_array.extract();

    std::vector<ScoredPair> scored;
    for (std::size_t i = 0; i < removed.size(); ++i) {
        double epsilon = epsilon_for_type(config_, removed[i].type);

        auto filter = make_document(
            kvp("hash", make_document(kvp("$in", hashes.view()))),
            kvp("geometry",
                make_document(kvp(
                    "$nearSphere",
                    make_document(kvp("$geometry", make_document(kvp("type", "Point"),
                                                                 kvp("coordinates",
                                                                     make_array(removed[i].lon,
                                                                                removed[i].lat)))),
                                  kvp("$maxDistance", epsilon))))));

        auto doc = collection.find_one(filter.view());
        if (!doc) {
            continue;
        }

        auto view = doc->view();
        if (!view["hash"]) {
            continue;
        }

        std::string matched_hash(view["hash"].get_string().value);
        auto it = hash_to_added.find(matched_hash);
        if (it == hash_to_added.end()) {
            continue;
        }

        std::size_t added_index = it->second;
        double distance = haversine_meters(removed[i].lon, removed[i].lat, added[added_index].lon,
                                           added[added_index].lat);

        ScoredPair pair;
        pair.removed_index = i;
        pair.added_index = added_index;
        pair.distance_m = distance;
        scored.push_back(pair);
    }

    return resolve_one_to_one(removed, added, std::move(scored), config_);
}

} // namespace diff
} // namespace geoversion
