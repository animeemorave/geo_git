#include "entity_matcher.h"

#include <algorithm>

namespace geoversion {
namespace diff {

double epsilon_for_type(const MatcherConfig& config, storage::GeometryType type) {
    switch (type) {
        case storage::GeometryType::Point:
        case storage::GeometryType::MultiPoint:
            return config.point_epsilon_m;
        case storage::GeometryType::LineString:
        case storage::GeometryType::MultiLineString:
            return config.line_epsilon_m;
        default:
            return config.polygon_epsilon_m;
    }
}

double confidence_for(double distance_m, double epsilon_m) {
    if (epsilon_m <= 0.0) {
        return 0.0;
    }
    double confidence = 1.0 - distance_m / epsilon_m;
    if (confidence < 0.0) {
        return 0.0;
    }
    if (confidence > 1.0) {
        return 1.0;
    }
    return confidence;
}

std::vector<DiffResult::SimilarityPair> resolve_one_to_one(const std::vector<GeoCandidate>& removed,
                                                           const std::vector<GeoCandidate>& added,
                                                           std::vector<ScoredPair> scored,
                                                           const MatcherConfig& config) {
    std::sort(scored.begin(), scored.end(), [](const ScoredPair& a, const ScoredPair& b) {
        if (a.distance_m != b.distance_m) {
            return a.distance_m < b.distance_m;
        }
        if (a.removed_index != b.removed_index) {
            return a.removed_index < b.removed_index;
        }
        return a.added_index < b.added_index;
    });

    std::vector<bool> removed_used(removed.size(), false);
    std::vector<bool> added_used(added.size(), false);

    std::vector<DiffResult::SimilarityPair> result;
    for (const auto& pair : scored) {
        if (removed_used[pair.removed_index] || added_used[pair.added_index]) {
            continue;
        }
        removed_used[pair.removed_index] = true;
        added_used[pair.added_index] = true;

        double epsilon = epsilon_for_type(config, removed[pair.removed_index].type);

        DiffResult::SimilarityPair similarity;
        similarity.removed_hash = removed[pair.removed_index].hash;
        similarity.added_hash = added[pair.added_index].hash;
        similarity.distance_m = pair.distance_m;
        similarity.confidence = confidence_for(pair.distance_m, epsilon);
        result.push_back(similarity);
    }

    std::sort(result.begin(), result.end(),
              [](const DiffResult::SimilarityPair& a, const DiffResult::SimilarityPair& b) {
                  return a.removed_hash < b.removed_hash;
              });

    return result;
}

} // namespace diff
} // namespace geoversion
