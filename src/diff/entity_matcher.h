#pragma once

#include "diff/diff_result.h"
#include "storage/bpo_storage/bpo_storage.h"

#include <cstddef>
#include <string>
#include <vector>

namespace geoversion {
namespace diff {

struct GeoCandidate {
    std::string object_id;
    std::string hash;
    double lon = 0.0;
    double lat = 0.0;
    storage::GeometryType type = storage::GeometryType::Unknown;
};

struct MatcherConfig {
    double point_epsilon_m = 1.0;
    double line_epsilon_m = 10.0;
    double polygon_epsilon_m = 10.0;
    std::size_t threshold = 10000;
};

struct ScoredPair {
    std::size_t removed_index = 0;
    std::size_t added_index = 0;
    double distance_m = 0.0;
};

double epsilon_for_type(const MatcherConfig& config, storage::GeometryType type);
double confidence_for(double distance_m, double epsilon_m);

std::vector<DiffResult::SimilarityPair> resolve_one_to_one(const std::vector<GeoCandidate>& removed,
                                                           const std::vector<GeoCandidate>& added,
                                                           std::vector<ScoredPair> scored,
                                                           const MatcherConfig& config);

class EntityMatcher {
public:
    virtual ~EntityMatcher() = default;

    virtual std::vector<DiffResult::SimilarityPair>
    match(const std::vector<GeoCandidate>& removed,
          const std::vector<GeoCandidate>& added) const = 0;
};

} // namespace diff
} // namespace geoversion
