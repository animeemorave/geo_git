#include "adaptive_matcher.h"

#include "diff/matchers/large_diff_matcher.h"
#include "diff/matchers/small_diff_matcher.h"

#include <utility>

namespace geoversion {
namespace diff {

AdaptiveMatcher::AdaptiveMatcher(mongocxx::collection collection, MatcherConfig config)
    : collection_(std::move(collection)), config_(config) {}

std::vector<DiffResult::SimilarityPair>
AdaptiveMatcher::match(const std::vector<GeoCandidate>& removed,
                       const std::vector<GeoCandidate>& added) const {
    if (added.size() < config_.threshold) {
        SmallDiffMatcher matcher(collection_, config_);
        return matcher.match(removed, added);
    }

    LargeDiffMatcher matcher(config_);
    return matcher.match(removed, added);
}

} // namespace diff
} // namespace geoversion
