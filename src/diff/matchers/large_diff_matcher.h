#pragma once

#include "diff/entity_matcher.h"

namespace geoversion {
namespace diff {

class LargeDiffMatcher : public EntityMatcher {
public:
    explicit LargeDiffMatcher(MatcherConfig config = {});

    std::vector<DiffResult::SimilarityPair>
    match(const std::vector<GeoCandidate>& removed,
          const std::vector<GeoCandidate>& added) const override;

private:
    MatcherConfig config_;
};

} // namespace diff
} // namespace geoversion
