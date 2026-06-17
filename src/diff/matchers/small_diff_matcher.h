#pragma once

#include "diff/entity_matcher.h"

#include <mongocxx/collection.hpp>

namespace geoversion {
namespace diff {

class SmallDiffMatcher : public EntityMatcher {
public:
    explicit SmallDiffMatcher(mongocxx::collection collection, MatcherConfig config = {});

    std::vector<DiffResult::SimilarityPair>
    match(const std::vector<GeoCandidate>& removed,
          const std::vector<GeoCandidate>& added) const override;

private:
    mongocxx::collection collection_;
    MatcherConfig config_;
};

} // namespace diff
} // namespace geoversion
