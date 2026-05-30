#pragma once

#include <string>
#include <vector>

namespace geoversion {
namespace diff {

struct DiffResult {
    std::vector<std::string> added;
    std::vector<std::string> removed;
    std::vector<std::string> modified;
    std::vector<std::string> unchanged;

    struct SimilarityPair {
        std::string removed_hash;
        std::string added_hash;
        double confidence = 0.0;
        double distance_m = 0.0;
    };

    std::vector<SimilarityPair> likely_modified;
};

} // namespace diff
} // namespace geoversion
