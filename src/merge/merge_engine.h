#pragma once

#include "storage/version_storage/version_storage.h"

#include <string>
#include <unordered_map>
#include <vector>

namespace geoversion {
namespace merge {

enum class ConflictType { BothModified, ModifiedDeleted, BothAdded };

enum class MergeStrategy { Manual, Ours, Theirs };

struct Conflict {
    std::string object_id;
    std::string base_hash;
    std::string ours_hash;
    std::string theirs_hash;
    ConflictType type = ConflictType::BothModified;
};

struct MergeResult {
    std::unordered_map<std::string, std::string> merged;
    std::vector<Conflict> conflicts;
};

MergeResult merge_object_maps(const std::unordered_map<std::string, std::string>& base,
                              const std::unordered_map<std::string, std::string>& ours,
                              const std::unordered_map<std::string, std::string>& theirs,
                              MergeStrategy strategy = MergeStrategy::Manual);

class MergeEngine {
public:
    explicit MergeEngine(storage::VersionStorage& version_storage);

    MergeResult merge(const std::string& base_version_id, const std::string& ours_version_id,
                      const std::string& theirs_version_id,
                      MergeStrategy strategy = MergeStrategy::Manual);

private:
    storage::VersionStorage& version_storage_;
};

} // namespace merge
} // namespace geoversion
