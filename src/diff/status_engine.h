#pragma once

#include "diff/diff_result.h"
#include "storage/branch_storage/branch_storage.h"
#include "storage/version_storage/version_storage.h"

#include <string>
#include <unordered_map>

namespace geoversion {
namespace diff {

class StatusEngine {
public:
    StatusEngine(storage::BranchStorage& branch_storage, storage::VersionStorage& version_storage);

    DiffResult compute_status(const std::string& branch_id,
                              const std::unordered_map<std::string, std::string>& proposed_objects);

private:
    storage::BranchStorage& branch_storage_;
    storage::VersionStorage& version_storage_;
};

} // namespace diff
} // namespace geoversion
