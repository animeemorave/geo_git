#include "status_engine.h"

#include "diff/diff_engine.h"

#include <stdexcept>

namespace geoversion {
namespace diff {

StatusEngine::StatusEngine(storage::BranchStorage& branch_storage,
                           storage::VersionStorage& version_storage)
    : branch_storage_(branch_storage), version_storage_(version_storage) {}

DiffResult
StatusEngine::compute_status(const std::string& branch_id,
                             const std::unordered_map<std::string, std::string>& proposed_objects) {
    auto branch = branch_storage_.get(branch_id);
    if (!branch) {
        throw std::runtime_error("Branch not found: " + branch_id);
    }

    std::unordered_map<std::string, std::string> head_map;
    if (branch->head_version_id) {
        head_map = version_storage_.get_version_map(*branch->head_version_id);
    }

    return diff_object_maps(head_map, proposed_objects);
}

} // namespace diff
} // namespace geoversion
