#include "diff_engine.h"

#include "storage/delta_storage/delta_storage.h"

#include <algorithm>
#include <stdexcept>

namespace geoversion {
namespace diff {

DiffResult diff_object_maps(const std::unordered_map<std::string, std::string>& from,
                            const std::unordered_map<std::string, std::string>& to) {
    DiffResult result;

    for (const auto& entry : to) {
        auto it = from.find(entry.first);
        if (it == from.end()) {
            result.added.push_back(entry.first);
        } else if (it->second != entry.second) {
            result.modified.push_back(entry.first);
        } else {
            result.unchanged.push_back(entry.first);
        }
    }

    for (const auto& entry : from) {
        if (to.find(entry.first) == to.end()) {
            result.removed.push_back(entry.first);
        }
    }

    std::sort(result.added.begin(), result.added.end());
    std::sort(result.removed.begin(), result.removed.end());
    std::sort(result.modified.begin(), result.modified.end());
    std::sort(result.unchanged.begin(), result.unchanged.end());

    return result;
}

DiffEngine::DiffEngine(storage::VersionStorage& version_storage)
    : version_storage_(version_storage) {}

DiffEngine::DiffEngine(storage::VersionStorage& version_storage,
                       storage::DeltaStorage& delta_storage)
    : version_storage_(version_storage), delta_storage_(&delta_storage) {}

DiffResult DiffEngine::compute(const std::string& from_version_id,
                               const std::string& to_version_id) {
    if (delta_storage_) {
        auto cached = delta_storage_->find(from_version_id, to_version_id);
        if (cached) {
            return *cached;
        }
    }

    DiffResult result = compute_level1(from_version_id, to_version_id);

    if (delta_storage_) {
        delta_storage_->store(result, from_version_id, to_version_id);
    }

    return result;
}

DiffResult DiffEngine::compute_level1(const std::string& from_version_id,
                                      const std::string& to_version_id) const {
    auto from = version_storage_.get_version(from_version_id);
    if (!from) {
        throw std::runtime_error("Version not found: " + from_version_id);
    }
    auto to = version_storage_.get_version(to_version_id);
    if (!to) {
        throw std::runtime_error("Version not found: " + to_version_id);
    }

    return diff_object_maps(from->objects, to->objects);
}

} // namespace diff
} // namespace geoversion
