#pragma once

#include "diff/diff_result.h"
#include "storage/version_storage/version_storage.h"

#include <string>
#include <unordered_map>

namespace geoversion {
namespace storage {
class DeltaStorage;
}

namespace diff {

DiffResult diff_object_maps(const std::unordered_map<std::string, std::string>& from,
                            const std::unordered_map<std::string, std::string>& to);

class DiffEngine {
public:
    explicit DiffEngine(storage::VersionStorage& version_storage);
    DiffEngine(storage::VersionStorage& version_storage, storage::DeltaStorage& delta_storage);

    DiffResult compute(const std::string& from_version_id, const std::string& to_version_id);

private:
    storage::VersionStorage& version_storage_;
    storage::DeltaStorage* delta_storage_ = nullptr;

    DiffResult compute_level1(const std::string& from_version_id,
                              const std::string& to_version_id) const;
};

} // namespace diff
} // namespace geoversion
