#include "merge_engine.h"

#include <algorithm>
#include <stdexcept>
#include <unordered_set>

namespace geoversion {
namespace merge {

namespace {

bool side_equal(bool present_a, const std::string& hash_a, bool present_b,
                const std::string& hash_b) {
    return present_a == present_b && (!present_a || hash_a == hash_b);
}

} // namespace

MergeResult merge_object_maps(const std::unordered_map<std::string, std::string>& base,
                              const std::unordered_map<std::string, std::string>& ours,
                              const std::unordered_map<std::string, std::string>& theirs,
                              MergeStrategy strategy) {
    MergeResult result;
    const std::string empty;

    std::unordered_set<std::string> ids;
    for (const auto& entry : base) {
        ids.insert(entry.first);
    }
    for (const auto& entry : ours) {
        ids.insert(entry.first);
    }
    for (const auto& entry : theirs) {
        ids.insert(entry.first);
    }

    for (const auto& id : ids) {
        auto base_it = base.find(id);
        auto ours_it = ours.find(id);
        auto theirs_it = theirs.find(id);

        bool in_base = base_it != base.end();
        bool in_ours = ours_it != ours.end();
        bool in_theirs = theirs_it != theirs.end();

        const std::string& base_hash = in_base ? base_it->second : empty;
        const std::string& ours_hash = in_ours ? ours_it->second : empty;
        const std::string& theirs_hash = in_theirs ? theirs_it->second : empty;

        if (side_equal(in_ours, ours_hash, in_theirs, theirs_hash)) {
            if (in_ours) {
                result.merged[id] = ours_hash;
            }
            continue;
        }
        if (side_equal(in_ours, ours_hash, in_base, base_hash)) {
            if (in_theirs) {
                result.merged[id] = theirs_hash;
            }
            continue;
        }
        if (side_equal(in_theirs, theirs_hash, in_base, base_hash)) {
            if (in_ours) {
                result.merged[id] = ours_hash;
            }
            continue;
        }

        ConflictType type = ConflictType::BothModified;
        if (!in_base && in_ours && in_theirs) {
            type = ConflictType::BothAdded;
        } else if (in_base && in_ours && in_theirs) {
            type = ConflictType::BothModified;
        } else {
            type = ConflictType::ModifiedDeleted;
        }

        if (strategy == MergeStrategy::Ours) {
            if (in_ours) {
                result.merged[id] = ours_hash;
            }
        } else if (strategy == MergeStrategy::Theirs) {
            if (in_theirs) {
                result.merged[id] = theirs_hash;
            }
        } else {
            Conflict conflict;
            conflict.object_id = id;
            conflict.base_hash = base_hash;
            conflict.ours_hash = ours_hash;
            conflict.theirs_hash = theirs_hash;
            conflict.type = type;
            result.conflicts.push_back(conflict);
        }
    }

    std::sort(result.conflicts.begin(), result.conflicts.end(),
              [](const Conflict& a, const Conflict& b) { return a.object_id < b.object_id; });

    return result;
}

MergeEngine::MergeEngine(storage::VersionStorage& version_storage)
    : version_storage_(version_storage) {}

MergeResult MergeEngine::merge(const std::string& base_version_id,
                               const std::string& ours_version_id,
                               const std::string& theirs_version_id, MergeStrategy strategy) {
    auto base = version_storage_.get_version(base_version_id);
    if (!base) {
        throw std::runtime_error("Version not found: " + base_version_id);
    }
    auto ours = version_storage_.get_version(ours_version_id);
    if (!ours) {
        throw std::runtime_error("Version not found: " + ours_version_id);
    }
    auto theirs = version_storage_.get_version(theirs_version_id);
    if (!theirs) {
        throw std::runtime_error("Version not found: " + theirs_version_id);
    }

    return merge_object_maps(base->objects, ours->objects, theirs->objects, strategy);
}

} // namespace merge
} // namespace geoversion
