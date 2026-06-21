#include "diff_engine.h"

#include "diff/entity_matcher.h"
#include "diff/geometry_utils.h"
#include "storage/bpo_storage/bpo_storage.h"
#include "storage/cas/cas.h"
#include "storage/delta_storage/delta_storage.h"

#include <algorithm>
#include <stdexcept>
#include <utility>
#include <vector>

namespace geoversion {
namespace diff {

namespace {

std::vector<GeoCandidate> build_candidates(const std::vector<std::string>& object_ids,
                                           const std::unordered_map<std::string, std::string>& map,
                                           storage::CAS& cas) {
    std::vector<std::string> hashes;
    hashes.reserve(object_ids.size());
    for (const auto& object_id : object_ids) {
        auto it = map.find(object_id);
        if (it != map.end()) {
            hashes.push_back(it->second);
        }
    }

    auto bpos_by_hash = cas.retrieve_many(hashes);

    std::vector<GeoCandidate> candidates;
    candidates.reserve(object_ids.size());
    for (const auto& object_id : object_ids) {
        auto map_it = map.find(object_id);
        if (map_it == map.end()) {
            continue;
        }

        auto bpo_it = bpos_by_hash.find(map_it->second);
        if (bpo_it == bpos_by_hash.end()) {
            continue;
        }
        const storage::BPO& bpo = bpo_it->second;

        RepresentativePoint point = representative_point(bpo.get_geometry(), bpo.get_geometry_type());
        if (!point.valid) {
            continue;
        }

        GeoCandidate candidate;
        candidate.object_id = object_id;
        candidate.hash = map_it->second;
        candidate.lon = point.lon;
        candidate.lat = point.lat;
        candidate.type = bpo.get_geometry_type();
        candidates.push_back(std::move(candidate));
    }
    return candidates;
}

} // namespace

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

void DiffEngine::set_entity_matcher(storage::CAS& cas, const EntityMatcher& matcher) {
    cas_ = &cas;
    matcher_ = &matcher;
}

DiffResult DiffEngine::compute(const std::string& from_version_id,
                               const std::string& to_version_id) {
    bool entity_resolution = matcher_ != nullptr && cas_ != nullptr;

    if (delta_storage_ && !entity_resolution) {
        auto cached = delta_storage_->find(from_version_id, to_version_id);
        if (cached) {
            return *cached;
        }
    }

    auto from = version_storage_.get_version(from_version_id);
    if (!from) {
        throw std::runtime_error("Version not found: " + from_version_id);
    }
    auto to = version_storage_.get_version(to_version_id);
    if (!to) {
        throw std::runtime_error("Version not found: " + to_version_id);
    }

    DiffResult result = diff_object_maps(from->objects, to->objects);

    if (entity_resolution) {
        compute_level2(from->objects, to->objects, result);
    }

    if (delta_storage_) {
        delta_storage_->store(result, from_version_id, to_version_id);
    }

    return result;
}

void DiffEngine::compute_level2(const std::unordered_map<std::string, std::string>& from_map,
                                const std::unordered_map<std::string, std::string>& to_map,
                                DiffResult& result) const {
    std::vector<GeoCandidate> removed = build_candidates(result.removed, from_map, *cas_);
    std::vector<GeoCandidate> added = build_candidates(result.added, to_map, *cas_);
    result.likely_modified = matcher_->match(removed, added);
}

} // namespace diff
} // namespace geoversion
