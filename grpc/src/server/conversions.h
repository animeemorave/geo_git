#pragma once

#include "geo_git.pb.h"

#include "diff/diff_result.h"
#include "storage/bpo_storage/bpo_storage.h"
#include "storage/branch_storage/branch_storage.h"
#include "storage/situation_storage/situation_storage.h"
#include "storage/version_storage/version_storage.h"

#include <bsoncxx/json.hpp>

#include <chrono>
#include <cstdint>

namespace geogit_server {

inline int64_t to_millis(std::chrono::system_clock::time_point time_point) {
    return std::chrono::duration_cast<std::chrono::milliseconds>(time_point.time_since_epoch())
        .count();
}

inline void fill_situation(const geoversion::storage::Situation& src, geogit::Situation* dst) {
    dst->set_situation_id(src.situation_id);
    dst->set_name(src.name);
    dst->set_description(src.description);
    dst->set_created_at(to_millis(src.created_at));
    dst->set_updated_at(to_millis(src.updated_at));
}

inline void fill_branch(const geoversion::storage::Branch& src, geogit::Branch* dst) {
    dst->set_branch_id(src.branch_id);
    dst->set_situation_id(src.situation_id);
    dst->set_name(src.name);
    dst->set_head_version_id(src.head_version_id.value_or(""));
    dst->set_created_at(to_millis(src.created_at));
    dst->set_updated_at(to_millis(src.updated_at));
}

inline void fill_version_meta(const geoversion::storage::VersionMeta& src,
                              geogit::VersionMeta* dst) {
    dst->set_version_id(src.version_id);
    dst->set_situation_id(src.situation_id);
    for (const auto& parent : src.parent_version_ids) {
        dst->add_parent_version_ids(parent);
    }
    dst->set_message(src.message);
    dst->set_author(src.author);
    dst->set_created_at(to_millis(src.created_at));
}

inline void fill_version(const geoversion::storage::Version& src, geogit::Version* dst) {
    fill_version_meta(src.meta, dst->mutable_meta());
    auto* objects = dst->mutable_objects();
    for (const auto& entry : src.objects) {
        (*objects)[entry.first] = entry.second;
    }
}

inline void fill_bpo(const geoversion::storage::BPO& src, geogit::BPO* dst) {
    dst->set_hash(src.get_hash());
    auto object_id = src.get_object_id();
    if (object_id) {
        dst->set_object_id(*object_id);
    }
    dst->set_geometry_json(bsoncxx::to_json(src.get_geometry()));
    dst->set_attributes_json(bsoncxx::to_json(src.get_attributes()));
}

inline void fill_diff(const geoversion::diff::DiffResult& src, geogit::DiffResponse* dst) {
    for (const auto& id : src.added) {
        dst->add_added(id);
    }
    for (const auto& id : src.removed) {
        dst->add_removed(id);
    }
    for (const auto& id : src.modified) {
        dst->add_modified(id);
    }
    for (const auto& id : src.unchanged) {
        dst->add_unchanged(id);
    }
    for (const auto& pair : src.likely_modified) {
        auto* similarity = dst->add_likely_modified();
        similarity->set_removed_hash(pair.removed_hash);
        similarity->set_added_hash(pair.added_hash);
        similarity->set_confidence(pair.confidence);
        similarity->set_distance_m(pair.distance_m);
    }
}

} // namespace geogit_server
