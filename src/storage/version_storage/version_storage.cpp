#include "version_storage.h"

#include "storage/cas/cas.h"
#include "utils/uuid/uuid.h"

#include <bsoncxx/builder/basic/array.hpp>
#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/builder/basic/kvp.hpp>
#include <bsoncxx/types.hpp>
#include <mongocxx/options/find.hpp>

#include <chrono>
#include <stdexcept>

namespace geoversion {
namespace storage {

using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::make_document;

VersionStorage::VersionStorage(MongoDBConnection& connection) : connection_(connection) {}

std::string
VersionStorage::create_version(const std::string& situation_id,
                               const std::unordered_map<std::string, std::string>& objects,
                               const std::vector<std::string>& parent_version_ids,
                               const std::string& message, const std::string& author) {
    if (situation_id.empty()) {
        throw std::runtime_error("Version situation_id must not be empty");
    }

    std::string version_id = utils::generate_uuid();
    const auto now = bsoncxx::types::b_date{std::chrono::system_clock::now()};

    bsoncxx::builder::basic::array parents_arr;
    for (const auto& parent : parent_version_ids) {
        parents_arr.append(parent);
    }

    auto version_doc =
        make_document(kvp("version_id", version_id), kvp("situation_id", situation_id),
                      kvp("parent_version_ids", parents_arr), kvp("commit_message", message),
                      kvp("author", author), kvp("created_at", now));

    auto versions_coll = connection_.get_situation_versions_collection();
    auto inserted = versions_coll.insert_one(version_doc.view());
    if (!inserted || inserted->result().inserted_count() == 0) {
        throw std::runtime_error("Failed to insert version into database");
    }

    if (!objects.empty()) {
        std::vector<bsoncxx::document::value> object_docs;
        object_docs.reserve(objects.size());
        for (const auto& entry : objects) {
            object_docs.push_back(
                make_document(kvp("version_id", version_id), kvp("object_id", entry.first),
                              kvp("bpo_hash", entry.second), kvp("created_at", now)));
        }

        auto objects_coll = connection_.get_version_objects_collection();
        objects_coll.insert_many(object_docs);
    }

    return version_id;
}

std::optional<Version> VersionStorage::get_version(const std::string& version_id) const {
    auto versions_coll = connection_.get_situation_versions_collection();
    auto filter = make_document(kvp("version_id", version_id));

    auto result = versions_coll.find_one(filter.view());
    if (!result) {
        return std::nullopt;
    }

    Version version;
    version.meta = meta_from_document(result->view());
    version.objects = get_version_map(version_id);
    return version;
}

std::vector<VersionMeta> VersionStorage::list_versions(const std::string& situation_id) const {
    auto versions_coll = connection_.get_situation_versions_collection();
    auto filter = make_document(kvp("situation_id", situation_id));

    mongocxx::options::find opts;
    opts.sort(make_document(kvp("created_at", -1), kvp("_id", -1)));

    auto cursor = versions_coll.find(filter.view(), opts);

    std::vector<VersionMeta> result;
    for (auto&& doc : cursor) {
        result.push_back(meta_from_document(doc));
    }

    return result;
}

std::unordered_map<std::string, std::string>
VersionStorage::get_version_map(const std::string& version_id) const {
    auto objects_coll = connection_.get_version_objects_collection();
    auto filter = make_document(kvp("version_id", version_id));

    auto cursor = objects_coll.find(filter.view());

    std::unordered_map<std::string, std::string> result;
    for (auto&& doc : cursor) {
        if (doc["object_id"] && doc["bpo_hash"]) {
            result.emplace(std::string(doc["object_id"].get_string().value),
                           std::string(doc["bpo_hash"].get_string().value));
        }
    }

    return result;
}

std::vector<BPO> VersionStorage::checkout(const std::string& version_id) const {
    if (!get_version(version_id)) {
        throw std::runtime_error("Version not found: " + version_id);
    }

    auto version_map = get_version_map(version_id);

    CAS cas(connection_.get_bpo_cas_collection());

    std::vector<BPO> result;
    result.reserve(version_map.size());
    for (const auto& entry : version_map) {
        auto bpo = cas.retrieve(entry.second);
        if (!bpo) {
            throw std::runtime_error("BPO not found in CAS for hash: " + entry.second);
        }
        bpo->set_object_id(entry.first);
        result.push_back(std::move(*bpo));
    }

    return result;
}

VersionMeta VersionStorage::meta_from_document(const bsoncxx::document::view& doc) const {
    VersionMeta meta;

    if (doc["version_id"]) {
        meta.version_id = std::string(doc["version_id"].get_string().value);
    }
    if (doc["situation_id"]) {
        meta.situation_id = std::string(doc["situation_id"].get_string().value);
    }
    if (doc["parent_version_ids"]) {
        for (auto&& parent : doc["parent_version_ids"].get_array().value) {
            meta.parent_version_ids.push_back(std::string(parent.get_string().value));
        }
    }
    if (doc["commit_message"]) {
        meta.message = std::string(doc["commit_message"].get_string().value);
    }
    if (doc["author"]) {
        meta.author = std::string(doc["author"].get_string().value);
    }
    if (doc["created_at"]) {
        meta.created_at = std::chrono::system_clock::time_point{doc["created_at"].get_date().value};
    }

    return meta;
}

} // namespace storage
} // namespace geoversion
