#include "delta_storage.h"

#include "utils/uuid/uuid.h"

#include <bsoncxx/builder/basic/array.hpp>
#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/builder/basic/kvp.hpp>
#include <bsoncxx/document/value.hpp>
#include <bsoncxx/types.hpp>
#include <mongocxx/collection.hpp>

#include <chrono>
#include <stdexcept>
#include <vector>

namespace geoversion {
namespace storage {

using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::make_document;

namespace {

constexpr std::size_t kItemBatchSize = 5000;

void flush_items(mongocxx::collection& items_coll, std::vector<bsoncxx::document::value>& batch) {
    if (batch.empty()) {
        return;
    }
    items_coll.insert_many(batch);
    batch.clear();
}

void append_plain_items(mongocxx::collection& items_coll, std::vector<bsoncxx::document::value>& batch,
                        const std::string& delta_id, const char* kind,
                        const std::vector<std::string>& values) {
    for (const auto& value : values) {
        batch.push_back(make_document(kvp("delta_id", delta_id), kvp("kind", kind), kvp("value", value)));
        if (batch.size() >= kItemBatchSize) {
            flush_items(items_coll, batch);
        }
    }
}

} // namespace

DeltaStorage::DeltaStorage(MongoDBConnection& connection) : connection_(connection) {}

std::string DeltaStorage::store(const diff::DiffResult& diff, const std::string& from_version_id,
                                const std::string& to_version_id) {
    if (from_version_id.empty() || to_version_id.empty()) {
        throw std::runtime_error("Delta from_version_id and to_version_id must not be empty");
    }

    auto collection = connection_.get_version_deltas_collection();

    auto existing_filter =
        make_document(kvp("from_version_id", from_version_id), kvp("to_version_id", to_version_id));
    auto existing = collection.find_one(existing_filter.view());
    if (existing && existing->view()["delta_id"]) {
        return std::string(existing->view()["delta_id"].get_string().value);
    }

    std::string delta_id = utils::generate_uuid();
    const auto now = bsoncxx::types::b_date{std::chrono::system_clock::now()};

    auto doc = make_document(kvp("delta_id", delta_id), kvp("from_version_id", from_version_id),
                             kvp("to_version_id", to_version_id), kvp("created_at", now));

    auto inserted = collection.insert_one(doc.view());
    if (!inserted || inserted->result().inserted_count() == 0) {
        throw std::runtime_error("Failed to insert delta into database");
    }

    auto items_coll = connection_.get_delta_items_collection();
    std::vector<bsoncxx::document::value> batch;
    batch.reserve(kItemBatchSize);

    append_plain_items(items_coll, batch, delta_id, "added", diff.added);
    append_plain_items(items_coll, batch, delta_id, "removed", diff.removed);
    append_plain_items(items_coll, batch, delta_id, "modified", diff.modified);
    append_plain_items(items_coll, batch, delta_id, "unchanged", diff.unchanged);

    for (const auto& pair : diff.likely_modified) {
        batch.push_back(make_document(
            kvp("delta_id", delta_id), kvp("kind", "likely_modified"),
            kvp("removed_hash", pair.removed_hash), kvp("added_hash", pair.added_hash),
            kvp("confidence", pair.confidence), kvp("distance_m", pair.distance_m)));
        if (batch.size() >= kItemBatchSize) {
            flush_items(items_coll, batch);
        }
    }
    flush_items(items_coll, batch);

    return delta_id;
}

std::optional<diff::DiffResult> DeltaStorage::get(const std::string& delta_id) const {
    auto collection = connection_.get_version_deltas_collection();
    auto filter = make_document(kvp("delta_id", delta_id));

    auto result = collection.find_one(filter.view());
    if (!result) {
        return std::nullopt;
    }

    return load_items(delta_id);
}

std::optional<diff::DiffResult> DeltaStorage::find(const std::string& from_version_id,
                                                   const std::string& to_version_id) const {
    auto collection = connection_.get_version_deltas_collection();
    auto filter =
        make_document(kvp("from_version_id", from_version_id), kvp("to_version_id", to_version_id));

    auto result = collection.find_one(filter.view());
    if (!result) {
        return std::nullopt;
    }

    auto delta_id = std::string(result->view()["delta_id"].get_string().value);
    return load_items(delta_id);
}

diff::DiffResult DeltaStorage::load_items(const std::string& delta_id) const {
    diff::DiffResult diff;

    auto items_coll = connection_.get_delta_items_collection();
    auto filter = make_document(kvp("delta_id", delta_id));
    auto cursor = items_coll.find(filter.view());

    for (auto&& doc : cursor) {
        if (!doc["kind"]) {
            continue;
        }
        std::string kind = std::string(doc["kind"].get_string().value);
        if (kind == "likely_modified") {
            diff::DiffResult::SimilarityPair pair;
            if (doc["removed_hash"]) {
                pair.removed_hash = std::string(doc["removed_hash"].get_string().value);
            }
            if (doc["added_hash"]) {
                pair.added_hash = std::string(doc["added_hash"].get_string().value);
            }
            if (doc["confidence"]) {
                pair.confidence = doc["confidence"].get_double().value;
            }
            if (doc["distance_m"]) {
                pair.distance_m = doc["distance_m"].get_double().value;
            }
            diff.likely_modified.push_back(pair);
            continue;
        }

        if (!doc["value"]) {
            continue;
        }
        std::string value = std::string(doc["value"].get_string().value);
        if (kind == "added") {
            diff.added.push_back(std::move(value));
        } else if (kind == "removed") {
            diff.removed.push_back(std::move(value));
        } else if (kind == "modified") {
            diff.modified.push_back(std::move(value));
        } else if (kind == "unchanged") {
            diff.unchanged.push_back(std::move(value));
        }
    }

    return diff;
}

} // namespace storage
} // namespace geoversion
