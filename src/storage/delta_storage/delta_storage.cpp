#include "delta_storage.h"

#include "utils/uuid/uuid.h"

#include <bsoncxx/builder/basic/array.hpp>
#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/builder/basic/kvp.hpp>
#include <bsoncxx/types.hpp>

#include <chrono>
#include <stdexcept>

namespace geoversion {
namespace storage {

using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::make_document;

namespace {

bsoncxx::builder::basic::array to_string_array(const std::vector<std::string>& values) {
    bsoncxx::builder::basic::array arr;
    for (const auto& value : values) {
        arr.append(value);
    }
    return arr;
}

std::vector<std::string> read_string_array(const bsoncxx::document::view& doc, const char* field) {
    std::vector<std::string> result;
    if (doc[field]) {
        for (auto&& element : doc[field].get_array().value) {
            result.push_back(std::string(element.get_string().value));
        }
    }
    return result;
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

    bsoncxx::builder::basic::array likely_modified_arr;
    for (const auto& pair : diff.likely_modified) {
        likely_modified_arr.append(make_document(
            kvp("removed_hash", pair.removed_hash), kvp("added_hash", pair.added_hash),
            kvp("confidence", pair.confidence), kvp("distance_m", pair.distance_m)));
    }

    auto doc = make_document(kvp("delta_id", delta_id), kvp("from_version_id", from_version_id),
                             kvp("to_version_id", to_version_id),
                             kvp("added", to_string_array(diff.added)),
                             kvp("removed", to_string_array(diff.removed)),
                             kvp("modified", to_string_array(diff.modified)),
                             kvp("unchanged", to_string_array(diff.unchanged)),
                             kvp("likely_modified", likely_modified_arr), kvp("created_at", now));

    auto inserted = collection.insert_one(doc.view());
    if (!inserted || inserted->result().inserted_count() == 0) {
        throw std::runtime_error("Failed to insert delta into database");
    }

    return delta_id;
}

std::optional<diff::DiffResult> DeltaStorage::get(const std::string& delta_id) const {
    auto collection = connection_.get_version_deltas_collection();
    auto filter = make_document(kvp("delta_id", delta_id));

    auto result = collection.find_one(filter.view());
    if (!result) {
        return std::nullopt;
    }

    return from_document(result->view());
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

    return from_document(result->view());
}

diff::DiffResult DeltaStorage::from_document(const bsoncxx::document::view& doc) const {
    diff::DiffResult diff;

    diff.added = read_string_array(doc, "added");
    diff.removed = read_string_array(doc, "removed");
    diff.modified = read_string_array(doc, "modified");
    diff.unchanged = read_string_array(doc, "unchanged");

    if (doc["likely_modified"]) {
        for (auto&& element : doc["likely_modified"].get_array().value) {
            auto pair_doc = element.get_document().value;
            diff::DiffResult::SimilarityPair pair;
            if (pair_doc["removed_hash"]) {
                pair.removed_hash = std::string(pair_doc["removed_hash"].get_string().value);
            }
            if (pair_doc["added_hash"]) {
                pair.added_hash = std::string(pair_doc["added_hash"].get_string().value);
            }
            if (pair_doc["confidence"]) {
                pair.confidence = pair_doc["confidence"].get_double().value;
            }
            if (pair_doc["distance_m"]) {
                pair.distance_m = pair_doc["distance_m"].get_double().value;
            }
            diff.likely_modified.push_back(pair);
        }
    }

    return diff;
}

} // namespace storage
} // namespace geoversion
