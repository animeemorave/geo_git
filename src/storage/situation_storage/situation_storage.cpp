#include "situation_storage.h"
#include "utils/uuid/uuid.h"

#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/builder/basic/array.hpp>
#include <bsoncxx/builder/basic/kvp.hpp>
#include <bsoncxx/builder/stream/document.hpp>
#include <bsoncxx/types.hpp>
#include <mongocxx/options/find.hpp>

#include <stdexcept>
#include <chrono>

namespace geoversion {
namespace storage {

using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::make_document;

SituationStorage::SituationStorage(MongoDBConnection& connection) : connection_(connection) {}

std::string SituationStorage::create(const std::string& name, const std::string& description) {
    if (name.empty()) {
        throw std::runtime_error("Situation name must not be empty");
    }

    std::string situation_id = utils::generate_uuid();
    const auto now = bsoncxx::types::b_date{std::chrono::system_clock::now()};

    auto doc = make_document(kvp("situation_id", situation_id), kvp("name", name),
                             kvp("description", description), kvp("created_at", now),
                             kvp("updated_at", now));

    auto collection = connection_.get_situations_collection();
    auto result = collection.insert_one(doc.view());

    if (!result || result->result().inserted_count() == 0) {
        throw std::runtime_error("Failed to insert situation into database");
    }

    return situation_id;
}

std::optional<Situation> SituationStorage::get(const std::string& situation_id) const {
    auto collection = connection_.get_situations_collection();
    auto filter = make_document(kvp("situation_id", situation_id));

    auto result = collection.find_one(filter.view());
    if (!result) {
        return std::nullopt;
    }

    return from_document(result->view());
}

std::vector<Situation> SituationStorage::list() const {
    auto collection = connection_.get_situations_collection();

    mongocxx::options::find opts;
    opts.sort(make_document(kvp("created_at", 1)));

    auto cursor = collection.find({}, opts);

    std::vector<Situation> result;
    for (auto&& doc : cursor) {
        result.push_back(from_document(doc));
    }

    return result;
}

void SituationStorage::update(const std::string& situation_id, const std::string& name,
                              const std::string& description) {
    if (name.empty()) {
        throw std::runtime_error("Situation name must not be empty");
    }

    auto collection = connection_.get_situations_collection();
    auto filter = make_document(kvp("situation_id", situation_id));

    auto update_doc = make_document(
        kvp("$set", make_document(kvp("name", name), kvp("description", description),
                                  kvp("updated_at",
                                      bsoncxx::types::b_date{std::chrono::system_clock::now()}))));

    auto result = collection.update_one(filter.view(), update_doc.view());

    if (!result || result->matched_count() == 0) {
        throw std::runtime_error("Situation not found: " + situation_id);
    }
}

void SituationStorage::remove(const std::string& situation_id) {
    if (!get(situation_id)) {
        throw std::runtime_error("Situation not found: " + situation_id);
    }

    auto versions_coll = connection_.get_situation_versions_collection();
    auto version_filter = make_document(kvp("situation_id", situation_id));

    bsoncxx::builder::basic::array version_ids_arr;
    {
        auto cursor = versions_coll.find(version_filter.view());
        for (auto&& doc : cursor) {
            if (doc["version_id"]) {
                version_ids_arr.append(std::string(doc["version_id"].get_string().value));
            }
        }
    }

    auto objects_coll = connection_.get_version_objects_collection();
    auto objects_filter =
        make_document(kvp("version_id", make_document(kvp("$in", version_ids_arr.view()))));
    objects_coll.delete_many(objects_filter.view());

    versions_coll.delete_many(version_filter.view());

    auto situations_coll = connection_.get_situations_collection();
    situations_coll.delete_one(make_document(kvp("situation_id", situation_id)).view());
}

Situation SituationStorage::from_document(const bsoncxx::document::view& doc) const {
    Situation s;

    if (doc["situation_id"]) {
        s.situation_id = std::string(doc["situation_id"].get_string().value);
    }
    if (doc["name"]) {
        s.name = std::string(doc["name"].get_string().value);
    }
    if (doc["description"]) {
        s.description = std::string(doc["description"].get_string().value);
    }
    if (doc["created_at"]) {
        s.created_at = std::chrono::system_clock::time_point{doc["created_at"].get_date().value};
    }
    if (doc["updated_at"]) {
        s.updated_at = std::chrono::system_clock::time_point{doc["updated_at"].get_date().value};
    }

    return s;
}

} // namespace storage
} // namespace geoversion
