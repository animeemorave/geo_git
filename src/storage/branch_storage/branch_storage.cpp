#include "branch_storage.h"

#include "utils/uuid/uuid.h"

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

BranchStorage::BranchStorage(MongoDBConnection& connection) : connection_(connection) {}

std::string BranchStorage::create(const std::string& situation_id, const std::string& name,
                                  const std::optional<std::string>& head_version_id) {
    if (situation_id.empty()) {
        throw std::runtime_error("Branch situation_id must not be empty");
    }
    if (name.empty()) {
        throw std::runtime_error("Branch name must not be empty");
    }
    if (get_by_name(situation_id, name)) {
        throw std::runtime_error("Branch already exists: " + name);
    }

    std::string branch_id = utils::generate_uuid();
    const auto now = bsoncxx::types::b_date{std::chrono::system_clock::now()};

    bsoncxx::builder::basic::document doc;
    doc.append(kvp("branch_id", branch_id));
    doc.append(kvp("situation_id", situation_id));
    doc.append(kvp("name", name));
    if (head_version_id) {
        doc.append(kvp("head_version_id", *head_version_id));
    }
    doc.append(kvp("created_at", now));
    doc.append(kvp("updated_at", now));

    auto collection = connection_.get_branches_collection();
    auto result = collection.insert_one(doc.view());

    if (!result || result->result().inserted_count() == 0) {
        throw std::runtime_error("Failed to insert branch into database");
    }

    return branch_id;
}

std::optional<Branch> BranchStorage::get(const std::string& branch_id) const {
    auto collection = connection_.get_branches_collection();
    auto filter = make_document(kvp("branch_id", branch_id));

    auto result = collection.find_one(filter.view());
    if (!result) {
        return std::nullopt;
    }

    return from_document(result->view());
}

std::optional<Branch> BranchStorage::get_by_name(const std::string& situation_id,
                                                 const std::string& name) const {
    auto collection = connection_.get_branches_collection();
    auto filter = make_document(kvp("situation_id", situation_id), kvp("name", name));

    auto result = collection.find_one(filter.view());
    if (!result) {
        return std::nullopt;
    }

    return from_document(result->view());
}

std::vector<Branch> BranchStorage::list(const std::string& situation_id) const {
    auto collection = connection_.get_branches_collection();
    auto filter = make_document(kvp("situation_id", situation_id));

    mongocxx::options::find opts;
    opts.sort(make_document(kvp("created_at", 1), kvp("_id", 1)));

    auto cursor = collection.find(filter.view(), opts);

    std::vector<Branch> result;
    for (auto&& doc : cursor) {
        result.push_back(from_document(doc));
    }

    return result;
}

void BranchStorage::advance(const std::string& branch_id, const std::string& new_version_id) {
    if (new_version_id.empty()) {
        throw std::runtime_error("Branch new_version_id must not be empty");
    }

    auto collection = connection_.get_branches_collection();
    auto filter = make_document(kvp("branch_id", branch_id));

    auto update_doc = make_document(
        kvp("$set", make_document(kvp("head_version_id", new_version_id),
                                  kvp("updated_at",
                                      bsoncxx::types::b_date{std::chrono::system_clock::now()}))));

    auto result = collection.update_one(filter.view(), update_doc.view());

    if (!result || result->matched_count() == 0) {
        throw std::runtime_error("Branch not found: " + branch_id);
    }
}

void BranchStorage::remove(const std::string& branch_id) {
    auto collection = connection_.get_branches_collection();
    auto filter = make_document(kvp("branch_id", branch_id));

    auto result = collection.delete_one(filter.view());

    if (!result || result->deleted_count() == 0) {
        throw std::runtime_error("Branch not found: " + branch_id);
    }
}

Branch BranchStorage::from_document(const bsoncxx::document::view& doc) const {
    Branch branch;

    if (doc["branch_id"]) {
        branch.branch_id = std::string(doc["branch_id"].get_string().value);
    }
    if (doc["situation_id"]) {
        branch.situation_id = std::string(doc["situation_id"].get_string().value);
    }
    if (doc["name"]) {
        branch.name = std::string(doc["name"].get_string().value);
    }
    if (doc["head_version_id"]) {
        branch.head_version_id = std::string(doc["head_version_id"].get_string().value);
    }
    if (doc["created_at"]) {
        branch.created_at =
            std::chrono::system_clock::time_point{doc["created_at"].get_date().value};
    }
    if (doc["updated_at"]) {
        branch.updated_at =
            std::chrono::system_clock::time_point{doc["updated_at"].get_date().value};
    }

    return branch;
}

} // namespace storage
} // namespace geoversion
