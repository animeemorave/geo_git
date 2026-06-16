#pragma once

#include "storage/mongodb_connection/mongodb_connection.h"

#include <bsoncxx/document/view.hpp>
#include <chrono>
#include <optional>
#include <string>
#include <vector>

namespace geoversion {
namespace storage {

struct Branch {
    std::string branch_id;
    std::string situation_id;
    std::string name;
    std::optional<std::string> head_version_id;
    std::chrono::system_clock::time_point created_at;
    std::chrono::system_clock::time_point updated_at;
};

class BranchStorage {
public:
    explicit BranchStorage(MongoDBConnection& connection);

    std::string create(const std::string& situation_id, const std::string& name,
                       const std::optional<std::string>& head_version_id = std::nullopt);

    std::optional<Branch> get(const std::string& branch_id) const;

    std::optional<Branch> get_by_name(const std::string& situation_id,
                                      const std::string& name) const;

    std::vector<Branch> list(const std::string& situation_id) const;

    void advance(const std::string& branch_id, const std::string& new_version_id);

    void remove(const std::string& branch_id);

private:
    MongoDBConnection& connection_;

    Branch from_document(const bsoncxx::document::view& doc) const;
};

} // namespace storage
} // namespace geoversion
