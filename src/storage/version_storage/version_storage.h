#pragma once

#include "storage/bpo_storage/bpo_storage.h"
#include "storage/mongodb_connection/mongodb_connection.h"

#include <bsoncxx/document/view.hpp>
#include <chrono>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace geoversion {
namespace storage {

struct VersionMeta {
    std::string version_id;
    std::string situation_id;
    std::vector<std::string> parent_version_ids;
    std::string message;
    std::string author;
    std::chrono::system_clock::time_point created_at;
};

struct Version {
    VersionMeta meta;
    std::unordered_map<std::string, std::string> objects;
};

class VersionStorage {
public:
    explicit VersionStorage(MongoDBConnection& connection);

    std::string create_version(const std::string& situation_id,
                               const std::unordered_map<std::string, std::string>& objects,
                               const std::vector<std::string>& parent_version_ids = {},
                               const std::string& message = "", const std::string& author = "");

    std::optional<Version> get_version(const std::string& version_id) const;

    std::vector<VersionMeta> list_versions(const std::string& situation_id) const;

    std::unordered_map<std::string, std::string>
    get_version_map(const std::string& version_id) const;

    std::vector<BPO> checkout(const std::string& version_id) const;

private:
    MongoDBConnection& connection_;

    VersionMeta meta_from_document(const bsoncxx::document::view& doc) const;
};

} // namespace storage
} // namespace geoversion
