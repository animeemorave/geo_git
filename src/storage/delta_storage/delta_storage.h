#pragma once

#include "diff/diff_result.h"
#include "storage/mongodb_connection/mongodb_connection.h"

#include <bsoncxx/document/view.hpp>
#include <optional>
#include <string>

namespace geoversion {
namespace storage {

class DeltaStorage {
public:
    explicit DeltaStorage(MongoDBConnection& connection);

    std::string store(const diff::DiffResult& diff, const std::string& from_version_id,
                      const std::string& to_version_id);

    std::optional<diff::DiffResult> get(const std::string& delta_id) const;

    std::optional<diff::DiffResult> find(const std::string& from_version_id,
                                         const std::string& to_version_id) const;

private:
    MongoDBConnection& connection_;

    diff::DiffResult load_items(const std::string& delta_id) const;
};

} // namespace storage
} // namespace geoversion
