#pragma once

#include "storage/mongodb_connection/mongodb_connection.h"

#include <bsoncxx/document/view.hpp>
#include <string>
#include <vector>
#include <chrono>
#include <optional>

namespace geoversion {
namespace storage {

struct Situation {
    std::string situation_id;
    std::string name;
    std::string description;
    std::chrono::system_clock::time_point created_at;
    std::chrono::system_clock::time_point updated_at;
};

class SituationStorage {
public:
    explicit SituationStorage(MongoDBConnection& connection);

    std::string create(const std::string& name, const std::string& description = "");
    std::optional<Situation> get(const std::string& situation_id) const;
    std::vector<Situation> list() const;
    void update(const std::string& situation_id,
                const std::string& name,
                const std::string& description);
    void remove(const std::string& situation_id);

private:
    MongoDBConnection& connection_;

    Situation from_document(const bsoncxx::document::view& doc) const;
};

}
}
