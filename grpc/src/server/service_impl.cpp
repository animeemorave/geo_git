#include "service_impl.h"

#include <cstdlib>
#include <string>

namespace geogit_server {

namespace {

geoversion::diff::MatcherConfig matcher_config_from_env() {
    geoversion::diff::MatcherConfig config;

    if (const char* value = std::getenv("POINT_EPSILON_METERS"); value && value[0] != '\0') {
        config.point_epsilon_m = std::stod(value);
    }
    if (const char* value = std::getenv("LINE_EPSILON_METERS"); value && value[0] != '\0') {
        config.line_epsilon_m = std::stod(value);
    }
    if (const char* value = std::getenv("POLYGON_EPSILON_METERS"); value && value[0] != '\0') {
        config.polygon_epsilon_m = std::stod(value);
    }
    if (const char* value = std::getenv("ENTITY_RESOLUTION_THRESHOLD"); value && value[0] != '\0') {
        config.threshold = static_cast<std::size_t>(std::stoull(value));
    }

    return config;
}

} // namespace

GeoGitServiceImpl::GeoGitServiceImpl(geoversion::storage::MongoDBConnection& connection)
    : connection_(connection), cas_(connection.get_bpo_cas_collection()), situations_(connection),
      branches_(connection), versions_(connection), deltas_(connection),
      matcher_(connection.get_bpo_cas_collection(), matcher_config_from_env()) {}

} // namespace geogit_server
