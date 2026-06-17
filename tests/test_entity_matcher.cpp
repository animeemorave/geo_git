#include <cmath>
#include <cstdlib>
#include <iostream>
#include <string>
#include <unordered_map>
#include <vector>

#include <bsoncxx/builder/basic/array.hpp>
#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/builder/basic/kvp.hpp>
#include <bsoncxx/builder/stream/document.hpp>

#include "diff/diff_engine.h"
#include "diff/entity_matcher.h"
#include "diff/geometry_utils.h"
#include "diff/matchers/adaptive_matcher.h"
#include "diff/matchers/large_diff_matcher.h"
#include "diff/matchers/small_diff_matcher.h"
#include "storage/bpo_storage/bpo_storage.h"
#include "storage/cas/cas.h"
#include "storage/mongodb_connection/mongodb_connection.h"
#include "storage/version_storage/version_storage.h"

using namespace geoversion;
using namespace geoversion::storage;
using namespace geoversion::diff;

using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::make_array;
using bsoncxx::builder::basic::make_document;

static void assert_true(bool condition, const std::string& message) {
    if (!condition) {
        std::cerr << "TEST FAILED: " << message << std::endl;
        std::exit(EXIT_FAILURE);
    }
}

static std::string get_mongo_uri() {
    const char* uri = std::getenv("MONGODB_URI");
    if (uri && uri[0] != '\0') {
        return std::string(uri);
    }
    return std::string("mongodb://mongodb:27017");
}

static void clear_collections(MongoDBConnection& conn) {
    bsoncxx::builder::stream::document empty_filter;
    conn.get_situation_versions_collection().delete_many(empty_filter.view());
    conn.get_version_objects_collection().delete_many(empty_filter.view());
    conn.get_bpo_cas_collection().delete_many(empty_filter.view());
}

static bsoncxx::document::value make_point_geometry(double lon, double lat) {
    return make_document(kvp("type", "Point"), kvp("coordinates", make_array(lon, lat)));
}

static GeoCandidate make_candidate(const std::string& id, const std::string& hash, double lon,
                                   double lat, GeometryType type) {
    GeoCandidate candidate;
    candidate.object_id = id;
    candidate.hash = hash;
    candidate.lon = lon;
    candidate.lat = lat;
    candidate.type = type;
    return candidate;
}

static std::string store_point(CAS& cas, double lon, double lat, const std::string& cls) {
    auto geometry = make_point_geometry(lon, lat);
    auto attributes = make_document(kvp("class", cls));
    BPO bpo("", geometry.view(), attributes.view());
    std::string hash = cas.compute_hash(bpo);
    bpo.set_hash(hash);
    cas.store(bpo);
    return hash;
}

void test_haversine_known_distance() {
    double meters = haversine_meters(0.0, 0.0, 0.0, 1.0);
    assert_true(std::fabs(meters - 111195.0) < 100.0, "haversine: one degree latitude ~111195 m");

    double zero = haversine_meters(30.0, 60.0, 30.0, 60.0);
    assert_true(zero < 1e-6, "haversine: identical points distance is zero");
}

void test_representative_point_types() {
    auto point = make_point_geometry(30.0, 60.0);
    RepresentativePoint rp_point = representative_point(point.view(), GeometryType::Point);
    assert_true(rp_point.valid, "rep-point: point should be valid");
    assert_true(std::fabs(rp_point.lon - 30.0) < 1e-9 && std::fabs(rp_point.lat - 60.0) < 1e-9,
                "rep-point: point coordinates preserved");

    bsoncxx::builder::basic::array line_coords;
    line_coords.append(make_array(0.0, 0.0));
    line_coords.append(make_array(2.0, 0.0));
    auto line = make_document(kvp("type", "LineString"), kvp("coordinates", line_coords));
    RepresentativePoint rp_line = representative_point(line.view(), GeometryType::LineString);
    assert_true(rp_line.valid, "rep-point: line should be valid");
    assert_true(std::fabs(rp_line.lon - 1.0) < 1e-6 && std::fabs(rp_line.lat - 0.0) < 1e-6,
                "rep-point: line centroid at midpoint");

    bsoncxx::builder::basic::array ring;
    ring.append(make_array(0.0, 0.0));
    ring.append(make_array(0.0, 2.0));
    ring.append(make_array(2.0, 2.0));
    ring.append(make_array(2.0, 0.0));
    ring.append(make_array(0.0, 0.0));
    bsoncxx::builder::basic::array polygon_coords;
    polygon_coords.append(ring);
    auto polygon = make_document(kvp("type", "Polygon"), kvp("coordinates", polygon_coords));
    RepresentativePoint rp_polygon = representative_point(polygon.view(), GeometryType::Polygon);
    assert_true(rp_polygon.valid, "rep-point: polygon should be valid");
    assert_true(std::fabs(rp_polygon.lon - 1.0) < 1e-6 && std::fabs(rp_polygon.lat - 1.0) < 1e-6,
                "rep-point: polygon centroid at center");
}

void test_confidence_and_epsilon() {
    MatcherConfig config;
    assert_true(std::fabs(epsilon_for_type(config, GeometryType::Point) - 1.0) < 1e-9,
                "epsilon: point uses point epsilon");
    assert_true(std::fabs(epsilon_for_type(config, GeometryType::LineString) - 10.0) < 1e-9,
                "epsilon: line uses line epsilon");
    assert_true(std::fabs(epsilon_for_type(config, GeometryType::Polygon) - 10.0) < 1e-9,
                "epsilon: polygon uses polygon epsilon");

    assert_true(std::fabs(confidence_for(0.0, 1.0) - 1.0) < 1e-9, "confidence: zero distance is 1");
    assert_true(std::fabs(confidence_for(0.5, 1.0) - 0.5) < 1e-9,
                "confidence: half epsilon is 0.5");
    assert_true(confidence_for(2.0, 1.0) == 0.0, "confidence: beyond epsilon clamps to 0");
}

void test_large_matcher_moved_point() {
    LargeDiffMatcher matcher;

    std::vector<GeoCandidate> removed = {
        make_candidate("old", "hash-old", 30.0, 60.0, GeometryType::Point)};
    std::vector<GeoCandidate> added = {
        make_candidate("new", "hash-new", 30.0, 60.000005, GeometryType::Point)};

    auto pairs = matcher.match(removed, added);
    assert_true(pairs.size() == 1, "large: one match expected");
    assert_true(pairs[0].removed_hash == "hash-old", "large: removed_hash");
    assert_true(pairs[0].added_hash == "hash-new", "large: added_hash");
    assert_true(pairs[0].distance_m < 1.0, "large: distance under one meter");
    assert_true(pairs[0].confidence > 0.0, "large: positive confidence");
}

void test_large_matcher_far_point() {
    LargeDiffMatcher matcher;

    std::vector<GeoCandidate> removed = {
        make_candidate("old", "hash-old", 30.0, 60.0, GeometryType::Point)};
    std::vector<GeoCandidate> added = {
        make_candidate("new", "hash-new", 30.0, 61.0, GeometryType::Point)};

    auto pairs = matcher.match(removed, added);
    assert_true(pairs.empty(), "large: far point should not match");
}

void test_large_matcher_one_to_one() {
    LargeDiffMatcher matcher;

    std::vector<GeoCandidate> removed = {
        make_candidate("old-a", "ra", 30.0, 60.0, GeometryType::Point),
        make_candidate("old-b", "rb", 40.0, 50.0, GeometryType::Point)};
    std::vector<GeoCandidate> added = {
        make_candidate("new-a", "aa", 30.0, 60.000004, GeometryType::Point),
        make_candidate("new-b", "ab", 40.0, 50.000004, GeometryType::Point)};

    auto pairs = matcher.match(removed, added);
    assert_true(pairs.size() == 2, "large: two distinct matches expected");
    for (const auto& pair : pairs) {
        bool valid = (pair.removed_hash == "ra" && pair.added_hash == "aa") ||
                     (pair.removed_hash == "rb" && pair.added_hash == "ab");
        assert_true(valid, "large: pairs matched to nearest counterpart");
    }
}

void test_large_matcher_empty() {
    LargeDiffMatcher matcher;
    assert_true(matcher.match({}, {}).empty(), "large: empty inputs give empty result");

    std::vector<GeoCandidate> some = {make_candidate("x", "hx", 1.0, 1.0, GeometryType::Point)};
    assert_true(matcher.match(some, {}).empty(), "large: no added gives empty result");
    assert_true(matcher.match({}, some).empty(), "large: no removed gives empty result");
}

void test_small_matcher_moved_point() {
    MongoDBConnection conn(get_mongo_uri(), "geoversion");
    conn.initialize_database();
    clear_collections(conn);
    CAS cas(conn.get_bpo_cas_collection());

    std::string added_hash = store_point(cas, 30.0, 60.0, "tree");

    SmallDiffMatcher matcher(conn.get_bpo_cas_collection());

    std::vector<GeoCandidate> removed = {
        make_candidate("old", "removed-hash", 30.0, 60.000005, GeometryType::Point)};
    std::vector<GeoCandidate> added = {
        make_candidate("new", added_hash, 30.0, 60.0, GeometryType::Point)};

    auto pairs = matcher.match(removed, added);
    assert_true(pairs.size() == 1, "small: one match expected");
    assert_true(pairs[0].removed_hash == "removed-hash", "small: removed_hash");
    assert_true(pairs[0].added_hash == added_hash, "small: added_hash");
    assert_true(pairs[0].confidence > 0.0, "small: positive confidence");
}

void test_small_matcher_far_point() {
    MongoDBConnection conn(get_mongo_uri(), "geoversion");
    conn.initialize_database();
    clear_collections(conn);
    CAS cas(conn.get_bpo_cas_collection());

    std::string added_hash = store_point(cas, 30.0, 60.0, "tree");

    SmallDiffMatcher matcher(conn.get_bpo_cas_collection());

    std::vector<GeoCandidate> removed = {
        make_candidate("old", "removed-hash", 30.0, 61.0, GeometryType::Point)};
    std::vector<GeoCandidate> added = {
        make_candidate("new", added_hash, 30.0, 60.0, GeometryType::Point)};

    auto pairs = matcher.match(removed, added);
    assert_true(pairs.empty(), "small: far point should not match");
}

void test_adaptive_matcher_small_path() {
    MongoDBConnection conn(get_mongo_uri(), "geoversion");
    conn.initialize_database();
    clear_collections(conn);
    CAS cas(conn.get_bpo_cas_collection());

    std::string added_hash = store_point(cas, 10.0, 20.0, "road");

    AdaptiveMatcher matcher(conn.get_bpo_cas_collection());

    std::vector<GeoCandidate> removed = {
        make_candidate("old", "removed-hash", 10.0, 20.000005, GeometryType::Point)};
    std::vector<GeoCandidate> added = {
        make_candidate("new", added_hash, 10.0, 20.0, GeometryType::Point)};

    auto pairs = matcher.match(removed, added);
    assert_true(pairs.size() == 1, "adaptive: one match via small path");
    assert_true(pairs[0].added_hash == added_hash, "adaptive: added_hash");
}

void test_adaptive_matcher_large_path() {
    MongoDBConnection conn(get_mongo_uri(), "geoversion");
    clear_collections(conn);

    MatcherConfig config;
    config.threshold = 1;
    AdaptiveMatcher matcher(conn.get_bpo_cas_collection(), config);

    std::vector<GeoCandidate> removed = {
        make_candidate("old", "hash-old", 30.0, 60.0, GeometryType::Point)};
    std::vector<GeoCandidate> added = {
        make_candidate("new", "hash-new", 30.0, 60.000005, GeometryType::Point)};

    auto pairs = matcher.match(removed, added);
    assert_true(pairs.size() == 1, "adaptive: one match via large path");
    assert_true(pairs[0].added_hash == "hash-new", "adaptive: large path added_hash");
}

void test_diff_engine_level2() {
    MongoDBConnection conn(get_mongo_uri(), "geoversion");
    clear_collections(conn);
    CAS cas(conn.get_bpo_cas_collection());
    VersionStorage versions(conn);

    std::string hash_old = store_point(cas, 30.0, 60.0, "building");
    std::string hash_new = store_point(cas, 30.0, 60.000005, "building");

    std::string from = versions.create_version("sit-1", {{"obj-old", hash_old}});
    std::string to = versions.create_version("sit-1", {{"obj-new", hash_new}});

    LargeDiffMatcher matcher;
    DiffEngine engine(versions);
    engine.set_entity_matcher(cas, matcher);

    DiffResult result = engine.compute(from, to);
    assert_true(result.added.size() == 1 && result.added[0] == "obj-new", "level2: one added");
    assert_true(result.removed.size() == 1 && result.removed[0] == "obj-old",
                "level2: one removed");
    assert_true(result.likely_modified.size() == 1, "level2: one likely_modified pair");
    assert_true(result.likely_modified[0].removed_hash == hash_old, "level2: removed_hash");
    assert_true(result.likely_modified[0].added_hash == hash_new, "level2: added_hash");
    assert_true(result.likely_modified[0].confidence > 0.0, "level2: positive confidence");
}

int main() {
    std::cout << "Running EntityMatcher tests..." << std::endl;

    test_haversine_known_distance();
    test_representative_point_types();
    test_confidence_and_epsilon();
    test_large_matcher_moved_point();
    test_large_matcher_far_point();
    test_large_matcher_one_to_one();
    test_large_matcher_empty();
    test_small_matcher_moved_point();
    test_small_matcher_far_point();
    test_adaptive_matcher_small_path();
    test_adaptive_matcher_large_path();
    test_diff_engine_level2();

    std::cout << "EntityMatcher tests passed." << std::endl;
    return EXIT_SUCCESS;
}
