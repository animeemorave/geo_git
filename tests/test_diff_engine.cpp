#include <cstdlib>
#include <iostream>
#include <string>
#include <unordered_map>

#include <bsoncxx/builder/stream/document.hpp>

#include "diff/diff_engine.h"
#include "storage/delta_storage/delta_storage.h"
#include "storage/mongodb_connection/mongodb_connection.h"
#include "storage/version_storage/version_storage.h"

using namespace geoversion;
using namespace geoversion::storage;
using geoversion::diff::DiffEngine;
using geoversion::diff::DiffResult;

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
    conn.get_version_deltas_collection().delete_many(empty_filter.view());
}

static std::string make_version(VersionStorage& storage, const std::string& situation_id,
                                const std::unordered_map<std::string, std::string>& objects) {
    return storage.create_version(situation_id, objects);
}

void test_diff_identical_versions() {
    MongoDBConnection conn(get_mongo_uri(), "geoversion");
    clear_collections(conn);
    VersionStorage versions(conn);
    DiffEngine engine(versions);

    std::string v1 = make_version(versions, "sit-1", {{"a", "h1"}, {"b", "h2"}});
    std::string v2 = make_version(versions, "sit-1", {{"a", "h1"}, {"b", "h2"}});

    DiffResult d = engine.compute(v1, v2);
    assert_true(d.added.empty(), "identical: added not empty");
    assert_true(d.removed.empty(), "identical: removed not empty");
    assert_true(d.modified.empty(), "identical: modified not empty");
    assert_true(d.unchanged.size() == 2, "identical: unchanged size != 2");
    assert_true(d.unchanged[0] == "a" && d.unchanged[1] == "b", "identical: unchanged content");
}

void test_diff_added() {
    MongoDBConnection conn(get_mongo_uri(), "geoversion");
    clear_collections(conn);
    VersionStorage versions(conn);
    DiffEngine engine(versions);

    std::string v1 = make_version(versions, "sit-1", {{"a", "h1"}});
    std::string v2 = make_version(versions, "sit-1", {{"a", "h1"}, {"b", "h2"}});

    DiffResult d = engine.compute(v1, v2);
    assert_true(d.added.size() == 1 && d.added[0] == "b", "added: wrong added set");
    assert_true(d.removed.empty(), "added: removed not empty");
    assert_true(d.modified.empty(), "added: modified not empty");
    assert_true(d.unchanged.size() == 1 && d.unchanged[0] == "a", "added: wrong unchanged set");
}

void test_diff_removed() {
    MongoDBConnection conn(get_mongo_uri(), "geoversion");
    clear_collections(conn);
    VersionStorage versions(conn);
    DiffEngine engine(versions);

    std::string v1 = make_version(versions, "sit-1", {{"a", "h1"}, {"b", "h2"}});
    std::string v2 = make_version(versions, "sit-1", {{"a", "h1"}});

    DiffResult d = engine.compute(v1, v2);
    assert_true(d.removed.size() == 1 && d.removed[0] == "b", "removed: wrong removed set");
    assert_true(d.added.empty(), "removed: added not empty");
    assert_true(d.modified.empty(), "removed: modified not empty");
    assert_true(d.unchanged.size() == 1 && d.unchanged[0] == "a", "removed: wrong unchanged set");
}

void test_diff_modified() {
    MongoDBConnection conn(get_mongo_uri(), "geoversion");
    clear_collections(conn);
    VersionStorage versions(conn);
    DiffEngine engine(versions);

    std::string v1 = make_version(versions, "sit-1", {{"a", "h1"}});
    std::string v2 = make_version(versions, "sit-1", {{"a", "h2"}});

    DiffResult d = engine.compute(v1, v2);
    assert_true(d.modified.size() == 1 && d.modified[0] == "a", "modified: wrong modified set");
    assert_true(d.added.empty(), "modified: added not empty");
    assert_true(d.removed.empty(), "modified: removed not empty");
    assert_true(d.unchanged.empty(), "modified: unchanged not empty");
}

void test_diff_from_empty_root() {
    MongoDBConnection conn(get_mongo_uri(), "geoversion");
    clear_collections(conn);
    VersionStorage versions(conn);
    DiffEngine engine(versions);

    std::string root = make_version(versions, "sit-1", {});
    std::string v2 = make_version(versions, "sit-1", {{"a", "h1"}, {"b", "h2"}});

    DiffResult d = engine.compute(root, v2);
    assert_true(d.added.size() == 2, "from-empty: added size != 2");
    assert_true(d.added[0] == "a" && d.added[1] == "b", "from-empty: wrong added set");
    assert_true(d.removed.empty() && d.modified.empty() && d.unchanged.empty(),
                "from-empty: other sets not empty");
}

void test_diff_combined() {
    MongoDBConnection conn(get_mongo_uri(), "geoversion");
    clear_collections(conn);
    VersionStorage versions(conn);
    DiffEngine engine(versions);

    std::string v1 = make_version(versions, "sit-1", {{"a", "h1"}, {"b", "h2"}, {"c", "h3"}});
    std::string v2 = make_version(versions, "sit-1", {{"a", "h1"}, {"b", "h2x"}, {"d", "h4"}});

    DiffResult d = engine.compute(v1, v2);
    assert_true(d.added.size() == 1 && d.added[0] == "d", "combined: wrong added");
    assert_true(d.removed.size() == 1 && d.removed[0] == "c", "combined: wrong removed");
    assert_true(d.modified.size() == 1 && d.modified[0] == "b", "combined: wrong modified");
    assert_true(d.unchanged.size() == 1 && d.unchanged[0] == "a", "combined: wrong unchanged");
}

void test_diff_missing_version_throws() {
    MongoDBConnection conn(get_mongo_uri(), "geoversion");
    clear_collections(conn);
    VersionStorage versions(conn);
    DiffEngine engine(versions);

    std::string v1 = make_version(versions, "sit-1", {{"a", "h1"}});

    bool threw = false;
    try {
        engine.compute(v1, "non-existent-version");
    } catch (const std::exception&) {
        threw = true;
    }
    assert_true(threw, "diff with non-existent version did not throw");
}

void test_diff_cache_integration() {
    MongoDBConnection conn(get_mongo_uri(), "geoversion");
    clear_collections(conn);
    VersionStorage versions(conn);
    DeltaStorage deltas(conn);
    DiffEngine engine(versions, deltas);

    std::string v1 = make_version(versions, "sit-1", {{"a", "h1"}, {"b", "h2"}});
    std::string v2 = make_version(versions, "sit-1", {{"a", "h1"}, {"b", "h2x"}, {"c", "h3"}});

    DiffResult first = engine.compute(v1, v2);
    auto cached = deltas.find(v1, v2);
    assert_true(static_cast<bool>(cached), "cache: delta not persisted after first compute");
    assert_true(cached->added.size() == 1 && cached->added[0] == "c",
                "cache: wrong persisted added");
    assert_true(cached->modified.size() == 1 && cached->modified[0] == "b",
                "cache: wrong persisted modified");

    DiffResult second = engine.compute(v1, v2);
    assert_true(second.added == first.added, "cache: added differ between calls");
    assert_true(second.removed == first.removed, "cache: removed differ between calls");
    assert_true(second.modified == first.modified, "cache: modified differ between calls");
    assert_true(second.unchanged == first.unchanged, "cache: unchanged differ between calls");
}

int main() {
    std::cout << "Running DiffEngine tests..." << std::endl;

    test_diff_identical_versions();
    test_diff_added();
    test_diff_removed();
    test_diff_modified();
    test_diff_from_empty_root();
    test_diff_combined();
    test_diff_missing_version_throws();
    test_diff_cache_integration();

    std::cout << "DiffEngine tests passed." << std::endl;
    return EXIT_SUCCESS;
}
