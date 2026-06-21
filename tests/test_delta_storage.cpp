#include <cstdlib>
#include <iostream>
#include <string>

#include <bsoncxx/builder/stream/document.hpp>

#include "diff/diff_result.h"
#include "storage/delta_storage/delta_storage.h"
#include "storage/mongodb_connection/mongodb_connection.h"

using namespace geoversion;
using namespace geoversion::storage;

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

static void clear_deltas(MongoDBConnection& conn) {
    bsoncxx::builder::stream::document empty_filter;
    conn.get_version_deltas_collection().delete_many(empty_filter.view());
    conn.get_delta_items_collection().delete_many(empty_filter.view());
}

static diff::DiffResult sample_diff() {
    diff::DiffResult d;
    d.added = {"obj-a1", "obj-a2"};
    d.removed = {"obj-r1"};
    d.modified = {"obj-m1", "obj-m2", "obj-m3"};
    d.unchanged = {"obj-u1"};

    diff::DiffResult::SimilarityPair pair;
    pair.removed_hash = "hash-removed";
    pair.added_hash = "hash-added";
    pair.confidence = 0.87;
    pair.distance_m = 3.5;
    d.likely_modified.push_back(pair);

    return d;
}

void test_delta_store_and_get() {
    MongoDBConnection conn(get_mongo_uri(), "geoversion");
    clear_deltas(conn);
    DeltaStorage storage(conn);

    std::string delta_id = storage.store(sample_diff(), "v1", "v2");
    assert_true(!delta_id.empty(), "store returned empty delta_id");

    auto loaded = storage.get(delta_id);
    assert_true(static_cast<bool>(loaded), "get returned nullopt after store");
    assert_true(loaded->added.size() == 2, "added size mismatch");
    assert_true(loaded->removed.size() == 1, "removed size mismatch");
    assert_true(loaded->modified.size() == 3, "modified size mismatch");
    assert_true(loaded->unchanged.size() == 1, "unchanged size mismatch");
    assert_true(loaded->added[0] == "obj-a1", "added content mismatch");
    assert_true(loaded->removed[0] == "obj-r1", "removed content mismatch");
}

void test_delta_likely_modified_round_trip() {
    MongoDBConnection conn(get_mongo_uri(), "geoversion");
    clear_deltas(conn);
    DeltaStorage storage(conn);

    std::string delta_id = storage.store(sample_diff(), "v1", "v2");
    auto loaded = storage.get(delta_id);
    assert_true(static_cast<bool>(loaded), "get returned nullopt");
    assert_true(loaded->likely_modified.size() == 1, "likely_modified size mismatch");

    const auto& pair = loaded->likely_modified[0];
    assert_true(pair.removed_hash == "hash-removed", "removed_hash mismatch");
    assert_true(pair.added_hash == "hash-added", "added_hash mismatch");
    assert_true(pair.confidence > 0.86 && pair.confidence < 0.88, "confidence mismatch");
    assert_true(pair.distance_m > 3.4 && pair.distance_m < 3.6, "distance_m mismatch");
}

void test_delta_find_by_versions() {
    MongoDBConnection conn(get_mongo_uri(), "geoversion");
    clear_deltas(conn);
    DeltaStorage storage(conn);

    storage.store(sample_diff(), "v10", "v11");

    auto found = storage.find("v10", "v11");
    assert_true(static_cast<bool>(found), "find returned nullopt for stored delta");
    assert_true(found->modified.size() == 3, "find returned wrong delta");

    auto missing = storage.find("v10", "v99");
    assert_true(!missing, "find returned a value for non-existent version pair");
}

void test_delta_store_idempotent() {
    MongoDBConnection conn(get_mongo_uri(), "geoversion");
    clear_deltas(conn);
    DeltaStorage storage(conn);

    std::string first = storage.store(sample_diff(), "v1", "v2");
    std::string second = storage.store(sample_diff(), "v1", "v2");
    assert_true(first == second, "storing same version pair returned a different delta_id");
}

void test_delta_get_missing() {
    MongoDBConnection conn(get_mongo_uri(), "geoversion");
    DeltaStorage storage(conn);

    auto loaded = storage.get("non-existent-delta");
    assert_true(!loaded, "get returned a value for non-existent delta_id");
}

void test_delta_empty_versions_rejected() {
    MongoDBConnection conn(get_mongo_uri(), "geoversion");
    DeltaStorage storage(conn);

    bool threw = false;
    try {
        storage.store(sample_diff(), "", "v2");
    } catch (const std::exception&) {
        threw = true;
    }
    assert_true(threw, "store with empty from_version_id did not throw");
}

int main() {
    std::cout << "Running DeltaStorage tests..." << std::endl;

    test_delta_store_and_get();
    test_delta_likely_modified_round_trip();
    test_delta_find_by_versions();
    test_delta_store_idempotent();
    test_delta_get_missing();
    test_delta_empty_versions_rejected();

    std::cout << "DeltaStorage tests passed." << std::endl;
    return EXIT_SUCCESS;
}
