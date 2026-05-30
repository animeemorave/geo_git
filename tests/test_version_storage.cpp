#include <algorithm>
#include <cstdlib>
#include <iostream>
#include <string>
#include <unordered_map>
#include <vector>

#include <bsoncxx/builder/basic/array.hpp>
#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/builder/basic/kvp.hpp>
#include <bsoncxx/builder/stream/document.hpp>

#include "storage/bpo_storage/bpo_storage.h"
#include "storage/cas/cas.h"
#include "storage/mongodb_connection/mongodb_connection.h"
#include "storage/version_storage/version_storage.h"

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

static void clear_collections(MongoDBConnection& conn) {
    bsoncxx::builder::stream::document empty_filter;
    conn.get_situation_versions_collection().delete_many(empty_filter.view());
    conn.get_version_objects_collection().delete_many(empty_filter.view());
    conn.get_bpo_cas_collection().delete_many(empty_filter.view());
}

static BPO make_point_bpo(double lon, double lat, const std::string& cls) {
    bsoncxx::builder::basic::document geom_builder;
    bsoncxx::builder::basic::array coords;
    coords.append(lon);
    coords.append(lat);
    geom_builder.append(bsoncxx::builder::basic::kvp("type", "Point"));
    geom_builder.append(bsoncxx::builder::basic::kvp("coordinates", coords));

    bsoncxx::builder::basic::document attr_builder;
    attr_builder.append(bsoncxx::builder::basic::kvp("class", cls));

    bsoncxx::document::value geom_value = geom_builder.extract();
    bsoncxx::document::value attr_value = attr_builder.extract();

    return BPO("", geom_value.view(), attr_value.view());
}

static std::string store_bpo(CAS& cas, double lon, double lat, const std::string& cls) {
    BPO bpo = make_point_bpo(lon, lat, cls);
    std::string hash = cas.compute_hash(bpo);
    bpo.set_hash(hash);
    cas.store(bpo);
    return hash;
}

void test_version_create_and_get() {
    MongoDBConnection conn(get_mongo_uri(), "geoversion");
    clear_collections(conn);
    CAS cas(conn.get_bpo_cas_collection());
    VersionStorage storage(conn);

    std::string h1 = store_bpo(cas, 30.0, 60.0, "tree");
    std::string h2 = store_bpo(cas, 31.0, 61.0, "road");

    std::unordered_map<std::string, std::string> objects = {{"obj-1", h1}, {"obj-2", h2}};

    std::string version_id =
        storage.create_version("sit-1", objects, {}, "initial commit", "alice");
    assert_true(!version_id.empty(), "create_version returned empty version_id");

    auto loaded = storage.get_version(version_id);
    assert_true(static_cast<bool>(loaded), "get_version returned nullopt after create");
    assert_true(loaded->meta.version_id == version_id, "version_id mismatch");
    assert_true(loaded->meta.situation_id == "sit-1", "situation_id mismatch");
    assert_true(loaded->meta.message == "initial commit", "message mismatch");
    assert_true(loaded->meta.author == "alice", "author mismatch");
    assert_true(loaded->objects.size() == 2, "objects map size mismatch");
    assert_true(loaded->objects.at("obj-1") == h1, "obj-1 hash mismatch");
    assert_true(loaded->objects.at("obj-2") == h2, "obj-2 hash mismatch");
}

void test_version_get_missing() {
    MongoDBConnection conn(get_mongo_uri(), "geoversion");
    VersionStorage storage(conn);

    auto loaded = storage.get_version("non-existent-version");
    assert_true(!loaded, "get_version returned a value for non-existent id");
}

void test_version_empty_situation_rejected() {
    MongoDBConnection conn(get_mongo_uri(), "geoversion");
    VersionStorage storage(conn);

    bool threw = false;
    try {
        storage.create_version("", {});
    } catch (const std::exception&) {
        threw = true;
    }
    assert_true(threw, "create_version with empty situation_id did not throw");
}

void test_version_parent_ids_preserved() {
    MongoDBConnection conn(get_mongo_uri(), "geoversion");
    clear_collections(conn);
    VersionStorage storage(conn);

    std::string root = storage.create_version("sit-1", {}, {}, "root", "alice");
    std::string child = storage.create_version("sit-1", {}, {root}, "child", "bob");

    auto loaded = storage.get_version(child);
    assert_true(static_cast<bool>(loaded), "get_version returned nullopt for child");
    assert_true(loaded->meta.parent_version_ids.size() == 1, "parent_version_ids size mismatch");
    assert_true(loaded->meta.parent_version_ids[0] == root, "parent_version_id value mismatch");
}

void test_version_list_sorted_desc() {
    MongoDBConnection conn(get_mongo_uri(), "geoversion");
    clear_collections(conn);
    VersionStorage storage(conn);

    storage.create_version("sit-1", {}, {}, "first", "alice");
    storage.create_version("sit-1", {}, {}, "second", "alice");
    storage.create_version("sit-2", {}, {}, "other situation", "alice");

    auto versions = storage.list_versions("sit-1");
    assert_true(versions.size() == 2, "list_versions did not return 2 versions for sit-1");
    assert_true(versions[0].message == "second", "list_versions not sorted newest-first");
    assert_true(versions[1].message == "first", "list_versions not sorted newest-first");
}

void test_version_get_version_map() {
    MongoDBConnection conn(get_mongo_uri(), "geoversion");
    clear_collections(conn);
    CAS cas(conn.get_bpo_cas_collection());
    VersionStorage storage(conn);

    std::string h1 = store_bpo(cas, 10.0, 20.0, "a");
    std::string h2 = store_bpo(cas, 11.0, 21.0, "b");

    std::string version_id = storage.create_version("sit-1", {{"obj-1", h1}, {"obj-2", h2}});

    auto map = storage.get_version_map(version_id);
    assert_true(map.size() == 2, "version_map size mismatch");
    assert_true(map.at("obj-1") == h1, "version_map obj-1 mismatch");
    assert_true(map.at("obj-2") == h2, "version_map obj-2 mismatch");
}

void test_version_checkout() {
    MongoDBConnection conn(get_mongo_uri(), "geoversion");
    clear_collections(conn);
    CAS cas(conn.get_bpo_cas_collection());
    VersionStorage storage(conn);

    std::string h1 = store_bpo(cas, 30.0, 60.0, "tree");
    std::string h2 = store_bpo(cas, 31.0, 61.0, "road");

    std::string version_id = storage.create_version("sit-1", {{"obj-1", h1}, {"obj-2", h2}});

    auto objects = storage.checkout(version_id);
    assert_true(objects.size() == 2, "checkout returned wrong number of BPOs");

    std::vector<std::string> object_ids;
    for (const auto& bpo : objects) {
        auto oid = bpo.get_object_id();
        assert_true(static_cast<bool>(oid), "checkout BPO missing object_id");
        object_ids.push_back(*oid);
        assert_true(!bpo.get_hash().empty(), "checkout BPO missing hash");
    }
    std::sort(object_ids.begin(), object_ids.end());
    assert_true(object_ids[0] == "obj-1", "checkout missing obj-1");
    assert_true(object_ids[1] == "obj-2", "checkout missing obj-2");
}

void test_version_checkout_missing_throws() {
    MongoDBConnection conn(get_mongo_uri(), "geoversion");
    VersionStorage storage(conn);

    bool threw = false;
    try {
        storage.checkout("non-existent-version");
    } catch (const std::exception&) {
        threw = true;
    }
    assert_true(threw, "checkout of non-existent version did not throw");
}

void test_version_empty_commit() {
    MongoDBConnection conn(get_mongo_uri(), "geoversion");
    clear_collections(conn);
    VersionStorage storage(conn);

    std::string version_id = storage.create_version("sit-1", {}, {}, "empty", "alice");
    auto loaded = storage.get_version(version_id);
    assert_true(static_cast<bool>(loaded), "get_version returned nullopt for empty commit");
    assert_true(loaded->objects.empty(), "empty commit should have no objects");

    auto objects = storage.checkout(version_id);
    assert_true(objects.empty(), "checkout of empty commit should return no BPOs");
}

int main() {
    std::cout << "Running VersionStorage tests..." << std::endl;

    test_version_create_and_get();
    test_version_get_missing();
    test_version_empty_situation_rejected();
    test_version_parent_ids_preserved();
    test_version_list_sorted_desc();
    test_version_get_version_map();
    test_version_checkout();
    test_version_checkout_missing_throws();
    test_version_empty_commit();

    std::cout << "VersionStorage tests passed." << std::endl;
    return EXIT_SUCCESS;
}
