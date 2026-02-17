#include <iostream>
#include <cstdlib>
#include <vector>
#include <string>

#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/builder/basic/array.hpp>
#include <bsoncxx/builder/basic/kvp.hpp>

#include "storage/mongodb_connection/mongodb_connection.h"
#include "storage/cas/cas.h"
#include "storage/bpo_storage/bpo_storage.h"

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

    BPO bpo("", geom_value.view(), attr_value.view());
    return bpo;
}

void test_cas_basic() {
    std::string uri = get_mongo_uri();
    MongoDBConnection conn(uri, "geoversion");
    bool ok = conn.test_connection();
    assert_true(ok, "MongoDB connection failed in test_cas_basic");
}

void test_cas_hash_computation() {
    std::string uri = get_mongo_uri();
    MongoDBConnection conn(uri, "geoversion");
    auto collection = conn.get_bpo_cas_collection();
    CAS cas(collection);

    BPO bpo = make_point_bpo(30.0, 60.0, "test_class");

    std::string h1 = cas.compute_hash(bpo);
    std::string h2 = cas.compute_hash(bpo);

    assert_true(!h1.empty(), "CAS hash is empty");
    assert_true(h1 == h2, "CAS hash is not deterministic");
}

void test_cas_store_retrieve() {
    std::string uri = get_mongo_uri();
    MongoDBConnection conn(uri, "geoversion");
    auto collection = conn.get_bpo_cas_collection();
    CAS cas(collection);

    bsoncxx::builder::stream::document empty_filter;
    collection.delete_many(empty_filter.view());

    BPO bpo = make_point_bpo(30.0, 60.0, "store_retrieve");

    std::string hash = cas.compute_hash(bpo);
    bpo.set_hash(hash);

    bool stored = cas.store(bpo);
    assert_true(stored, "CAS store failed");
    bool exists = cas.exists(hash);
    assert_true(exists, "CAS exists returned false after store");

    auto loaded = cas.retrieve(hash);
    assert_true(static_cast<bool>(loaded), "CAS retrieve returned null");
    assert_true(loaded->get_hash() == hash, "CAS retrieved BPO hash mismatch");
}

void test_cas_deduplication() {
    std::string uri = get_mongo_uri();
    MongoDBConnection conn(uri, "geoversion");
    auto collection = conn.get_bpo_cas_collection();
    CAS cas(collection);

    bsoncxx::builder::stream::document empty_filter;
    collection.delete_many(empty_filter.view());

    BPO bpo1 = make_point_bpo(10.0, 20.0, "dedup");
    BPO bpo2 = make_point_bpo(10.0, 20.0, "dedup");

    std::string h1 = cas.compute_hash(bpo1);
    std::string h2 = cas.compute_hash(bpo2);

    bpo1.set_hash(h1);
    bpo2.set_hash(h2);

    bool s1 = cas.store(bpo1);
    bool s2 = cas.store(bpo2);

    assert_true(s1, "CAS first store failed");
    assert_true(s2, "CAS second store failed");

    size_t c = cas.count();
    assert_true(c == 1, "CAS deduplication failed, count != 1");

    std::vector<std::string> hashes = cas.get_all_hashes();
    assert_true(hashes.size() == 1, "CAS deduplication failed, hashes size != 1");
}

