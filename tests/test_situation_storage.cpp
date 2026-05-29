#include <iostream>
#include <cstdlib>
#include <string>
#include <stdexcept>

#include <bsoncxx/builder/stream/document.hpp>

#include "storage/mongodb_connection/mongodb_connection.h"
#include "storage/situation_storage/situation_storage.h"

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

static void clear_situations(MongoDBConnection& conn) {
    bsoncxx::builder::stream::document empty_filter;
    conn.get_situations_collection().delete_many(empty_filter.view());
}

void test_situation_create_and_get() {
    MongoDBConnection conn(get_mongo_uri(), "geoversion");
    clear_situations(conn);
    SituationStorage storage(conn);

    std::string id = storage.create("downtown", "city center survey");
    assert_true(!id.empty(), "create returned empty situation_id");

    auto loaded = storage.get(id);
    assert_true(static_cast<bool>(loaded), "get returned nullopt after create");
    assert_true(loaded->situation_id == id, "situation_id mismatch");
    assert_true(loaded->name == "downtown", "name mismatch");
    assert_true(loaded->description == "city center survey", "description mismatch");
}

void test_situation_get_missing() {
    MongoDBConnection conn(get_mongo_uri(), "geoversion");
    SituationStorage storage(conn);

    auto loaded = storage.get("non-existent-id");
    assert_true(!loaded, "get returned a value for non-existent id");
}

void test_situation_empty_name_rejected() {
    MongoDBConnection conn(get_mongo_uri(), "geoversion");
    SituationStorage storage(conn);

    bool threw = false;
    try {
        storage.create("");
    } catch (const std::exception&) {
        threw = true;
    }
    assert_true(threw, "create with empty name did not throw");
}

void test_situation_list() {
    MongoDBConnection conn(get_mongo_uri(), "geoversion");
    clear_situations(conn);
    SituationStorage storage(conn);

    storage.create("alpha", "first");
    storage.create("beta", "second");

    auto all = storage.list();
    assert_true(all.size() == 2, "list did not return 2 situations");
    assert_true(all[0].name == "alpha", "list not sorted by created_at (alpha first)");
    assert_true(all[1].name == "beta", "list not sorted by created_at (beta second)");
}

void test_situation_update() {
    MongoDBConnection conn(get_mongo_uri(), "geoversion");
    clear_situations(conn);
    SituationStorage storage(conn);

    std::string id = storage.create("old name", "old desc");
    storage.update(id, "new name", "new desc");

    auto loaded = storage.get(id);
    assert_true(static_cast<bool>(loaded), "get returned nullopt after update");
    assert_true(loaded->name == "new name", "name not updated");
    assert_true(loaded->description == "new desc", "description not updated");
}

void test_situation_update_missing_throws() {
    MongoDBConnection conn(get_mongo_uri(), "geoversion");
    SituationStorage storage(conn);

    bool threw = false;
    try {
        storage.update("non-existent-id", "x", "y");
    } catch (const std::exception&) {
        threw = true;
    }
    assert_true(threw, "update of non-existent situation did not throw");
}

void test_situation_remove() {
    MongoDBConnection conn(get_mongo_uri(), "geoversion");
    clear_situations(conn);
    SituationStorage storage(conn);

    std::string id = storage.create("to delete", "");
    storage.remove(id);

    auto loaded = storage.get(id);
    assert_true(!loaded, "situation still present after remove");

    bool threw = false;
    try {
        storage.remove(id);
    } catch (const std::exception&) {
        threw = true;
    }
    assert_true(threw, "removing an already-removed situation did not throw");
}

int main() {
    std::cout << "Running SituationStorage tests..." << std::endl;

    test_situation_create_and_get();
    test_situation_get_missing();
    test_situation_empty_name_rejected();
    test_situation_list();
    test_situation_update();
    test_situation_update_missing_throws();
    test_situation_remove();

    std::cout << "SituationStorage tests passed." << std::endl;
    return EXIT_SUCCESS;
}
