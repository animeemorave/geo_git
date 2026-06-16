#include <cstdlib>
#include <iostream>
#include <string>

#include <bsoncxx/builder/stream/document.hpp>

#include "storage/branch_storage/branch_storage.h"
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

static void clear_branches(MongoDBConnection& conn) {
    bsoncxx::builder::stream::document empty_filter;
    conn.get_branches_collection().delete_many(empty_filter.view());
}

void test_branch_create_and_get() {
    MongoDBConnection conn(get_mongo_uri(), "geoversion");
    clear_branches(conn);
    BranchStorage storage(conn);

    std::string id = storage.create("sit-1", "main", "ver-1");
    assert_true(!id.empty(), "create returned empty branch_id");

    auto loaded = storage.get(id);
    assert_true(static_cast<bool>(loaded), "get returned nullopt after create");
    assert_true(loaded->branch_id == id, "branch_id mismatch");
    assert_true(loaded->situation_id == "sit-1", "situation_id mismatch");
    assert_true(loaded->name == "main", "name mismatch");
    assert_true(static_cast<bool>(loaded->head_version_id), "head_version_id missing");
    assert_true(*loaded->head_version_id == "ver-1", "head_version_id mismatch");
}

void test_branch_create_empty_head() {
    MongoDBConnection conn(get_mongo_uri(), "geoversion");
    clear_branches(conn);
    BranchStorage storage(conn);

    std::string id = storage.create("sit-1", "main");
    auto loaded = storage.get(id);
    assert_true(static_cast<bool>(loaded), "get returned nullopt for empty-head branch");
    assert_true(!loaded->head_version_id, "empty branch should have no head_version_id");
}

void test_branch_get_missing() {
    MongoDBConnection conn(get_mongo_uri(), "geoversion");
    BranchStorage storage(conn);

    auto loaded = storage.get("non-existent-branch");
    assert_true(!loaded, "get returned a value for non-existent id");
}

void test_branch_empty_name_rejected() {
    MongoDBConnection conn(get_mongo_uri(), "geoversion");
    BranchStorage storage(conn);

    bool threw = false;
    try {
        storage.create("sit-1", "");
    } catch (const std::exception&) {
        threw = true;
    }
    assert_true(threw, "create with empty name did not throw");
}

void test_branch_duplicate_name_rejected() {
    MongoDBConnection conn(get_mongo_uri(), "geoversion");
    clear_branches(conn);
    BranchStorage storage(conn);

    storage.create("sit-1", "main");

    bool threw = false;
    try {
        storage.create("sit-1", "main");
    } catch (const std::exception&) {
        threw = true;
    }
    assert_true(threw, "duplicate branch name within situation did not throw");

    std::string other = storage.create("sit-2", "main");
    assert_true(!other.empty(), "same branch name in different situation was rejected");
}

void test_branch_get_by_name() {
    MongoDBConnection conn(get_mongo_uri(), "geoversion");
    clear_branches(conn);
    BranchStorage storage(conn);

    std::string id = storage.create("sit-1", "feature/roads", "ver-7");

    auto loaded = storage.get_by_name("sit-1", "feature/roads");
    assert_true(static_cast<bool>(loaded), "get_by_name returned nullopt");
    assert_true(loaded->branch_id == id, "get_by_name returned wrong branch");

    auto missing = storage.get_by_name("sit-1", "no-such-branch");
    assert_true(!missing, "get_by_name returned a value for missing branch");
}

void test_branch_list() {
    MongoDBConnection conn(get_mongo_uri(), "geoversion");
    clear_branches(conn);
    BranchStorage storage(conn);

    storage.create("sit-1", "main");
    storage.create("sit-1", "feature/a");
    storage.create("sit-2", "main");

    auto branches = storage.list("sit-1");
    assert_true(branches.size() == 2, "list did not return 2 branches for sit-1");
    assert_true(branches[0].name == "main", "list not ordered by created_at (main first)");
    assert_true(branches[1].name == "feature/a",
                "list not ordered by created_at (feature/a second)");
}

void test_branch_advance() {
    MongoDBConnection conn(get_mongo_uri(), "geoversion");
    clear_branches(conn);
    BranchStorage storage(conn);

    std::string id = storage.create("sit-1", "main");
    storage.advance(id, "ver-1");

    auto loaded = storage.get(id);
    assert_true(static_cast<bool>(loaded), "get returned nullopt after advance");
    assert_true(static_cast<bool>(loaded->head_version_id),
                "head_version_id missing after advance");
    assert_true(*loaded->head_version_id == "ver-1", "head_version_id not advanced");

    storage.advance(id, "ver-2");
    auto reloaded = storage.get(id);
    assert_true(*reloaded->head_version_id == "ver-2", "head_version_id not re-advanced");
}

void test_branch_advance_missing_throws() {
    MongoDBConnection conn(get_mongo_uri(), "geoversion");
    BranchStorage storage(conn);

    bool threw = false;
    try {
        storage.advance("non-existent-branch", "ver-1");
    } catch (const std::exception&) {
        threw = true;
    }
    assert_true(threw, "advance of non-existent branch did not throw");
}

void test_branch_remove() {
    MongoDBConnection conn(get_mongo_uri(), "geoversion");
    clear_branches(conn);
    BranchStorage storage(conn);

    std::string id = storage.create("sit-1", "temp");
    storage.remove(id);

    auto loaded = storage.get(id);
    assert_true(!loaded, "branch still present after remove");

    bool threw = false;
    try {
        storage.remove(id);
    } catch (const std::exception&) {
        threw = true;
    }
    assert_true(threw, "removing an already-removed branch did not throw");
}

int main() {
    std::cout << "Running BranchStorage tests..." << std::endl;

    test_branch_create_and_get();
    test_branch_create_empty_head();
    test_branch_get_missing();
    test_branch_empty_name_rejected();
    test_branch_duplicate_name_rejected();
    test_branch_get_by_name();
    test_branch_list();
    test_branch_advance();
    test_branch_advance_missing_throws();
    test_branch_remove();

    std::cout << "BranchStorage tests passed." << std::endl;
    return EXIT_SUCCESS;
}
