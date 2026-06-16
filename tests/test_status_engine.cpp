#include <cstdlib>
#include <iostream>
#include <string>
#include <unordered_map>

#include <bsoncxx/builder/stream/document.hpp>

#include "diff/diff_result.h"
#include "diff/status_engine.h"
#include "storage/branch_storage/branch_storage.h"
#include "storage/mongodb_connection/mongodb_connection.h"
#include "storage/version_storage/version_storage.h"

using namespace geoversion;
using namespace geoversion::storage;
using geoversion::diff::DiffResult;
using geoversion::diff::StatusEngine;

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
    conn.get_branches_collection().delete_many(empty_filter.view());
}

void test_status_empty_branch_all_added() {
    MongoDBConnection conn(get_mongo_uri(), "geoversion");
    clear_collections(conn);
    BranchStorage branches(conn);
    VersionStorage versions(conn);
    StatusEngine engine(branches, versions);

    std::string branch_id = branches.create("sit-1", "main");

    std::unordered_map<std::string, std::string> proposed = {{"a", "h1"}, {"b", "h2"}};
    DiffResult d = engine.compute_status(branch_id, proposed);

    assert_true(d.added.size() == 2, "empty-branch: added size != 2");
    assert_true(d.added[0] == "a" && d.added[1] == "b", "empty-branch: wrong added set");
    assert_true(d.removed.empty() && d.modified.empty() && d.unchanged.empty(),
                "empty-branch: other sets not empty");
}

void test_status_no_changes() {
    MongoDBConnection conn(get_mongo_uri(), "geoversion");
    clear_collections(conn);
    BranchStorage branches(conn);
    VersionStorage versions(conn);
    StatusEngine engine(branches, versions);

    std::string version_id = versions.create_version("sit-1", {{"a", "h1"}, {"b", "h2"}});
    std::string branch_id = branches.create("sit-1", "main", version_id);

    std::unordered_map<std::string, std::string> proposed = {{"a", "h1"}, {"b", "h2"}};
    DiffResult d = engine.compute_status(branch_id, proposed);

    assert_true(d.added.empty() && d.removed.empty() && d.modified.empty(),
                "no-changes: unexpected changes");
    assert_true(d.unchanged.size() == 2, "no-changes: unchanged size != 2");
}

void test_status_partial_changes() {
    MongoDBConnection conn(get_mongo_uri(), "geoversion");
    clear_collections(conn);
    BranchStorage branches(conn);
    VersionStorage versions(conn);
    StatusEngine engine(branches, versions);

    std::string version_id =
        versions.create_version("sit-1", {{"a", "h1"}, {"b", "h2"}, {"c", "h3"}});
    std::string branch_id = branches.create("sit-1", "main", version_id);

    std::unordered_map<std::string, std::string> proposed = {
        {"a", "h1"}, {"b", "h2x"}, {"d", "h4"}};
    DiffResult d = engine.compute_status(branch_id, proposed);

    assert_true(d.added.size() == 1 && d.added[0] == "d", "partial: wrong added");
    assert_true(d.removed.size() == 1 && d.removed[0] == "c", "partial: wrong removed");
    assert_true(d.modified.size() == 1 && d.modified[0] == "b", "partial: wrong modified");
    assert_true(d.unchanged.size() == 1 && d.unchanged[0] == "a", "partial: wrong unchanged");
}

void test_status_empty_proposed_against_head() {
    MongoDBConnection conn(get_mongo_uri(), "geoversion");
    clear_collections(conn);
    BranchStorage branches(conn);
    VersionStorage versions(conn);
    StatusEngine engine(branches, versions);

    std::string version_id = versions.create_version("sit-1", {{"a", "h1"}, {"b", "h2"}});
    std::string branch_id = branches.create("sit-1", "main", version_id);

    DiffResult d = engine.compute_status(branch_id, {});

    assert_true(d.removed.size() == 2, "empty-proposed: removed size != 2");
    assert_true(d.removed[0] == "a" && d.removed[1] == "b", "empty-proposed: wrong removed set");
    assert_true(d.added.empty() && d.modified.empty() && d.unchanged.empty(),
                "empty-proposed: other sets not empty");
}

void test_status_missing_branch_throws() {
    MongoDBConnection conn(get_mongo_uri(), "geoversion");
    clear_collections(conn);
    BranchStorage branches(conn);
    VersionStorage versions(conn);
    StatusEngine engine(branches, versions);

    bool threw = false;
    try {
        engine.compute_status("non-existent-branch", {{"a", "h1"}});
    } catch (const std::exception&) {
        threw = true;
    }
    assert_true(threw, "status on non-existent branch did not throw");
}

int main() {
    std::cout << "Running StatusEngine tests..." << std::endl;

    test_status_empty_branch_all_added();
    test_status_no_changes();
    test_status_partial_changes();
    test_status_empty_proposed_against_head();
    test_status_missing_branch_throws();

    std::cout << "StatusEngine tests passed." << std::endl;
    return EXIT_SUCCESS;
}
