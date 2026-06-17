#include <cstdlib>
#include <iostream>
#include <string>
#include <unordered_map>

#include <bsoncxx/builder/stream/document.hpp>

#include "merge/merge_engine.h"
#include "storage/mongodb_connection/mongodb_connection.h"
#include "storage/version_storage/version_storage.h"

using namespace geoversion;
using namespace geoversion::storage;
using geoversion::merge::Conflict;
using geoversion::merge::ConflictType;
using geoversion::merge::merge_object_maps;
using geoversion::merge::MergeEngine;
using geoversion::merge::MergeResult;
using geoversion::merge::MergeStrategy;

using Map = std::unordered_map<std::string, std::string>;

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
}

void test_merge_no_changes() {
    MergeResult r = merge_object_maps({{"a", "h1"}}, {{"a", "h1"}}, {{"a", "h1"}});
    assert_true(r.conflicts.empty(), "no-changes: unexpected conflicts");
    assert_true(r.merged.size() == 1 && r.merged.at("a") == "h1", "no-changes: wrong merged");
}

void test_merge_only_ours_modified() {
    MergeResult r = merge_object_maps({{"a", "h1"}}, {{"a", "h2"}}, {{"a", "h1"}});
    assert_true(r.conflicts.empty(), "ours-only: unexpected conflicts");
    assert_true(r.merged.at("a") == "h2", "ours-only: should take ours");
}

void test_merge_only_theirs_modified() {
    MergeResult r = merge_object_maps({{"a", "h1"}}, {{"a", "h1"}}, {{"a", "h2"}});
    assert_true(r.conflicts.empty(), "theirs-only: unexpected conflicts");
    assert_true(r.merged.at("a") == "h2", "theirs-only: should take theirs");
}

void test_merge_added_each_side() {
    MergeResult ours_add = merge_object_maps({}, {{"a", "h1"}}, {});
    assert_true(ours_add.conflicts.empty() && ours_add.merged.at("a") == "h1",
                "added: ours add not merged");

    MergeResult theirs_add = merge_object_maps({}, {}, {{"a", "h1"}});
    assert_true(theirs_add.conflicts.empty() && theirs_add.merged.at("a") == "h1",
                "added: theirs add not merged");
}

void test_merge_both_deleted() {
    MergeResult r = merge_object_maps({{"a", "h1"}}, {}, {});
    assert_true(r.conflicts.empty(), "both-deleted: unexpected conflicts");
    assert_true(r.merged.empty(), "both-deleted: should be empty");
}

void test_merge_one_side_deleted() {
    MergeResult r = merge_object_maps({{"a", "h1"}}, {}, {{"a", "h1"}});
    assert_true(r.conflicts.empty(), "one-deleted: unexpected conflicts");
    assert_true(r.merged.empty(), "one-deleted: deletion should win");
}

void test_merge_both_modified_same() {
    MergeResult r = merge_object_maps({{"a", "h1"}}, {{"a", "h2"}}, {{"a", "h2"}});
    assert_true(r.conflicts.empty(), "both-same: unexpected conflicts");
    assert_true(r.merged.at("a") == "h2", "both-same: wrong merged");
}

void test_merge_conflict_both_modified() {
    MergeResult r = merge_object_maps({{"a", "h1"}}, {{"a", "h2"}}, {{"a", "h3"}});
    assert_true(r.merged.find("a") == r.merged.end(), "both-modified: should not be merged");
    assert_true(r.conflicts.size() == 1, "both-modified: expected one conflict");
    const Conflict& c = r.conflicts[0];
    assert_true(c.object_id == "a", "both-modified: wrong object_id");
    assert_true(c.type == ConflictType::BothModified, "both-modified: wrong type");
    assert_true(c.base_hash == "h1" && c.ours_hash == "h2" && c.theirs_hash == "h3",
                "both-modified: wrong hashes");
}

void test_merge_conflict_modified_deleted() {
    MergeResult r = merge_object_maps({{"a", "h1"}}, {{"a", "h2"}}, {});
    assert_true(r.conflicts.size() == 1, "modified-deleted: expected one conflict");
    const Conflict& c = r.conflicts[0];
    assert_true(c.type == ConflictType::ModifiedDeleted, "modified-deleted: wrong type");
    assert_true(c.ours_hash == "h2" && c.theirs_hash.empty(), "modified-deleted: wrong hashes");
}

void test_merge_conflict_both_added() {
    MergeResult r = merge_object_maps({}, {{"a", "h2"}}, {{"a", "h3"}});
    assert_true(r.conflicts.size() == 1, "both-added: expected one conflict");
    const Conflict& c = r.conflicts[0];
    assert_true(c.type == ConflictType::BothAdded, "both-added: wrong type");
    assert_true(c.base_hash.empty() && c.ours_hash == "h2" && c.theirs_hash == "h3",
                "both-added: wrong hashes");
}

void test_merge_strategy_ours() {
    MergeResult r =
        merge_object_maps({{"a", "h1"}}, {{"a", "h2"}}, {{"a", "h3"}}, MergeStrategy::Ours);
    assert_true(r.conflicts.empty(), "strategy-ours: conflicts should be resolved");
    assert_true(r.merged.at("a") == "h2", "strategy-ours: should take ours");
}

void test_merge_strategy_theirs() {
    MergeResult r =
        merge_object_maps({{"a", "h1"}}, {{"a", "h2"}}, {{"a", "h3"}}, MergeStrategy::Theirs);
    assert_true(r.conflicts.empty(), "strategy-theirs: conflicts should be resolved");
    assert_true(r.merged.at("a") == "h3", "strategy-theirs: should take theirs");
}

void test_merge_strategy_ours_resolves_deletion() {
    MergeResult r = merge_object_maps({{"a", "h1"}}, {}, {{"a", "h3"}}, MergeStrategy::Ours);
    assert_true(r.conflicts.empty(), "strategy-ours-del: conflicts should be resolved");
    assert_true(r.merged.find("a") == r.merged.end(),
                "strategy-ours-del: ours deletion should win");
}

void test_merge_combined() {
    Map base = {{"a", "h1"}, {"b", "h2"}, {"c", "h3"}};
    Map ours = {{"a", "h1"}, {"b", "h2x"}, {"c", "h3"}, {"d", "h4"}};
    Map theirs = {{"a", "h1x"}, {"b", "h2"}, {"c", "h3"}};

    MergeResult r = merge_object_maps(base, ours, theirs);
    assert_true(r.conflicts.empty(), "combined: unexpected conflicts");
    assert_true(r.merged.at("a") == "h1x", "combined: a should take theirs");
    assert_true(r.merged.at("b") == "h2x", "combined: b should take ours");
    assert_true(r.merged.at("c") == "h3", "combined: c unchanged");
    assert_true(r.merged.at("d") == "h4", "combined: d added by ours");
    assert_true(r.merged.size() == 4, "combined: wrong merged size");
}

void test_merge_via_version_storage() {
    MongoDBConnection conn(get_mongo_uri(), "geoversion");
    clear_collections(conn);
    VersionStorage versions(conn);
    MergeEngine engine(versions);

    std::string base = versions.create_version("sit-1", {{"a", "h1"}, {"b", "h2"}});
    std::string ours = versions.create_version("sit-1", {{"a", "h2"}, {"b", "h2"}});
    std::string theirs = versions.create_version("sit-1", {{"a", "h1"}, {"b", "h3"}});

    MergeResult r = engine.merge(base, ours, theirs);
    assert_true(r.conflicts.empty(), "via-storage: unexpected conflicts");
    assert_true(r.merged.at("a") == "h2", "via-storage: a should take ours");
    assert_true(r.merged.at("b") == "h3", "via-storage: b should take theirs");
}

void test_merge_missing_version_throws() {
    MongoDBConnection conn(get_mongo_uri(), "geoversion");
    clear_collections(conn);
    VersionStorage versions(conn);
    MergeEngine engine(versions);

    std::string base = versions.create_version("sit-1", {{"a", "h1"}});

    bool threw = false;
    try {
        engine.merge(base, "non-existent", base);
    } catch (const std::exception&) {
        threw = true;
    }
    assert_true(threw, "merge with non-existent version did not throw");
}

int main() {
    std::cout << "Running MergeEngine tests..." << std::endl;

    test_merge_no_changes();
    test_merge_only_ours_modified();
    test_merge_only_theirs_modified();
    test_merge_added_each_side();
    test_merge_both_deleted();
    test_merge_one_side_deleted();
    test_merge_both_modified_same();
    test_merge_conflict_both_modified();
    test_merge_conflict_modified_deleted();
    test_merge_conflict_both_added();
    test_merge_strategy_ours();
    test_merge_strategy_theirs();
    test_merge_strategy_ours_resolves_deletion();
    test_merge_combined();
    test_merge_via_version_storage();
    test_merge_missing_version_throws();

    std::cout << "MergeEngine tests passed." << std::endl;
    return EXIT_SUCCESS;
}
