#include "server/service_impl.h"
#include "storage/mongodb_connection/mongodb_connection.h"

#include "geo_git.grpc.pb.h"

#include <bsoncxx/builder/stream/document.hpp>
#include <grpcpp/grpcpp.h>

#include <cstdlib>
#include <iostream>
#include <map>
#include <memory>
#include <string>

using geoversion::storage::MongoDBConnection;

namespace {

void assert_true(bool condition, const std::string& message) {
    if (!condition) {
        std::cerr << "TEST FAILED: " << message << std::endl;
        std::exit(EXIT_FAILURE);
    }
}

std::string get_mongo_uri() {
    const char* uri = std::getenv("MONGODB_URI");
    if (uri && uri[0] != '\0') {
        return std::string(uri);
    }
    return std::string("mongodb://mongodb:27017");
}

void clear_collections(MongoDBConnection& conn) {
    bsoncxx::builder::stream::document empty;
    conn.get_situations_collection().delete_many(empty.view());
    conn.get_situation_versions_collection().delete_many(empty.view());
    conn.get_version_objects_collection().delete_many(empty.view());
    conn.get_bpo_cas_collection().delete_many(empty.view());
    conn.get_branches_collection().delete_many(empty.view());
    conn.get_version_deltas_collection().delete_many(empty.view());
}

std::string point_geometry(double lon, double lat) {
    return "{\"type\":\"Point\",\"coordinates\":[" + std::to_string(lon) + "," +
           std::to_string(lat) + "]}";
}

std::string store_point(geogit::GeoGit::Stub* stub, double lon, double lat,
                        const std::string& cls) {
    grpc::ClientContext ctx;
    geogit::StoreBPORequest request;
    request.set_geometry_json(point_geometry(lon, lat));
    request.set_attributes_json("{\"class\":\"" + cls + "\"}");
    geogit::StoreBPOResponse response;
    grpc::Status status = stub->StoreBPO(&ctx, request, &response);
    assert_true(status.ok(), "StoreBPO failed: " + status.error_message());
    assert_true(!response.hash().empty(), "StoreBPO returned empty hash");
    return response.hash();
}

std::string create_version(geogit::GeoGit::Stub* stub, const std::string& situation_id,
                           const std::map<std::string, std::string>& objects,
                           const std::string& message) {
    grpc::ClientContext ctx;
    geogit::CreateVersionRequest request;
    request.set_situation_id(situation_id);
    request.set_message(message);
    auto* map = request.mutable_objects();
    for (const auto& entry : objects) {
        (*map)[entry.first] = entry.second;
    }
    geogit::VersionResponse response;
    grpc::Status status = stub->CreateVersion(&ctx, request, &response);
    assert_true(status.ok(), "CreateVersion failed: " + status.error_message());
    return response.version().meta().version_id();
}

void test_situations(geogit::GeoGit::Stub* stub, std::string& situation_id_out) {
    grpc::ClientContext ctx;
    geogit::CreateSituationRequest request;
    request.set_name("integration");
    request.set_description("integration test");
    geogit::SituationResponse response;
    grpc::Status status = stub->CreateSituation(&ctx, request, &response);
    assert_true(status.ok(), "CreateSituation failed: " + status.error_message());
    std::string id = response.situation().situation_id();
    assert_true(!id.empty(), "CreateSituation returned empty id");

    grpc::ClientContext get_ctx;
    geogit::GetSituationRequest get_request;
    get_request.set_situation_id(id);
    geogit::SituationResponse get_response;
    status = stub->GetSituation(&get_ctx, get_request, &get_response);
    assert_true(status.ok(), "GetSituation failed");
    assert_true(get_response.situation().name() == "integration", "GetSituation wrong name");

    grpc::ClientContext list_ctx;
    geogit::ListSituationsRequest list_request;
    geogit::ListSituationsResponse list_response;
    status = stub->ListSituations(&list_ctx, list_request, &list_response);
    assert_true(status.ok(), "ListSituations failed");
    assert_true(list_response.situations_size() >= 1, "ListSituations empty");

    situation_id_out = id;
}

std::string test_branch_create(geogit::GeoGit::Stub* stub, const std::string& situation_id) {
    grpc::ClientContext ctx;
    geogit::CreateBranchRequest request;
    request.set_situation_id(situation_id);
    request.set_name("main");
    geogit::BranchResponse response;
    grpc::Status status = stub->CreateBranch(&ctx, request, &response);
    assert_true(status.ok(), "CreateBranch failed: " + status.error_message());

    grpc::ClientContext name_ctx;
    geogit::GetBranchByNameRequest name_request;
    name_request.set_situation_id(situation_id);
    name_request.set_name("main");
    geogit::BranchResponse name_response;
    status = stub->GetBranchByName(&name_ctx, name_request, &name_response);
    assert_true(status.ok(), "GetBranchByName failed");
    assert_true(name_response.branch().branch_id() == response.branch().branch_id(),
                "GetBranchByName mismatch");

    return response.branch().branch_id();
}

void test_version_and_diff(geogit::GeoGit::Stub* stub, const std::string& situation_id,
                           const std::string& branch_id, const std::string& hash1,
                           const std::string& hash2) {
    std::string v1 = create_version(stub, situation_id, {{"obj-1", hash1}}, "v1");
    std::string v2 = create_version(stub, situation_id, {{"obj-1", hash1}, {"obj-2", hash2}}, "v2");

    grpc::ClientContext get_ctx;
    geogit::GetVersionRequest get_request;
    get_request.set_version_id(v1);
    geogit::VersionResponse get_response;
    grpc::Status status = stub->GetVersion(&get_ctx, get_request, &get_response);
    assert_true(status.ok(), "GetVersion failed");
    assert_true(get_response.version().objects().at("obj-1") == hash1, "GetVersion wrong object");

    grpc::ClientContext advance_ctx;
    geogit::AdvanceBranchRequest advance_request;
    advance_request.set_branch_id(branch_id);
    advance_request.set_new_version_id(v1);
    geogit::BranchResponse advance_response;
    status = stub->AdvanceBranch(&advance_ctx, advance_request, &advance_response);
    assert_true(status.ok(), "AdvanceBranch failed");
    assert_true(advance_response.branch().head_version_id() == v1, "AdvanceBranch head wrong");

    grpc::ClientContext diff_ctx;
    geogit::ComputeDiffRequest diff_request;
    diff_request.set_from_version_id(v1);
    diff_request.set_to_version_id(v2);
    geogit::DiffResponse diff_response;
    status = stub->ComputeDiff(&diff_ctx, diff_request, &diff_response);
    assert_true(status.ok(), "ComputeDiff failed");
    assert_true(diff_response.added_size() == 1 && diff_response.added(0) == "obj-2",
                "ComputeDiff added wrong");
    assert_true(diff_response.unchanged_size() == 1, "ComputeDiff unchanged wrong");

    grpc::ClientContext delta_ctx;
    geogit::GetDeltaRequest delta_request;
    delta_request.set_from_version_id(v1);
    delta_request.set_to_version_id(v2);
    geogit::DiffResponse delta_response;
    status = stub->GetDelta(&delta_ctx, delta_request, &delta_response);
    assert_true(status.ok(), "GetDelta failed (delta not cached)");
    assert_true(delta_response.added_size() == 1, "GetDelta added wrong");

    grpc::ClientContext status_ctx;
    geogit::GetStatusRequest status_request;
    status_request.set_branch_id(branch_id);
    (*status_request.mutable_proposed_objects())["obj-1"] = hash2;
    geogit::DiffResponse status_response;
    status = stub->GetStatus(&status_ctx, status_request, &status_response);
    assert_true(status.ok(), "GetStatus failed");
    assert_true(status_response.modified_size() == 1 && status_response.modified(0) == "obj-1",
                "GetStatus modified wrong");
}

void test_checkout_and_bbox(geogit::GeoGit::Stub* stub, const std::string& situation_id,
                            const std::string& hash1, const std::string& hash2) {
    std::string version =
        create_version(stub, situation_id, {{"obj-1", hash1}, {"obj-2", hash2}}, "checkout");

    grpc::ClientContext checkout_ctx;
    geogit::CheckoutRequest checkout_request;
    checkout_request.set_version_id(version);
    auto reader = stub->Checkout(&checkout_ctx, checkout_request);
    geogit::BPOResponse bpo;
    int count = 0;
    while (reader->Read(&bpo)) {
        assert_true(!bpo.bpo().hash().empty(), "Checkout BPO missing hash");
        ++count;
    }
    grpc::Status status = reader->Finish();
    assert_true(status.ok(), "Checkout failed");
    assert_true(count == 2, "Checkout wrong object count");

    grpc::ClientContext bbox_ctx;
    geogit::FindInBBoxRequest bbox_request;
    bbox_request.set_min_lon(29.0);
    bbox_request.set_min_lat(59.0);
    bbox_request.set_max_lon(32.0);
    bbox_request.set_max_lat(62.0);
    auto bbox_reader = stub->FindInBBox(&bbox_ctx, bbox_request);
    geogit::BPOResponse bbox_bpo;
    int bbox_count = 0;
    while (bbox_reader->Read(&bbox_bpo)) {
        ++bbox_count;
    }
    status = bbox_reader->Finish();
    assert_true(status.ok(), "FindInBBox failed");
    assert_true(bbox_count >= 2, "FindInBBox wrong count");
}

void test_merge(geogit::GeoGit::Stub* stub, const std::string& situation_id) {
    std::string base = create_version(stub, situation_id, {{"a", "h1"}}, "base");
    std::string ours = create_version(stub, situation_id, {{"a", "h2"}}, "ours");
    std::string theirs = create_version(stub, situation_id, {{"a", "h3"}}, "theirs");

    grpc::ClientContext manual_ctx;
    geogit::MergeRequest manual_request;
    manual_request.set_base_version_id(base);
    manual_request.set_ours_version_id(ours);
    manual_request.set_theirs_version_id(theirs);
    manual_request.set_strategy(geogit::MANUAL);
    geogit::MergeResponse manual_response;
    grpc::Status status = stub->Merge(&manual_ctx, manual_request, &manual_response);
    assert_true(status.ok(), "Merge (manual) failed");
    assert_true(manual_response.conflicts_size() == 1, "Merge expected one conflict");
    assert_true(manual_response.conflicts(0).type() == geogit::BOTH_MODIFIED,
                "Merge wrong conflict type");

    grpc::ClientContext ours_ctx;
    geogit::MergeRequest ours_request = manual_request;
    ours_request.set_strategy(geogit::OURS);
    geogit::MergeResponse ours_response;
    status = stub->Merge(&ours_ctx, ours_request, &ours_response);
    assert_true(status.ok(), "Merge (ours) failed");
    assert_true(ours_response.conflicts_size() == 0, "Merge (ours) should resolve conflicts");
    assert_true(ours_response.merged().at("a") == "h2", "Merge (ours) wrong result");
}

} // namespace

int main() {
    std::cout << "Running gRPC integration tests..." << std::endl;

    MongoDBConnection connection(get_mongo_uri(), "geoversion");
    connection.initialize_database();
    clear_collections(connection);

    geogit_server::GeoGitServiceImpl service(connection);

    int selected_port = 0;
    grpc::ServerBuilder builder;
    builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &selected_port);
    builder.RegisterService(&service);
    std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
    assert_true(server != nullptr, "Failed to start in-process server");

    std::string target = "127.0.0.1:" + std::to_string(selected_port);
    auto channel = grpc::CreateChannel(target, grpc::InsecureChannelCredentials());
    auto stub = geogit::GeoGit::NewStub(channel);

    std::string situation_id;
    test_situations(stub.get(), situation_id);
    std::string branch_id = test_branch_create(stub.get(), situation_id);

    std::string hash1 = store_point(stub.get(), 30.0, 60.0, "tree");
    std::string hash2 = store_point(stub.get(), 31.0, 61.0, "road");

    grpc::ClientContext bpo_ctx;
    geogit::GetBPORequest bpo_request;
    bpo_request.set_hash(hash1);
    geogit::BPOResponse bpo_response;
    grpc::Status bpo_status = stub->GetBPO(&bpo_ctx, bpo_request, &bpo_response);
    assert_true(bpo_status.ok(), "GetBPO failed");
    assert_true(bpo_response.bpo().hash() == hash1, "GetBPO wrong hash");

    test_version_and_diff(stub.get(), situation_id, branch_id, hash1, hash2);
    test_checkout_and_bbox(stub.get(), situation_id, hash1, hash2);
    test_merge(stub.get(), situation_id);

    server->Shutdown();
    std::cout << "gRPC integration tests passed." << std::endl;
    return EXIT_SUCCESS;
}
