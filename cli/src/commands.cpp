#include "commands.h"

#include <google/protobuf/empty.pb.h>

#include <cstdint>
#include <ctime>
#include <iostream>
#include <string>
#include <vector>

namespace geogit_cli {

namespace {

std::string format_time(int64_t millis) {
    std::time_t seconds = static_cast<std::time_t>(millis / 1000);
    std::tm tm{};
    gmtime_r(&seconds, &tm);
    char buffer[32];
    std::strftime(buffer, sizeof(buffer), "%Y-%m-%d %H:%M:%S", &tm);
    return std::string(buffer) + " UTC";
}

bool split_object(const std::string& token, std::string& object_id, std::string& hash) {
    auto pos = token.find('=');
    if (pos == std::string::npos) {
        return false;
    }
    object_id = token.substr(0, pos);
    hash = token.substr(pos + 1);
    return true;
}

int fail(const grpc::Status& status) {
    std::cerr << "Error: " << status.error_message() << std::endl;
    return 1;
}

void print_situation(const geogit::Situation& situation) {
    std::cout << "situation " << situation.situation_id() << "\n";
    std::cout << "  name:        " << situation.name() << "\n";
    std::cout << "  description: " << situation.description() << "\n";
    std::cout << "  created:     " << format_time(situation.created_at()) << "\n";
}

void print_branch(const geogit::Branch& branch) {
    std::cout << "branch " << branch.branch_id() << "\n";
    std::cout << "  name: " << branch.name() << "\n";
    std::cout << "  head: "
              << (branch.head_version_id().empty() ? "(none)" : branch.head_version_id()) << "\n";
}

void print_version_meta(const geogit::VersionMeta& meta) {
    std::cout << "version " << meta.version_id() << "\n";
    if (!meta.author().empty()) {
        std::cout << "Author: " << meta.author() << "\n";
    }
    std::cout << "Date:   " << format_time(meta.created_at()) << "\n";
    for (const auto& parent : meta.parent_version_ids()) {
        std::cout << "Parent: " << parent << "\n";
    }
    if (!meta.message().empty()) {
        std::cout << "\n    " << meta.message() << "\n";
    }
    std::cout << "\n";
}

void print_diff(const geogit::DiffResponse& diff) {
    for (const auto& id : diff.added()) {
        std::cout << "A  " << id << "\n";
    }
    for (const auto& id : diff.removed()) {
        std::cout << "D  " << id << "\n";
    }
    for (const auto& id : diff.modified()) {
        std::cout << "M  " << id << "\n";
    }
    for (const auto& pair : diff.likely_modified()) {
        std::cout << "~  " << pair.removed_hash() << " -> " << pair.added_hash() << "  (conf "
                  << pair.confidence() << ", " << pair.distance_m() << " m)\n";
    }
    std::cout << diff.added_size() << " added, " << diff.removed_size() << " removed, "
              << diff.modified_size() << " modified, " << diff.unchanged_size() << " unchanged";
    if (diff.likely_modified_size() > 0) {
        std::cout << ", " << diff.likely_modified_size() << " likely-modified";
    }
    std::cout << "\n";
}

int cmd_init(GeoGitClient& client, const std::vector<std::string>& args) {
    if (args.size() < 2) {
        std::cerr << "usage: geogit init <name> [description]" << std::endl;
        return 1;
    }

    geogit::CreateSituationRequest request;
    request.set_name(args[1]);
    if (args.size() > 2) {
        request.set_description(args[2]);
    }

    grpc::ClientContext situation_ctx;
    geogit::SituationResponse situation_response;
    grpc::Status status =
        client.stub()->CreateSituation(&situation_ctx, request, &situation_response);
    if (!status.ok()) {
        return fail(status);
    }

    const std::string& situation_id = situation_response.situation().situation_id();

    geogit::CreateBranchRequest branch_request;
    branch_request.set_situation_id(situation_id);
    branch_request.set_name("main");

    grpc::ClientContext branch_ctx;
    geogit::BranchResponse branch_response;
    status = client.stub()->CreateBranch(&branch_ctx, branch_request, &branch_response);
    if (!status.ok()) {
        return fail(status);
    }

    std::cout << "Initialized situation " << situation_id << " with branch 'main' ("
              << branch_response.branch().branch_id() << ")\n";
    return 0;
}

int cmd_situations(GeoGitClient& client) {
    grpc::ClientContext ctx;
    geogit::ListSituationsRequest request;
    geogit::ListSituationsResponse response;
    grpc::Status status = client.stub()->ListSituations(&ctx, request, &response);
    if (!status.ok()) {
        return fail(status);
    }
    for (const auto& situation : response.situations()) {
        print_situation(situation);
        std::cout << "\n";
    }
    return 0;
}

int cmd_log(GeoGitClient& client, const std::vector<std::string>& args) {
    if (args.size() < 2) {
        std::cerr << "usage: geogit log <situation_id>" << std::endl;
        return 1;
    }
    grpc::ClientContext ctx;
    geogit::ListVersionsRequest request;
    request.set_situation_id(args[1]);
    geogit::ListVersionsResponse response;
    grpc::Status status = client.stub()->ListVersions(&ctx, request, &response);
    if (!status.ok()) {
        return fail(status);
    }
    for (const auto& meta : response.versions()) {
        print_version_meta(meta);
    }
    return 0;
}

int cmd_show(GeoGitClient& client, const std::vector<std::string>& args) {
    if (args.size() < 2) {
        std::cerr << "usage: geogit show <version_id>" << std::endl;
        return 1;
    }
    grpc::ClientContext ctx;
    geogit::GetVersionRequest request;
    request.set_version_id(args[1]);
    geogit::VersionResponse response;
    grpc::Status status = client.stub()->GetVersion(&ctx, request, &response);
    if (!status.ok()) {
        return fail(status);
    }
    print_version_meta(response.version().meta());
    std::cout << "Objects (" << response.version().objects_size() << "):\n";
    for (const auto& entry : response.version().objects()) {
        std::cout << "  " << entry.first << " -> " << entry.second << "\n";
    }
    return 0;
}

int cmd_status(GeoGitClient& client, const std::vector<std::string>& args) {
    if (args.size() < 2) {
        std::cerr << "usage: geogit status <branch_id> [object_id=hash ...]" << std::endl;
        return 1;
    }
    grpc::ClientContext ctx;
    geogit::GetStatusRequest request;
    request.set_branch_id(args[1]);
    auto* proposed = request.mutable_proposed_objects();
    for (size_t i = 2; i < args.size(); ++i) {
        std::string object_id;
        std::string hash;
        if (split_object(args[i], object_id, hash)) {
            (*proposed)[object_id] = hash;
        }
    }
    geogit::DiffResponse response;
    grpc::Status status = client.stub()->GetStatus(&ctx, request, &response);
    if (!status.ok()) {
        return fail(status);
    }
    print_diff(response);
    return 0;
}

int cmd_diff(GeoGitClient& client, const std::vector<std::string>& args) {
    if (args.size() < 3) {
        std::cerr << "usage: geogit diff <from_version> <to_version> [--er]" << std::endl;
        return 1;
    }
    grpc::ClientContext ctx;
    geogit::ComputeDiffRequest request;
    request.set_from_version_id(args[1]);
    request.set_to_version_id(args[2]);
    for (size_t i = 3; i < args.size(); ++i) {
        if (args[i] == "--er") {
            request.set_entity_resolution(true);
        }
    }
    geogit::DiffResponse response;
    grpc::Status status = client.stub()->ComputeDiff(&ctx, request, &response);
    if (!status.ok()) {
        return fail(status);
    }
    print_diff(response);
    return 0;
}

int cmd_delta(GeoGitClient& client, const std::vector<std::string>& args) {
    if (args.size() < 3) {
        std::cerr << "usage: geogit delta <from_version> <to_version>" << std::endl;
        return 1;
    }
    grpc::ClientContext ctx;
    geogit::GetDeltaRequest request;
    request.set_from_version_id(args[1]);
    request.set_to_version_id(args[2]);
    geogit::DiffResponse response;
    grpc::Status status = client.stub()->GetDelta(&ctx, request, &response);
    if (!status.ok()) {
        return fail(status);
    }
    print_diff(response);
    return 0;
}

int cmd_merge(GeoGitClient& client, const std::vector<std::string>& args) {
    if (args.size() < 4) {
        std::cerr << "usage: geogit merge <base> <ours> <theirs> [--ours|--theirs]" << std::endl;
        return 1;
    }
    grpc::ClientContext ctx;
    geogit::MergeRequest request;
    request.set_base_version_id(args[1]);
    request.set_ours_version_id(args[2]);
    request.set_theirs_version_id(args[3]);
    request.set_strategy(geogit::MANUAL);
    for (size_t i = 4; i < args.size(); ++i) {
        if (args[i] == "--ours") {
            request.set_strategy(geogit::OURS);
        } else if (args[i] == "--theirs") {
            request.set_strategy(geogit::THEIRS);
        }
    }
    geogit::MergeResponse response;
    grpc::Status status = client.stub()->Merge(&ctx, request, &response);
    if (!status.ok()) {
        return fail(status);
    }
    std::cout << "Merged objects (" << response.merged_size() << "):\n";
    for (const auto& entry : response.merged()) {
        std::cout << "  " << entry.first << " -> " << entry.second << "\n";
    }
    if (response.conflicts_size() > 0) {
        std::cout << "Conflicts (" << response.conflicts_size() << "):\n";
        for (const auto& conflict : response.conflicts()) {
            std::cout << "  " << conflict.object_id() << "  base=" << conflict.base_hash()
                      << " ours=" << conflict.ours_hash() << " theirs=" << conflict.theirs_hash()
                      << "\n";
        }
        return 2;
    }
    return 0;
}

int cmd_commit(GeoGitClient& client, const std::vector<std::string>& args) {
    if (args.size() < 2) {
        std::cerr << "usage: geogit commit <situation_id> -m <message> [-a author] "
                     "[-p parent ...] [object_id=hash ...]"
                  << std::endl;
        return 1;
    }
    grpc::ClientContext ctx;
    geogit::CreateVersionRequest request;
    request.set_situation_id(args[1]);
    auto* objects = request.mutable_objects();
    for (size_t i = 2; i < args.size(); ++i) {
        if (args[i] == "-m" && i + 1 < args.size()) {
            request.set_message(args[++i]);
        } else if (args[i] == "-a" && i + 1 < args.size()) {
            request.set_author(args[++i]);
        } else if (args[i] == "-p" && i + 1 < args.size()) {
            request.add_parent_version_ids(args[++i]);
        } else {
            std::string object_id;
            std::string hash;
            if (split_object(args[i], object_id, hash)) {
                (*objects)[object_id] = hash;
            }
        }
    }
    geogit::VersionResponse response;
    grpc::Status status = client.stub()->CreateVersion(&ctx, request, &response);
    if (!status.ok()) {
        return fail(status);
    }
    std::cout << "Created version " << response.version().meta().version_id() << "\n";
    return 0;
}

int cmd_branch(GeoGitClient& client, const std::vector<std::string>& args) {
    if (args.size() < 2) {
        std::cerr << "usage: geogit branch <list|create|advance|delete> ..." << std::endl;
        return 1;
    }
    const std::string& sub = args[1];

    if (sub == "list" && args.size() >= 3) {
        grpc::ClientContext ctx;
        geogit::ListBranchesRequest request;
        request.set_situation_id(args[2]);
        geogit::ListBranchesResponse response;
        grpc::Status status = client.stub()->ListBranches(&ctx, request, &response);
        if (!status.ok()) {
            return fail(status);
        }
        for (const auto& branch : response.branches()) {
            print_branch(branch);
            std::cout << "\n";
        }
        return 0;
    }

    if (sub == "create" && args.size() >= 4) {
        grpc::ClientContext ctx;
        geogit::CreateBranchRequest request;
        request.set_situation_id(args[2]);
        request.set_name(args[3]);
        if (args.size() > 4) {
            request.set_head_version_id(args[4]);
        }
        geogit::BranchResponse response;
        grpc::Status status = client.stub()->CreateBranch(&ctx, request, &response);
        if (!status.ok()) {
            return fail(status);
        }
        print_branch(response.branch());
        return 0;
    }

    if (sub == "advance" && args.size() >= 4) {
        grpc::ClientContext ctx;
        geogit::AdvanceBranchRequest request;
        request.set_branch_id(args[2]);
        request.set_new_version_id(args[3]);
        geogit::BranchResponse response;
        grpc::Status status = client.stub()->AdvanceBranch(&ctx, request, &response);
        if (!status.ok()) {
            return fail(status);
        }
        print_branch(response.branch());
        return 0;
    }

    if (sub == "delete" && args.size() >= 3) {
        grpc::ClientContext ctx;
        geogit::DeleteBranchRequest request;
        request.set_branch_id(args[2]);
        google::protobuf::Empty response;
        grpc::Status status = client.stub()->DeleteBranch(&ctx, request, &response);
        if (!status.ok()) {
            return fail(status);
        }
        std::cout << "Deleted branch " << args[2] << "\n";
        return 0;
    }

    std::cerr << "usage: geogit branch <list|create|advance|delete> ..." << std::endl;
    return 1;
}

int cmd_checkout(GeoGitClient& client, const std::vector<std::string>& args) {
    if (args.size() < 2) {
        std::cerr << "usage: geogit checkout <version_id>" << std::endl;
        return 1;
    }
    grpc::ClientContext ctx;
    geogit::CheckoutRequest request;
    request.set_version_id(args[1]);

    auto reader = client.stub()->Checkout(&ctx, request);
    geogit::BPOResponse response;
    int count = 0;
    while (reader->Read(&response)) {
        const auto& bpo = response.bpo();
        std::cout << bpo.hash() << "  " << bpo.object_id() << "  " << bpo.geometry_json() << "\n";
        ++count;
    }
    grpc::Status status = reader->Finish();
    if (!status.ok()) {
        return fail(status);
    }
    std::cout << count << " object(s)\n";
    return 0;
}

int cmd_store_bpo(GeoGitClient& client, const std::vector<std::string>& args) {
    if (args.size() < 2) {
        std::cerr << "usage: geogit store-bpo <geometry_json> [attributes_json]" << std::endl;
        return 1;
    }
    grpc::ClientContext ctx;
    geogit::StoreBPORequest request;
    request.set_geometry_json(args[1]);
    if (args.size() > 2) {
        request.set_attributes_json(args[2]);
    }
    geogit::StoreBPOResponse response;
    grpc::Status status = client.stub()->StoreBPO(&ctx, request, &response);
    if (!status.ok()) {
        return fail(status);
    }
    std::cout << response.hash() << "\n";
    return 0;
}

int cmd_get_bpo(GeoGitClient& client, const std::vector<std::string>& args) {
    if (args.size() < 2) {
        std::cerr << "usage: geogit get-bpo <hash>" << std::endl;
        return 1;
    }
    grpc::ClientContext ctx;
    geogit::GetBPORequest request;
    request.set_hash(args[1]);
    geogit::BPOResponse response;
    grpc::Status status = client.stub()->GetBPO(&ctx, request, &response);
    if (!status.ok()) {
        return fail(status);
    }
    const auto& bpo = response.bpo();
    std::cout << "hash:       " << bpo.hash() << "\n";
    std::cout << "object_id:  " << bpo.object_id() << "\n";
    std::cout << "geometry:   " << bpo.geometry_json() << "\n";
    std::cout << "attributes: " << bpo.attributes_json() << "\n";
    return 0;
}

} // namespace

void print_usage() {
    std::cout
        << "geogit - git-like CLI for the GeoVersion control system\n\n"
        << "usage: geogit [--server addr] <command> [args]\n\n"
        << "commands:\n"
        << "  init <name> [description]                      create a situation + 'main' branch\n"
        << "  situations                                     list situations\n"
        << "  log <situation_id>                             list versions\n"
        << "  show <version_id>                              show a version and its objects\n"
        << "  status <branch_id> [object_id=hash ...]        diff a working set vs branch HEAD\n"
        << "  diff <from> <to> [--er]                        diff two versions (--er: level 2)\n"
        << "  delta <from> <to>                              fetch a cached delta\n"
        << "  merge <base> <ours> <theirs> [--ours|--theirs] 3-way merge\n"
        << "  commit <situation_id> -m <msg> [-a author]\n"
        << "         [-p parent ...] [object_id=hash ...]    create a version\n"
        << "  branch list <situation_id>\n"
        << "  branch create <situation_id> <name> [head]\n"
        << "  branch advance <branch_id> <version_id>\n"
        << "  branch delete <branch_id>\n"
        << "  checkout <version_id>                          stream a version's BPOs\n"
        << "  store-bpo <geometry_json> [attributes_json]    store a BPO, print its hash\n"
        << "  get-bpo <hash>                                 fetch a BPO\n\n"
        << "server address: --server flag or GEOGIT_SERVER env (default localhost:50051)\n";
}

int dispatch(GeoGitClient& client, const std::vector<std::string>& args) {
    const std::string& command = args[0];

    if (command == "init") {
        return cmd_init(client, args);
    }
    if (command == "situations") {
        return cmd_situations(client);
    }
    if (command == "log") {
        return cmd_log(client, args);
    }
    if (command == "show") {
        return cmd_show(client, args);
    }
    if (command == "status") {
        return cmd_status(client, args);
    }
    if (command == "diff") {
        return cmd_diff(client, args);
    }
    if (command == "delta") {
        return cmd_delta(client, args);
    }
    if (command == "merge") {
        return cmd_merge(client, args);
    }
    if (command == "commit") {
        return cmd_commit(client, args);
    }
    if (command == "branch") {
        return cmd_branch(client, args);
    }
    if (command == "checkout") {
        return cmd_checkout(client, args);
    }
    if (command == "store-bpo") {
        return cmd_store_bpo(client, args);
    }
    if (command == "get-bpo") {
        return cmd_get_bpo(client, args);
    }
    if (command == "help" || command == "--help" || command == "-h") {
        print_usage();
        return 0;
    }

    std::cerr << "unknown command: " << command << "\n\n";
    print_usage();
    return 1;
}

} // namespace geogit_cli
