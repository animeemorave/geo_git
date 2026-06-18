#include "client.h"
#include "commands.h"

#include <cstdlib>
#include <string>
#include <vector>

int main(int argc, char* argv[]) {
    std::string target = "localhost:50051";
    if (const char* env = std::getenv("GEOGIT_SERVER"); env && env[0] != '\0') {
        target = env;
    }

    std::vector<std::string> args;
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if ((arg == "--server" || arg == "-s") && i + 1 < argc) {
            target = argv[++i];
        } else {
            args.push_back(arg);
        }
    }

    if (args.empty()) {
        geogit_cli::print_usage();
        return 1;
    }

    geogit_cli::GeoGitClient client(target);
    return geogit_cli::dispatch(client, args);
}
