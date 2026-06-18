#pragma once

#include "client.h"

#include <string>
#include <vector>

namespace geogit_cli {

void print_usage();

int dispatch(GeoGitClient& client, const std::vector<std::string>& args);

} // namespace geogit_cli
