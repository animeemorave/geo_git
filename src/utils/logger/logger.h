#pragma once

#include <string>
#include <iostream>
#include <sstream>
#include <chrono>
#include <iomanip>

namespace geoversion {
namespace utils {

enum class LogLevel {
    DEBUG,
    INFO,
    WARNING,
    ERROR
};

class Logger {
public:
    static void log(LogLevel level, const std::string& message);
    static void debug(const std::string& message);
    static void info(const std::string& message);
    static void warning(const std::string& message);
    static void error(const std::string& message);

private:
    static std::string level_to_string(LogLevel level);
    static std::string get_timestamp();
};

}
}
