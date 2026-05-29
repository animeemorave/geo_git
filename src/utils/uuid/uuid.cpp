#include "uuid.h"

#include <openssl/rand.h>
#include <sstream>
#include <iomanip>
#include <stdexcept>

namespace geoversion {
namespace utils {

std::string generate_uuid() {
    unsigned char bytes[16];
    if (RAND_bytes(bytes, sizeof(bytes)) != 1) {
        throw std::runtime_error("Failed to generate random bytes for UUID");
    }

    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;

    std::ostringstream oss;
    oss << std::hex << std::setfill('0');

    for (int i = 0; i < 16; ++i) {
        if (i == 4 || i == 6 || i == 8 || i == 10) {
            oss << '-';
        }
        oss << std::setw(2) << static_cast<int>(bytes[i]);
    }

    return oss.str();
}

} // namespace utils
} // namespace geoversion
