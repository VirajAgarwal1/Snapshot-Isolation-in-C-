#ifndef UTIL_MVCC_METADATA_CODEC_HPP
#define UTIL_MVCC_METADATA_CODEC_HPP

#include <cstdint>
#include <optional>
#include <string>

namespace util {

struct MVCCMetadata {
    std::uint64_t transactionId = 0;
    bool isDead = false;
};

std::string encodeMVCCMetadata(const MVCCMetadata& metadata);
std::optional<MVCCMetadata> decodeMVCCMetadata(const std::string& encodedMetadata);

}  // namespace util

#endif