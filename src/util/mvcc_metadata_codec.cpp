#include "util/mvcc_metadata_codec.hpp"

#include <charconv>
#include <cstddef>
#include <system_error>

namespace util {

std::string encodeMVCCMetadata(const MVCCMetadata& metadata) {
    return std::to_string(metadata.transactionId) + "|" + (metadata.isDead ? "1" : "0");
}

std::optional<MVCCMetadata> decodeMVCCMetadata(const std::string& encodedMetadata) {
    if (encodedMetadata.empty()) {
        return std::nullopt;
    }

    const std::size_t delimiterPos = encodedMetadata.find('|');
    if (delimiterPos == std::string::npos || delimiterPos == 0 ||
        delimiterPos == encodedMetadata.size() - 1) {
        return std::nullopt;
    }

    if (encodedMetadata.find('|', delimiterPos + 1) != std::string::npos) {
        return std::nullopt;
    }

    std::uint64_t transactionId = 0;
    const char* begin = encodedMetadata.data();
    const char* delimiter = begin + static_cast<std::ptrdiff_t>(delimiterPos);
    const auto parseResult = std::from_chars(begin, delimiter, transactionId);
    if (parseResult.ec != std::errc() || parseResult.ptr != delimiter) {
        return std::nullopt;
    }

    const char deadMarker = encodedMetadata[delimiterPos + 1];
    if (delimiterPos + 2 != encodedMetadata.size()) {
        return std::nullopt;
    }

    if (deadMarker != '0' && deadMarker != '1') {
        return std::nullopt;
    }

    return MVCCMetadata{transactionId, deadMarker == '1'};
}

}  // namespace util