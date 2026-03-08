#include "server/connection_handler.hpp"

#include <array>
#include <cstring>
#include <iostream>
#include <unistd.h>

namespace {
constexpr std::size_t kBufferSize = 1024;
}

void handleConnection(int connectionFd) {
    std::array<char, kBufferSize> buffer{};

    const ssize_t bytesRead = ::read(connectionFd, buffer.data(), buffer.size());
    if (bytesRead > 0) {
        std::cout << "Request (" << bytesRead << " bytes):\n";
        std::cout.write(buffer.data(), bytesRead);
        if (buffer[static_cast<std::size_t>(bytesRead) - 1] != '\n') {
            std::cout << '\n';
        }
    }

    const char* response = "OK\n";
    (void)::write(connectionFd, response, std::strlen(response));

    ::close(connectionFd);
}