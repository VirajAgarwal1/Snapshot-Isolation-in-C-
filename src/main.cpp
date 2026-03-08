#include "server/connection_handler.hpp"
#include "server/thread_pool/thread_pool.hpp"

#include <cerrno>
#include <csignal>
#include <cstdlib>
#include <iostream>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

namespace {
constexpr int kPort = 8080;
constexpr int kBacklog = 16;
constexpr std::size_t kWorkerCount = 4;

int createListeningSocket() {
    const int listenFd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (listenFd < 0) {
        std::perror("socket");
        return -1;
    }

    int reuseAddr = 1;
    if (::setsockopt(listenFd, SOL_SOCKET, SO_REUSEADDR, &reuseAddr, sizeof(reuseAddr)) != 0) {
        std::perror("setsockopt(SO_REUSEADDR)");
        ::close(listenFd);
        return -1;
    }

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_ANY);
    address.sin_port = htons(kPort);

    if (::bind(listenFd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0) {
        std::perror("bind");
        ::close(listenFd);
        return -1;
    }

    if (::listen(listenFd, kBacklog) != 0) {
        std::perror("listen");
        ::close(listenFd);
        return -1;
    }

    return listenFd;
}
}

int main() {
    // Prevent process termination when writing to sockets closed by the peer.
    std::signal(SIGPIPE, SIG_IGN);

    ThreadPool pool(kWorkerCount);
    const int listenFd = createListeningSocket();
    if (listenFd < 0) {
        return EXIT_FAILURE;
    }

    std::cout << "Listening on 0.0.0.0:" << kPort << " with " << kWorkerCount << " workers\n";

    while (true) {
        // accept() returns a new file descriptor for one TCP client connection.
        const int connectionFd = ::accept(listenFd, nullptr, nullptr);
        if (connectionFd < 0) {
            if (errno == EINTR) {
                continue;
            }
            std::perror("accept");
            continue;
        }

        // We pass connectionFd into the task so a worker can handle this exact client.
        pool.submit([connectionFd]() { handleConnection(connectionFd); });
    }

    ::close(listenFd);
    return EXIT_SUCCESS;
}