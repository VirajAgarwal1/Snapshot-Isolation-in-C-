#include "server/connection_handler.hpp"
#include "server/thread_pool/thread_pool.hpp"

#include <atomic>
#include <chrono>
#include <cstring>
#include <future>
#include <iostream>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>

bool threadPoolExecutesTasks() {
    constexpr int kTaskCount = 32;

    ThreadPool pool(4);
    std::atomic<int> completed{0};
    std::promise<void> allDone;
    std::future<void> doneFuture = allDone.get_future();

    for (int i = 0; i < kTaskCount; ++i) {
        pool.submit([&completed, &allDone]() {
            const int now = completed.fetch_add(1) + 1;
            if (now == kTaskCount) {
                allDone.set_value();
            }
        });
    }

    const auto status = doneFuture.wait_for(std::chrono::seconds(2));
    return status == std::future_status::ready && completed.load() == kTaskCount;
}

bool connectionHandlerRespondsOk() {
    int fds[2] = {-1, -1};
    if (::socketpair(AF_UNIX, SOCK_STREAM, 0, fds) != 0) {
        return false;
    }

    const char* request = "hello from test\n";
    (void)::write(fds[0], request, std::strlen(request));

    std::thread worker([&]() { handleConnection(fds[1]); });

    char response[16] = {};
    const ssize_t n = ::read(fds[0], response, sizeof(response));
    ::close(fds[0]);

    worker.join();

    return n == 3 && std::string(response, static_cast<std::size_t>(n)) == "OK\n";
}

int main() {
    const bool poolOk = threadPoolExecutesTasks();
    const bool connectionOk = connectionHandlerRespondsOk();

    if (!poolOk) {
        std::cerr << "threadPoolExecutesTasks failed\n";
    }

    if (!connectionOk) {
        std::cerr << "connectionHandlerRespondsOk failed\n";
    }

    return (poolOk && connectionOk) ? 0 : 1;
}