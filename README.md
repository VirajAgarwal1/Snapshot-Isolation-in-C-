# Minimal TCP Server + Thread Pool (C++20)

This project demonstrates a small educational TCP server in modern C++20.

- The main thread accepts TCP connections.
- Each accepted socket file descriptor is wrapped in a `std::function<void()>` task.
- A fixed-size thread pool executes those tasks.
- Each worker reads up to 1024 bytes, prints the request, writes `OK\n`, and closes the socket.

## Build And Run

```bash
mkdir build
cd build
cmake ..
make
./src/tcp_server
```

## Run Tests

```bash
cd build
ctest --output-on-failure
```

## Quick `nc` Example

Terminal 1:

```bash
cd build
./src/tcp_server
```

Terminal 2:

```bash
nc 127.0.0.1 8080
hello from netcat
```

Expected client response:

```text
OK
```