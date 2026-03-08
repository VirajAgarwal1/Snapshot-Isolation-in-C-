#ifndef SERVER_CONNECTION_HANDLER_HPP
#define SERVER_CONNECTION_HANDLER_HPP

// Each accepted TCP client connection is represented by a file descriptor.
// The worker thread receives this descriptor so it can read/write that client socket.
void handleConnection(int connectionFd);

#endif