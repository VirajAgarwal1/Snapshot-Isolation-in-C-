find_package(Threads REQUIRED)

add_library(server_core
	server/thread_pool/thread_pool.cpp
	server/connection_handler.cpp
)

target_include_directories(server_core PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(server_core PUBLIC Threads::Threads)

add_executable(tcp_server main.cpp)
target_link_libraries(tcp_server PRIVATE server_core)
