find_package(Threads REQUIRED)

add_library(naive_storage_engine
	naive_storage_engine/naive_storage_engine.cpp
)
target_include_directories(naive_storage_engine PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)

add_library(mvcc_metadata_codec
	util/mvcc_metadata_codec.cpp
)
target_include_directories(mvcc_metadata_codec PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)

add_library(mvcc
	mvcc/mvcc.cpp
)
target_include_directories(mvcc PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(mvcc PUBLIC naive_storage_engine mvcc_metadata_codec)

add_library(server_core
	server/thread_pool/thread_pool.cpp
	server/connection_handler.cpp
)

target_include_directories(server_core PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(server_core PUBLIC Threads::Threads)

add_executable(tcp_server main.cpp)
target_link_libraries(tcp_server PRIVATE server_core)
