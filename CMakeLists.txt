cmake_minimum_required(VERSION 3.16)
project(PgPooler VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Dependencies
find_package(PkgConfig REQUIRED)
pkg_check_modules(LIBEVENT REQUIRED libevent)

include(FetchContent)
FetchContent_Declare(
  yaml-cpp
  URL https://github.com/jbeder/yaml-cpp/archive/refs/tags/yaml-cpp-0.7.0.tar.gz
)
set(YAML_CPP_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(YAML_CPP_BUILD_TOOLS OFF CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(yaml-cpp)

add_executable(pgpooler
  src/main.cpp
  src/common/log.cpp
  src/config/config.cpp
  src/config/config_yaml.cpp
  src/pool/backend_connection_pool.cpp
  src/pool/connection_wait_queue.cpp
  src/protocol/error_response.cpp
  src/protocol/message.cpp
  src/server/dispatcher.cpp
  src/server/fd_send.cpp
  src/server/listener.cpp
  src/session/client_session.cpp
)

target_include_directories(pgpooler PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/src
  ${LIBEVENT_INCLUDE_DIRS}
)

target_link_libraries(pgpooler PRIVATE
  ${LIBEVENT_LIBRARIES}
  yaml-cpp::yaml-cpp
)

# Install
install(TARGETS pgpooler RUNTIME DESTINATION bin)
