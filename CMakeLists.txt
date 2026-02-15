cmake_minimum_required(VERSION 3.16)
project(PgPooler VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Dependencies
find_package(PkgConfig REQUIRED)
pkg_check_modules(LIBEVENT REQUIRED libevent)

# pugixml: скачиваем архив по URL (без git — в Docker часто проблемы с сертификатами при clone)
include(FetchContent)
FetchContent_Declare(
  pugixml
  URL https://github.com/zeux/pugixml/archive/refs/tags/v1.15.tar.gz
)
FetchContent_MakeAvailable(pugixml)

add_executable(pgpooler
  src/main.cpp
  src/config/config.cpp
  src/protocol/message.cpp
  src/server/listener.cpp
  src/session/client_session.cpp
)

target_include_directories(pgpooler PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/src
  ${LIBEVENT_INCLUDE_DIRS}
)

target_link_libraries(pgpooler PRIVATE
  ${LIBEVENT_LIBRARIES}
  pugixml::pugixml
)

# Install
install(TARGETS pgpooler RUNTIME DESTINATION bin)
