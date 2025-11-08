cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME bitmap_idx)

# DuckDB's extension distribution supports vcpkg. As such, dependencies can be added in ./vcpkg.json and then
# used in cmake with find_package. Feel free to remove or replace with other dependencies.
# Note that it should also be removed from vcpkg.json to prevent needlessly installing it..

# Add vcpkg packages to CMAKE_PREFIX_PATH if using vcpkg
# vcpkg toolchain should handle this, but we add it explicitly as a fallback
if(DEFINED VCPKG_BUILD AND VCPKG_BUILD AND DEFINED CMAKE_TOOLCHAIN_FILE)
    # Find vcpkg root directory from toolchain file path
    get_filename_component(VCPKG_ROOT_DIR "${CMAKE_TOOLCHAIN_FILE}" DIRECTORY)
    get_filename_component(VCPKG_ROOT_DIR "${VCPKG_ROOT_DIR}" DIRECTORY)
    get_filename_component(VCPKG_ROOT_DIR "${VCPKG_ROOT_DIR}" DIRECTORY)
    
    # Add vcpkg packages directory to search path (vcpkg uses packages/ in manifest mode)
    if(EXISTS "${VCPKG_ROOT_DIR}/packages")
        list(APPEND CMAKE_PREFIX_PATH "${VCPKG_ROOT_DIR}/packages")
    endif()
    # Also check installed directory (vcpkg uses installed/ in classic mode)
    if(EXISTS "${VCPKG_ROOT_DIR}/installed")
        list(APPEND CMAKE_PREFIX_PATH "${VCPKG_ROOT_DIR}/installed")
    endif()
endif()

find_package(OpenSSL REQUIRED)
find_package(roaring CONFIG REQUIRED)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
add_subdirectory(src)
include_directories(src)


#set(EXTENSION_SOURCES src/bitmap_idx_extension.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Link OpenSSL in both the static library as the loadable extension
target_link_libraries(${EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto roaring::roaring)
target_link_libraries(${LOADABLE_EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto roaring::roaring)


install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
