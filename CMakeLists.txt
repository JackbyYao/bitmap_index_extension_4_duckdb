cmake_minimum_required(VERSION 3.5)

# Force C++17 for extension build to match DuckDB core
set(CMAKE_CXX_STANDARD "17" CACHE STRING "C++ standard to enforce")
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

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

# rdynamic is not available on macOS
if (CMAKE_SYSTEM_NAME STREQUAL "Linux")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -rdynamic")
endif()

find_package(OpenSSL REQUIRED)
find_package(roaring CONFIG REQUIRED)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

## Inherit C++ standard from top-level DuckDB build to avoid ODR mismatches

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

# Prevent the static extension archive target from being linked into the
# loadable extension target. Some DuckDB build configurations propagate the
# `${EXTENSION_NAME}` static archive into the loadable target which can lead
# to duplicate symbol/linker errors when both the core static library and the
# extension archive end up on the same link line. Keep the static archive in
# the build (so `build_static_extension` still works) but remove it from the
# loadable target's LINK_LIBRARIES list.
if(TARGET ${LOADABLE_EXTENSION_NAME})
    get_target_property(_old_link_libs ${LOADABLE_EXTENSION_NAME} LINK_LIBRARIES)
    if(_old_link_libs)
        set(_new_link_libs)
        foreach(_lib ${_old_link_libs})
            if(NOT _lib STREQUAL ${EXTENSION_NAME})
                list(APPEND _new_link_libs ${_lib})
            endif()
        endforeach()
        set_target_properties(${LOADABLE_EXTENSION_NAME} PROPERTIES LINK_LIBRARIES "${_new_link_libs}")
    endif()
endif()
