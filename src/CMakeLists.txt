add_subdirectory(index)

set(EXTENSION_SOURCES
    ${EXTENSION_SOURCES}
    ${CMAKE_CURRENT_SOURCE_DIR}/bitmap_idx_extension.cpp
PARENT_SCOPE)