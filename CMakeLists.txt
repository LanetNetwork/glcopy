cmake_minimum_required(VERSION 2.6.4)

if (NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
	message(STATUS "Setting build type to 'Release' as none was specified.")
	set(CMAKE_BUILD_TYPE Release CACHE STRING "Choose the type of build." FORCE)
	set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS "Debug" "Release" "MinSizeRel" "RelWithDebInfo")
endif (NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)

if (CMAKE_BUILD_TYPE MATCHES Debug)
	message("This is DEBUG build")
endif (CMAKE_BUILD_TYPE MATCHES Debug)

include(FindPkgConfig)
if (PKG_CONFIG_FOUND)
	pkg_check_modules(GLUSTERFS-API glusterfs-api)
	if(NOT CMAKE_BUILD_TYPE MATCHES Debug)
		pkg_check_modules(LIBTCMALLOC_MINIMAL libtcmalloc_minimal)
		if(LIBTCMALLOC_MINIMAL_FOUND EQUAL 1)
			set(GB_LD_EXTRA "${LIBTCMALLOC_MINIMAL_LIBRARIES}")
		endif (LIBTCMALLOC_MINIMAL_FOUND EQUAL 1)
	endif(NOT CMAKE_BUILD_TYPE MATCHES Debug)
endif (PKG_CONFIG_FOUND)

set(CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake-scripts)
find_package(CEPHFS)

set(CMAKE_C_FLAGS
	"-std=c99 -D_DEFAULT_SOURCE -D_GNU_SOURCE -D_FILE_OFFSET_BITS=64 -W -Wall -Wextra -pedantic -Wwrite-strings -Winit-self -Wcast-qual -Wpointer-arith -Wstrict-aliasing -Wformat=2 -Wmissing-declarations -Wmissing-include-dirs -Wno-unused-parameter -Wuninitialized -Wold-style-definition -Wstrict-prototypes -Wmissing-prototypes")
if (NOT "${CMAKE_C_COMPILER_ID}" STREQUAL "Intel")
	set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wpadded")
endif (NOT "${CMAKE_C_COMPILER_ID}" STREQUAL "Intel")

set(CMAKE_C_FLAGS_DEBUG "-O0 -g -ggdb -DMODE_DEBUG")
if ("${CMAKE_C_COMPILER_ID}" STREQUAL "Intel")
	set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -w3")
endif ("${CMAKE_C_COMPILER_ID}" STREQUAL "Intel")

set(CMAKE_C_FLAGS_RELEASE "-O3 -march=native -DMODE_NORMAL")

include_directories(.
	pfcephfsquirks
	pfcquirks
	pfglfsquirks
	pfpthquirks
	${CEPHFS_INCLUDE_DIRS})

add_subdirectory(pfcephfsquirks)
add_subdirectory(pfcquirks)
add_subdirectory(pfglfsquirks)
add_subdirectory(pfpthquirks)

add_executable(glcopy
	glcopy.c)

target_link_libraries(glcopy
	pthread
	${CEPHFS_LIBRARIES}
	${GLUSTERFS-API_LIBRARIES}
	ln_pfcfsq
	ln_pfcquirks
	ln_pfglfsquirks
	ln_pfpthquirks
	${GB_LD_EXTRA})
