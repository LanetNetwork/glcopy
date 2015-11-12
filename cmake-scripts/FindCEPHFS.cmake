# - Try to find cephfs API
# Once done this will define
#
#  CEPHFS_FOUND - system has CEPHFS
#  CEPHFS_INCLUDE_DIRS - the CEPHFS include directory
#  CEPHFS_LIBRARIES - Link these to use CEPHFS
#  CEPHFS_DEFINITIONS - Compiler switches required for using CEPHFS
#
#  Copyright (c) 2011 Lee Hambley <lee.hambley@gmail.com>
#  Modified by Oleksandr Natalenko aka post-factum <oleksandr@natalenko.name>
#
#  Redistribution and use is allowed according to the terms of the New
#  BSD license.
#

if (CEPHFS_LIBRARIES AND CEPHFS_INCLUDE_DIRS)
  # in cache already
  set(CEPHFS_FOUND TRUE)
else (CEPHFS_LIBRARIES AND CEPHFS_INCLUDE_DIRS)

  find_path(CEPHFS_INCLUDE_DIR
    NAMES
      libcephfs.h
    PATHS
      /usr/include
      /usr/local/include
	  /usr/include/cephfs
	  /usr/local/include/cephfs
  )

  find_library(CEPHFS_LIBRARY
    NAMES
      libcephfs.so
    PATHS
      /usr/lib
      /usr/local/lib
      /usr/lib64
      /usr/local/lib64
  )

  set(CEPHFS_INCLUDE_DIRS
    ${CEPHFS_INCLUDE_DIR}
  )

  if (CEPHFS_LIBRARY)
    set(CEPHFS_LIBRARIES
        ${CEPHFS_LIBRARIES}
        ${CEPHFS_LIBRARY}
    )
  endif (CEPHFS_LIBRARY)

  include(FindPackageHandleStandardArgs)
  find_package_handle_standard_args(CEPHFS DEFAULT_MSG CEPHFS_LIBRARIES CEPHFS_INCLUDE_DIRS)

  mark_as_advanced(CEPHFS_INCLUDE_DIRS CEPHFS_LIBRARIES)

endif (CEPHFS_LIBRARIES AND CEPHFS_INCLUDE_DIRS)

