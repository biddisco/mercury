cmake_minimum_required(VERSION 2.8.5 FATAL_ERROR)
project(NA C CXX)

#------------------------------------------------------------------------------
# Setup install directories and cmake module
#------------------------------------------------------------------------------
if(NOT NA_INSTALL_BIN_DIR)
  set(NA_INSTALL_BIN_DIR ${CMAKE_INSTALL_PREFIX}/bin)
endif()
if(NOT NA_INSTALL_LIB_DIR)
  set(NA_INSTALL_LIB_DIR ${CMAKE_INSTALL_PREFIX}/lib)
endif()
if(NOT NA_INSTALL_INCLUDE_DIR)
  set(NA_INSTALL_INCLUDE_DIR ${CMAKE_INSTALL_PREFIX}/include)
endif()
if(NOT NA_INSTALL_DATA_DIR)
  set(NA_INSTALL_DATA_DIR ${CMAKE_INSTALL_PREFIX}/share)
endif()

set(NA_CMAKE_DIR "${NA_SOURCE_DIR}/CMake")
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${NA_CMAKE_DIR})

#-----------------------------------------------------------------------------
# Targets built within this project are exported at Install time for use
# by other projects.
#-----------------------------------------------------------------------------
if(NOT NA_EXPORTED_TARGETS)
  set(NA_EXPORTED_TARGETS ${MERCURY_EXPORTED_TARGETS})
endif()

#------------------------------------------------------------------------------
# External dependencies
#------------------------------------------------------------------------------
# mercury_util
include_directories(${MERCURY_UTIL_EXT_INCLUDE_DEPENDENCIES})
include_directories(${NA_SOURCE_DIR}/../util)

#------------------------------------------------------------------------------
# Set sources - 1
# Setting this before NA_USE_XXX to avoid header file conflict if Mercury 
# was previously installed in $INSTALL_PREFIX and this prefix is also used
# for NA plugin libraries/dependencies.
# (Refer Mercury-Trac #15)
#------------------------------------------------------------------------------
include_directories(
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${CMAKE_CURRENT_BINARY_DIR}
)

# BMI
option(NA_USE_BMI "Use BMI." OFF)
if(NA_USE_BMI)
  find_package(BMI REQUIRED)
  if(BMI_FOUND)
    message(STATUS "BMI include directory: ${BMI_INCLUDE_DIR}")
    set(NA_PLUGINS ${NA_PLUGINS} bmi)
    set(NA_HAS_BMI 1)
    include_directories(${BMI_INCLUDE_DIR})
    set(NA_EXT_LIB_DEPENDENCIES
      ${NA_EXT_LIB_DEPENDENCIES}
      ${BMI_LIBRARIES}
    )
    set(NA_EXT_INCLUDE_DEPENDENCIES
      ${NA_EXT_INCLUDE_DEPENDENCIES}
      ${BMI_INCLUDE_DIRS}
    )
    include(CheckCSourceCompiles)
    check_c_source_compiles("#include <bmi.h>
      int main(int argc, char **argv) {
        BMI_set_info(0,BMI_ZOID_POST_TIMEOUT,0);
      }
     " HAVE_BMI_ZOID_TIMEOUT)
  else()
    message(FATAL_ERROR "Could not find BMI.")
  endif()
endif()

option(NA_USE_VERBS "Use VERBS." OFF)
if(NA_USE_VERBS)
  find_package(IB_VERBS)
  find_package(RDMA_CM)
  find_package(Boost 1.48.0 REQUIRED regex)
  find_package(rdmahelper)

  if(IB_VERBS_FOUND AND RDMA_CM_FOUND AND rdmahelper_FOUND)
    set(NA_PLUGINS ${NA_PLUGINS} verbs)
    set(NA_HAS_VERBS 1)
    set(NA_EXT_LIB_DEPENDENCIES
      ${NA_EXT_LIB_DEPENDENCIES}
      ${IB_VERBS_LIBRARIES}
      ${RDMA_CM_LIBRARIES}
      ${Boost_LIBRARIES}
    )
    set(NA_EXT_INCLUDE_DEPENDENCIES
      ${NA_EXT_INCLUDE_DEPENDENCIES}
      ${BGC_INCLUDE_DIR}
    )
    include_directories(
      ${Boost_INCLUDE_DIR}
      ${IB_VERBS_INCLUDE_DIRS}
      ${RDMA_CM_INCLUDE_DIRS}
      ${rdmahelper_INCLUDE_DIR}
    )
    set(NA_EXT_INCLUDE_DEPENDENCIES
      ${NA_EXT_INCLUDE_DEPENDENCIES}
      ${BGC_INCLUDE_DIR}
    )
    add_definitions(${rdmahelper_DEFINITIONS})

    set(VERBS_DEVICE_NAME "roq" CACHE STRING "Infiniband device name to use")
    set(VERBS_INTERFACE_NAME "roq0" CACHE STRING "Infiniband interface name to use")
    set(VERBS_PORT_NUMBER "10000" CACHE STRING "Infiniband port number to use")  
    
    if (${VERBS_DEVICE_NAME} MATCHES "bgvrnic")
      ADD_DEFINITIONS(-DBGQ_BGVRNIC)
    endif()
    
  else()
    message("Fatal : Could not find RDMA.")
  endif()
endif()


# SSM
option(NA_USE_SSM "Use SSM." OFF)
if(NA_USE_SSM)
  find_package(SSM REQUIRED)
  if(SSM_FOUND)
    message(STATUS "SSM include directory: ${SSM_INCLUDE_DIR}")
    set(NA_PLUGINS ${NA_PLUGINS} ssm)
    set(NA_HAS_SSM 1)
    include_directories(${SSM_INCLUDE_DIR})
    set(NA_EXT_LIB_DEPENDENCIES
      ${NA_EXT_LIB_DEPENDENCIES}
      ${SSM_LIBRARIES}
    )
    set(NA_EXT_INCLUDE_DEPENDENCIES
      ${NA_EXT_INCLUDE_DEPENDENCIES}
      ${SSM_INCLUDE_DIRS}
    )
  else()
     message(FATAL_ERROR "Could not find SSM.")
  endif()
endif()


# MPI
option(NA_USE_MPI "Use MPI." OFF)
if(NA_USE_MPI)
  find_package(MPI REQUIRED)
  if(MPI_FOUND)
    message(STATUS "MPI include directory: ${MPI_INCLUDE_PATH}")
    set(NA_HAS_MPI 1)
    set(NA_PLUGINS ${NA_PLUGINS} mpi)
    include_directories(${MPI_INCLUDE_PATH})
    set(NA_EXT_LIB_DEPENDENCIES
      ${NA_EXT_LIB_DEPENDENCIES}
      ${MPI_LIBRARIES}
    )
    set(NA_EXT_INCLUDE_DEPENDENCIES
      ${NA_EXT_INCLUDE_DEPENDENCIES}
      ${MPI_INCLUDE_PATH}
    )
    # Extra job setup for Cray MPI without ALPS support
    option(NA_MPI_USE_GNI_SETUP
      "Define NA_MPI_Gni_job_setup() to setup the Aries NIC resources for the job." OFF)
    if(NA_MPI_USE_GNI_SETUP)
      find_package(GNI REQUIRED)
      if(GNI_FOUND)
        set(NA_MPI_HAS_GNI_SETUP 1)
        include_directories(${GNI_INCLUDE_DIRS})
        set(NA_EXT_LIB_DEPENDENCIES
          ${NA_EXT_LIB_DEPENDENCIES}
          ${GNI_LIBRARIES}
        )
      else()
        message(FATAL_ERROR "Could not find GNI.")
      endif()
    endif()
  # we may want to add an option for MPI_VERSION >= 3
  else()
    message(FATAL_ERROR "Could not find MPI.")
  endif()
endif()

#------------------------------------------------------------------------------
# Configure module header files
#------------------------------------------------------------------------------
# Set unique vars used in the autogenerated config file (symbol import/export)
if(BUILD_SHARED_LIBS)
  set(NA_BUILD_SHARED_LIBS 1)
  set(NA_LIBTYPE SHARED)
else()
  set(NA_BUILD_SHARED_LIBS 0)
  set(NA_LIBTYPE STATIC)
endif()

if(MERCURY_ENABLE_VERBOSE_ERROR)
  set(NA_HAS_VERBOSE_ERROR 1)
else()
  set(NA_HAS_VERBOSE_ERROR 0)
endif()

configure_file(
  ${CMAKE_CURRENT_SOURCE_DIR}/na_config.h.in
  ${CMAKE_CURRENT_BINARY_DIR}/na_config.h
)

#------------------------------------------------------------------------------
# Set sources - 2
#------------------------------------------------------------------------------
set(NA_SRCS
  ${CMAKE_CURRENT_SOURCE_DIR}/na.c
)

if(NA_USE_BMI)
  set(NA_SRCS
    ${NA_SRCS}
    ${CMAKE_CURRENT_SOURCE_DIR}/na_bmi.c
  )
endif()

if(NA_USE_SSM)
  set(NA_SRCS
    ${NA_SRCS}
    ${CMAKE_CURRENT_SOURCE_DIR}/na_ssm.c
  )
endif()

if(NA_USE_MPI)
  set(NA_SRCS
    ${NA_SRCS}
    ${CMAKE_CURRENT_SOURCE_DIR}/na_mpi.c
  )
endif()

if(NA_USE_VERBS)
  set(NA_SRCS
    ${NA_SRCS}
    ${CMAKE_CURRENT_SOURCE_DIR}/na_verbs.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/MercuryController.cc
  )
endif()

#----------------------------------------------------------------------------
# Libraries
#----------------------------------------------------------------------------
# NA
add_library(na ${NA_SRCS})
target_link_libraries(na ${NA_EXT_LIB_DEPENDENCIES} mercury_util)
mercury_set_lib_options(na "na" ${NA_LIBTYPE})
if(MERCURY_ENABLE_COVERAGE)
  set_coverage_flags(na)
endif()

if(NA_USE_VERBS)
  target_link_libraries(na
    rdmahelper
    ${Boost_REGEX_LIBRARY_DEBUG}
  )
endif()
#---------------------------------------------------------------------------
# Add Target(s) to CMake Install
#---------------------------------------------------------------------------
install(
  TARGETS
    na
  EXPORT
    ${NA_EXPORTED_TARGETS}
  LIBRARY DESTINATION ${NA_INSTALL_LIB_DIR}
  ARCHIVE DESTINATION ${NA_INSTALL_LIB_DIR}
  RUNTIME DESTINATION ${NA_INSTALL_BIN_DIR}
)

#-----------------------------------------------------------------------------
# Specify project header files to be installed
#-----------------------------------------------------------------------------
set(NA_HEADERS
  ${CMAKE_CURRENT_SOURCE_DIR}/na.h
  ${CMAKE_CURRENT_SOURCE_DIR}/na_error.h
  ${CMAKE_CURRENT_BINARY_DIR}/na_config.h
)

if(NA_USE_BMI)
  set(NA_HEADERS
    ${NA_HEADERS}
    ${CMAKE_CURRENT_SOURCE_DIR}/na_bmi.h
  )
endif()

if(NA_USE_MPI)
  set(NA_HEADERS
    ${NA_HEADERS}
    ${CMAKE_CURRENT_SOURCE_DIR}/na_mpi.h
  )
endif()

#-----------------------------------------------------------------------------
# Add file(s) to CMake Install
#-----------------------------------------------------------------------------
install(
  FILES
    ${NA_HEADERS}
  DESTINATION
    ${NA_INSTALL_INCLUDE_DIR}
  COMPONENT
    headers
)

#------------------------------------------------------------------------------
# Set variables for parent scope
#------------------------------------------------------------------------------
set(NA_PLUGINS ${NA_PLUGINS} PARENT_SCOPE)
set(NA_EXT_INCLUDE_DEPENDENCIES ${NA_EXT_INCLUDE_DEPENDENCIES} PARENT_SCOPE)
set(NA_EXT_LIB_DEPENDENCIES ${NA_EXT_LIB_DEPENDENCIES} PARENT_SCOPE)
