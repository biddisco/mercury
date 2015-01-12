#------------------------------------------------------------------------------
# Set up test options
#------------------------------------------------------------------------------
set(MERCURY_TESTING_BUFFER_SIZE "16" CACHE STRING
  "Total buffer size (in MB) used for testing.")
mark_as_advanced(MERCURY_TESTING_BUFFER_SIZE)

set(MERCURY_TESTING_MAX_LOOP "1" CACHE STRING
  "Loop n times for average bandwidth/time measures.")
mark_as_advanced(MERCURY_TESTING_MAX_LOOP)

set(MERCURY_TESTING_NUM_THREADS "8" CACHE STRING
  "Number of server threads used for testing.")
mark_as_advanced(MERCURY_TESTING_NUM_THREADS)

option(MERCURY_TESTING_USE_THREAD_POOL
  "Run tests using thread pool." ON)
if(MERCURY_TESTING_USE_THREAD_POOL)
  set(MERCURY_TESTING_HAS_THREAD_POOL ON)
endif(MERCURY_TESTING_USE_THREAD_POOL)
mark_as_advanced(MERCURY_TESTING_USE_THREAD_POOL)

option(MERCURY_TESTING_VERIFY_DATA
  "Verify data when running tests (disable when measuring bandwidth)." ON)
if(MERCURY_TESTING_VERIFY_DATA)
  set(MERCURY_TESTING_HAS_VERIFY_DATA ON)
endif(MERCURY_TESTING_VERIFY_DATA)
mark_as_advanced(MERCURY_TESTING_VERIFY_DATA)

option(MERCURY_ENABLE_PARALLEL_TESTING
  "Enable testing in parallel (requires MPI)." OFF)
if(MERCURY_ENABLE_PARALLEL_TESTING OR NA_USE_MPI)
  set(MERCURY_HAS_PARALLEL_TESTING 1)

  # use N-1 clients (and 1 server) 
  math(EXPR MPI_NUM_CLIENTS "${MPIEXEC_MAX_NUMPROCS}-1")

  if(NA_USE_MPI AND NOT MERCURY_ENABLE_PARALLEL_TESTING)
    # Always force parallel testing if NA_USE_MPI is turned ON
    set(MERCURY_ENABLE_PARALLEL_TESTING "ON" CACHE BOOL
      "Enable testing in parallel (requires MPI)." FORCE)
  endif()
else()
  set(MERCURY_HAS_PARALLEL_TESTING 0)
endif()

set(MERCURY_TEST_INIT_COMMAND "" CACHE STRING
  "Command to run before a client/server test begins. Multiple commands are separated by ';'.")
mark_as_advanced(MERCURY_TEST_INIT_COMMAND)

set(MERCURY_TESTING_TEMP_DIRECTORY "." CACHE PATH
  "Location to use for temp data (default is current directory).")
mark_as_advanced(MERCURY_TESTING_TEMP_DIRECTORY)

option(MERCURY_TESTING_CORESIDENT "Enable testing of coresident mode." OFF)
mark_as_advanced(MERCURY_TESTING_CORESIDENT)

option(MERCURY_TESTING_SRUN_MPMD "Generate srun compatible test commands (srun --multi-prog config) " 0) 
mark_as_advanced(MERCURY_TESTING_SRUN_MPMD)

#------------------------------------------------------------------------------
# NA protocols used for testing
# (case where the NA plugin defines multiple protocols)
#------------------------------------------------------------------------------
if(NA_USE_BMI)
  set(NA_BMI_TESTING_PROTOCOL "" CACHE STRING "Protocol(s) used for testing (e.g., tcp;ib).")
  mark_as_advanced(NA_BMI_TESTING_PROTOCOL)
endif()

#Does not really make sense for MPI (so do not add an option for it)
#if(NA_USE_MPI)
#  set(NA_MPI_TESTING_PROTOCOL "" CACHE STRING "Protocol(s) used for testing (e.g., tcp;ib).")
#  mark_as_advanced(NA_MPI_TESTING_PROTOCOL)
#endif()

if(NA_USE_SSM)
  set(NA_SSM_TESTING_PROTOCOL "" CACHE STRING "Protocol(s) used for testing (e.g., tcp;ib).")
  mark_as_advanced(NA_SSM_TESTING_PROTOCOL)
endif()

if(NA_USE_CCI)
  set(NA_CCI_TESTING_PROTOCOL "" CACHE STRING "Protocol(s) used for testing (e.g., tcp;ib).")
  mark_as_advanced(NA_CCI_TESTING_PROTOCOL)
endif()

#------------------------------------------------------------------------------
# Compile kwsys library and setup TestDriver
#------------------------------------------------------------------------------
configure_file(
  ${CMAKE_CURRENT_SOURCE_DIR}/mercury_test_config.h.in
  ${CMAKE_CURRENT_BINARY_DIR}/mercury_test_config.h
)

include_directories(
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${CMAKE_CURRENT_BINARY_DIR}
  ${MERCURY_INCLUDES_BUILD_TIME}
)

if(MERCURY_ENABLE_PARALLEL_TESTING)
  find_package(MPI REQUIRED)
  if(MPI_FOUND)
    include_directories(${MPI_INCLUDE_PATH})
    set(MERCURY_TEST_EXT_LIB_DEPENDENCIES
      ${MERCURY_TEST_EXT_LIB_DEPENDENCIES}
      ${MPI_LIBRARIES}
    )
  else()
    message(FATAL_ERROR "Could not find MPI.")
  endif()
endif()

add_subdirectory(driver)

#------------------------------------------------------------------------------
# Set up test macros
#------------------------------------------------------------------------------
#
# hg prefix is added to executable
#
function(build_mercury_test test_name)
  add_executable(hg_test_${test_name} test_${test_name}.c)
  target_link_libraries(hg_test_${test_name} mercury_test)
  if(MERCURY_ENABLE_COVERAGE)
    set_coverage_flags(hg_test_${test_name})
  endif()
endfunction()

#-----------------------------------------------------------------------------
# Adds a test with the correct MPI command line
# supports standard mpiexec and also srun --multi-prog format
#-----------------------------------------------------------------------------
function(add_test_wrapper distributed test_name name_suffixes server_args client_args)
  # Set full test name
  set(full_test_name ${test_name})
  foreach(suffix ${name_suffixes})
    set(full_test_name ${full_test_name}_${suffix})
  endforeach()
  
  if (${distributed})
    # if we are running with srun 
    if (MERCURY_TESTING_SRUN_MPMD)
      file(WRITE ${CMAKE_CURRENT_BINARY_DIR}/hg_test_${full_test_name} 
        " 0   ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/hg_test_server ${server_args} \n"
        " 1-${MPI_NUM_CLIENTS} ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/hg_test_${test_name} ${client_args} \n"
      )
      add_test(
        NAME "mercury_${full_test_name}"
        COMMAND ${MPIEXEC} 
          --multi-prog ${CMAKE_CURRENT_BINARY_DIR}/hg_test_${full_test_name}
      )
    else()
      add_test(
        NAME "mercury_${full_test_name}"
        COMMAND ${MPIEXEC}
          ${MPIEXEC_NUMPROC_FLAG} 1 ${MPIEXEC_PREFLAGS} $<TARGET_FILE:hg_test_server> ${MPIEXEC_POSTFLAGS} ${server_args} :
          ${MPIEXEC_NUMPROC_FLAG} ${MPI_NUM_CLIENTS} ${MPIEXEC_PREFLAGS} $<TARGET_FILE:hg_test_${test_name}> ${MPIEXEC_POSTFLAGS} ${client_args}
      )
    endif()
  else()
    add_test(NAME "mercury_driver_${full_test_name}"
      COMMAND $<TARGET_FILE:mercury_test_driver>
      --server $<TARGET_FILE:hg_test_server> ${server_args}
      --client $<TARGET_FILE:hg_test_${test_name}> ${client_args}
    )
  endif() 
endfunction()

#-----------------------------------------------------------------------------
# Add a test for all supported comm and protocol variants
# an additional parameter list may be supplied or "" is empty
#-----------------------------------------------------------------------------
function(add_mercury_test test_name name_suffix params)
  foreach(comm ${NA_PLUGINS})
    string(TOUPPER ${comm} upper_comm)
   
    set(distributed "0")  
    # if the communication plugin only supports running on separete nodes
    if(NA_${upper_comm}_TESTING_DISTRIBUTED)
      set(distributed "1") 
    endif()

    # currently we pass the same args to server and client
    # support for different args to each process can be added here
    set(test_args "--comm ${comm} ${params} ")
    if(NA_${upper_comm}_TESTING_PROTOCOL)
      # add special args to test needed by certain protocols
      foreach(protocol ${NA_${upper_comm}_TESTING_PROTOCOL})
        set(test_args "${test_args} --protocol ${protocol}")
        add_test_wrapper(${distributed} ${test_name}"${name_suffix};${comm};${protocol}" ${test_args} ${test_args})
      endforeach()
    elseif(${comm} STREQUAL "verbs")
      # add special args to test needed by verbs plugin 
      set(test_args "${test_args} --device ${VERBS_DEVICE_NAME} --iface ${VERBS_INTERFACE_NAME}")
      add_test_wrapper(${distributed} ${test_name} "${name_suffix};${comm}" ${test_args} ${test_args})
    elseif(${comm} STREQUAL "mpi")
      # normal MPI test
      add_test_wrapper(${distributed} ${test_name} "${name_suffix};${comm}" ${test_args} ${test_args})
      # Static client/server version of test
      set(test_args "${test_args} --static")
      add_test_wrapper(${distributed} ${test_name} "${name_suffix};${comm};static" ${test_args} ${test_args})
      if(MERCURY_TESTING_CORESIDENT)
        set(test_args "${test_args} --self")
        if (MERCURY_ENABLE_PARALLEL_TESTING)
          add_test_wrapper(${distributed} ${test_name} "${name_suffix};${comm};self" ${test_args} ${test_args})
        else()
          add_test(NAME "mercury_${test_name}_self"
            COMMAND $<TARGET_FILE:hg_test_${test_name}> ${cores_test_args}
          )
        endif()
      endif()
    else()  
      add_test_wrapper(${distributed} ${test_name} "${name_suffix};${comm}" ${test_args} ${test_args})
    endif()
    # Coresident test
  endforeach()  
endfunction()

#------------------------------------------------------------------------------
# NA tests
#------------------------------------------------------------------------------
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/na)
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/na)

#------------------------------------------------------------------------------
# UTIL tests
#------------------------------------------------------------------------------
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/util)

#------------------------------------------------------------------------------
# mercury_test : Lib used by tests contains main test initialization etc
#------------------------------------------------------------------------------
add_library(mercury_test STATIC mercury_test.c mercury_rpc_cb.c)
target_link_libraries(mercury_test mercury na_test ${MERCURY_TEST_EXT_LIB_DEPENDENCIES})
if(MERCURY_ENABLE_COVERAGE)
  set_coverage_flags(mercury_test)
endif()

#------------------------------------------------------------------------------
# Set up test
#------------------------------------------------------------------------------

# Server used for testing
build_mercury_test(server)

set(MERCURY_tests
  rpc
  bulk
  bulk_seg
#  pipeline
  perf
  overflow
)
if(NOT WIN32)
  set(MERCURY_tests ${MERCURY_tests} posix)
endif()

# Build tests and add them to ctest
foreach(MERCURY_test ${MERCURY_tests})
  build_mercury_test(${MERCURY_test})
  add_mercury_test(${MERCURY_test} "" "")
endforeach()

add_mercury_test(bulk_seg "extra" "--extra")
add_mercury_test(bulk_seg "variable" "--variable")

#add_executable(hg_test_BW test_BW.cpp)
#target_link_libraries(hg_test_BW mercury_test)
#add_mercury_test_comm(BW verbs "" "${opt}" "device;${VERBS_DEVICE_NAME};iface;${VERBS_INTERFACE_NAME}")
