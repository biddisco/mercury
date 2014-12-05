#------------------------------------------------------------------------------
# Set up test macros
#------------------------------------------------------------------------------
#
# na prefix is added to executable
#
function(build_na_test test_name)
  add_executable(na_test_${test_name} test_${test_name}.c)
  target_link_libraries(na_test_${test_name} na_test)
  if(MERCURY_ENABLE_COVERAGE)
    set_coverage_flags(na_test_${test_name})
  endif()
endfunction()

#-----------------------------------------------------------------------------
# Adds a test with the correct MPI command line
# supports standard mpiexec and also srun --multiprog format
#-----------------------------------------------------------------------------
function(add_na_test_mpi test_name name_suffixes server_args client_args)
  # Set full test name
  set(full_test_name ${test_name})
  foreach(opt_name ${name_suffixes})
    set(full_test_name ${full_test_name}_${opt_name})
  endforeach()
  
  if (MERCURY_TESTING_SRUN_MPMD)
    file(WRITE ${CMAKE_CURRENT_BINARY_DIR}/na_test_${full_test_name} 
      " 0   ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/na_test_server ${server_args} \n"
      " 1-${MPI_NUM_CLIENTS} ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/na_test_client ${client_args} \n"
    )
    add_test(
      NAME "na_test_${full_test_name}"
      COMMAND ${MPIEXEC} 
        --multi-prog ${CMAKE_CURRENT_BINARY_DIR}/na_test_${full_test_name}
    )
  else()
    add_test(
      NAME "na_${full_test_name}"
      COMMAND ${MPIEXEC}
        ${MPIEXEC_NUMPROC_FLAG} 1 ${MPIEXEC_PREFLAGS} $<TARGET_FILE:na_test_server> ${MPIEXEC_POSTFLAGS} ${server_args} :
        ${MPIEXEC_NUMPROC_FLAG} ${MPI_NUM_CLIENTS} ${MPIEXEC_PREFLAGS} $<TARGET_FILE:na_test_client> ${MPIEXEC_POSTFLAGS} ${client_args}
    )
  endif() 
endfunction()

#-----------------------------------------------------------------------------
# Add a test for all supported comm and protocol variants
# an additional parameter list may be supplied or "" is empty
#-----------------------------------------------------------------------------
function(add_na_test test_name name_suffix params)
  foreach(comm ${NA_PLUGINS})
    string(TOUPPER ${comm} upper_comm)
    set(test_args "--comm ${comm} ${params} ")
    if(NA_${upper_comm}_TESTING_PROTOCOL)
      # add special args to test needed by certain protocols
      foreach(protocol ${NA_${upper_comm}_TESTING_PROTOCOL})
        set(test_args "${test_args} --protocol ${protocol}")
        add_na_test_mpi(${test_name} "${name_suffix};${comm};${protocol}" ${test_args} ${test_args})
      endforeach()
    elseif(${comm} STREQUAL "verbs")
      # add special args to test needed by verbs plugin 
      set(test_args "${test_args} --device ${VERBS_DEVICE_NAME} --iface ${VERBS_INTERFACE_NAME}")
      add_na_test_mpi(${test_name} "${name_suffix};${comm}" ${test_args} ${test_args})
    elseif(${comm} STREQUAL "mpi")
      # normal MPI test
      add_na_test_mpi(${test_name} "${name_suffix};${comm}" ${test_args} ${test_args})
      # Static client/server version of test
      set(test_args "${test_args} --static")
      add_na_test_mpi(${test_name} "${name_suffix};${comm};static" ${test_args} ${test_args})
    else()  
      add_na_test_mpi(${test_name} "${name_suffix};${comm}" ${test_args} ${test_args})
    endif()
  endforeach()  
endfunction()

#------------------------------------------------------------------------------
# na_test : Lib used by tests contains main test initialization etc
#------------------------------------------------------------------------------
add_library(na_test STATIC na_test.c na_test_getopt.c)
target_link_libraries(na_test na ${MERCURY_TEST_EXT_LIB_DEPENDENCIES})
if(MERCURY_ENABLE_COVERAGE)
  set_coverage_flags(na_test)
endif()

#------------------------------------------------------------------------------
# Network abstraction test
build_na_test(client)
build_na_test(server)

#------------------------------------------------------------------------------
# Set list of tests

# Client / server test with all enabled NA plugins
add_na_test(simple "" "")
