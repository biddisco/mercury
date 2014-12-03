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

function(add_na_test_comm test_name server client comm protocol)
  if(MPI_NUM_CLIENTS)
    math(EXPR LOOP_CLIENTS "${MPI_NUM_CLIENTS}-1")
  else()
    set(LOOP_CLIENTS 1)
  endif()

  # Set full test name
  set(full_test_name ${test_name})
  set(opt_names ${comm} ${protocol})
  foreach(opt_name ${opt_names})
    set(full_test_name ${full_test_name}_${opt_name})
  endforeach()

  # Set test arguments
  set(test_args --comm ${comm})
  if(protocol)
    set(test_args ${test_args} --protocol ${protocol})
  endif()

    #
    # Dynamic client/server test
    #
    if(${comm} STREQUAL "verbs")
      if (MERCURY_TESTING_SRUN_MPMD)
        string(REPLACE ";" " " test_args "${test_args}")
        file(WRITE ${CMAKE_CURRENT_BINARY_DIR}/na_test_${full_test_name} 
          " 0   ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/na_test_${server} ${test_args} --device ${VERBS_DEVICE_NAME} --iface ${VERBS_INTERFACE_NAME} \n"
        )
        file(APPEND ${CMAKE_CURRENT_BINARY_DIR}/na_test_${full_test_name}
          " 1-${MPI_NUM_CLIENTS} ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/na_test_${client} ${test_args} --device ${VERBS_DEVICE_NAME} --iface ${VERBS_INTERFACE_NAME} \n"
        )
        add_test(
          NAME "na_test_${full_test_name}"
          COMMAND ${MPIEXEC} -n ${MPIEXEC_MAX_NUMPROCS} --multi-prog ${CMAKE_CURRENT_BINARY_DIR}/na_test_${full_test_name}
        )
      else()
        add_test(
          NAME "na_${full_test_name}"
          COMMAND ${MPIEXEC}
            -n 1 $<TARGET_FILE:na_test_${server}> ${test_args} :
            -n ${MPI_NUM_CLIENTS} $<TARGET_FILE:na_test_${client}> ${test_args} --device ${VERBS_DEVICE_NAME} --iface ${VERBS_INTERFACE_NAME}
        )
      endif()
    else()
      add_test(NAME "na_${full_test_name}"
        COMMAND $<TARGET_FILE:mercury_test_driver>
        --server $<TARGET_FILE:na_test_${server}>
        --client $<TARGET_FILE:na_test_${client}> ${test_args}
      )
    endif()

    # Static client/server test
    if(${comm} STREQUAL "mpi")
      set(static_test_name ${full_test_name}_static)
      set(static_test_args ${test_args} --static)
      add_test(NAME "na_${static_test_name}"
        COMMAND ${MPIEXEC} ${MPIEXEC_NUMPROC_FLAG} 1
        ${MPIEXEC_PREFLAGS} $<TARGET_FILE:na_test_${server}> ${MPIEXEC_POSTFLAGS}
        ${static_test_args} : ${MPIEXEC_NUMPROC_FLAG} ${MPIEXEC_MAX_NUMPROCS}
        ${MPIEXEC_PREFLAGS} $<TARGET_FILE:na_test_${client}> ${static_test_args}
      )
  endif()
endfunction()

function(add_na_test test_name server client)
  foreach(comm ${NA_PLUGINS})
    string(TOUPPER ${comm} upper_comm)
    if(NA_${upper_comm}_TESTING_PROTOCOL)
      foreach(protocol ${NA_${upper_comm}_TESTING_PROTOCOL})
        add_na_test_comm(${test_name} ${server} ${client} ${comm} ${protocol})
      endforeach()
    else()
      add_na_test_comm(${test_name} ${server} ${client} ${comm} "")
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
add_na_test(simple server client)
