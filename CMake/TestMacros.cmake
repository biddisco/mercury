#-----------------------------------------------------------------------------
# Generate a test name from a prefix/name/suffixlist
#
# Arguments:
# result (output)
#   the generated name will be placed into this var
# test_name 
#   a simple string such as "rpc" which is the base test name
# name_prefix
#   a prefix such as "mercury" or "na" which is a prefix to the name
# name_suffixes 
#   a list such as "cci;sm;extra" which will be converted into a string and 
#   appended to the test name to give the full test name (mercury_rpc_cci_sm_extra)
#-----------------------------------------------------------------------------
macro(generate_test_name result test_name name_prefix name_suffixes)
  # Generate the full test name from name plus suffixes
  set(${result} ${name_prefix}_${test_name})
  foreach(suffix ${name_suffixes})
    set(${result} ${full_test_name}_${suffix})
  endforeach()
endmacro()

#-----------------------------------------------------------------------------
# Create a test using srun MPMD syntax
#-----------------------------------------------------------------------------
function(test_slurm_mpmd full_test_name server_exe server_args client_exe client_args)
  # use GENERATE instead of WRITE to evaluate $<TARGET_ expressions before writing
  file(GENERATE 
    OUTPUT 
      ${CMAKE_CURRENT_BINARY_DIR}/srun_script_${full_test_name}
    CONTENT 
      "0 ${server_exe} ${server_args} \n1-${MPI_NUM_CLIENTS} ${client_exe} ${client_args} \n"
  )
  # add test using srun and config file as param
  add_test(
    NAME "${full_test_name}"
    COMMAND ${SLURM_SRUN_COMMAND} 
      --multi-prog ${CMAKE_CURRENT_BINARY_DIR}/srun_script_${full_test_name}
  )
endfunction()

#-----------------------------------------------------------------------------
# Create a test using mpi MPMD syntax
#-----------------------------------------------------------------------------
function(test_mpi_mpmd full_test_name server_exe server_args client_exe client_args)
  # we need to make sure arguments are individually quoted and not passed as a long string
  separate_arguments(_server_args UNIX_COMMAND "${server_args}")
  separate_arguments(_client_args UNIX_COMMAND "${client_args}")
  
  # add standard mpi MPMD command
  add_test(
    NAME "${full_test_name}"
    COMMAND ${MPIEXEC}
      ${MPIEXEC_NUMPROC_FLAG} 1 ${MPIEXEC_PREFLAGS} ${server_exe} ${MPIEXEC_POSTFLAGS} ${_server_args} :
      ${MPIEXEC_NUMPROC_FLAG} ${MPI_NUM_CLIENTS} ${MPIEXEC_PREFLAGS} ${client_exe} ${MPIEXEC_POSTFLAGS} ${_client_args}
  )
endfunction()

#-----------------------------------------------------------------------------
# Create a test that launches using the mercury test driver
#-----------------------------------------------------------------------------
function(test_mercury_driver full_test_name server_exe server_args client_exe client_args)
  # we need to make sure arguments are individually quoted and not passed as a long string
  separate_arguments(_server_args UNIX_COMMAND "${server_args}")
  separate_arguments(_client_args UNIX_COMMAND "${client_args}")
  
  # add test using mercury driver to launch client and server
  add_test(NAME "${full_test_name}"
    COMMAND  
      $<TARGET_FILE:mercury_test_driver>
      --server ${server_exe} ${_server_args}
      --client ${client_exe} ${_client_args}
  )
endfunction()

#-----------------------------------------------------------------------------
# Add a test which may be a parallel job, or a serial one
# Note : MERCURY_TESTING_WITH_SLURM controls if distributed tests are spawned
# using ${MPIEXEC} or ${SLURM_SRUN_COMMAND}
#
# Arguments:
# distributed 
#   if 1, test is run using mpi/batch scheduler to place executables
#   on nodes. Server will run on rank 0, clients on successive nodes
#   if 0, the test will be run using the mercury test driver to launch
#   server and clients.
# test_name 
#   a simple string such as "rpc" which is the base test name
# name_prefix
#   a prefix such as "mercury" or "na" which is a prefix to the name
# name_suffixes 
#   a list such as "cci;sm;extra" which will be converted into a string and 
#   appended to the test name to give the full test name (mercury_rpc_cci_sm_extra)
# server_exe
#   the server executable (such as hg_test_server)  
# server_args
#   parameters for the server such as "--comm bmi"
# client_exe 
#   the client executable (such as hg_test_rpc)
# client_args
#   parameters for the client such as "--comm bmi"
#
#-----------------------------------------------------------------------------
function(mercury_test_wrapper distributed test_name name_prefix name_suffixes server_exe server_args client_exe client_args)
  # Generate the full test name from name plus suffixes
  generate_test_name(full_test_name "${test_name}" "${name_prefix}" "${name_suffixes}")
  
  if (${distributed})
    if (MERCURY_TESTING_WITH_SLURM)
      test_slurm_mpmd(${full_test_name} "${server_exe}" "${server_args}" "${client_exe}" "${client_args}")
    else()
      test_mpi_mpmd(${full_test_name} "${server_exe}" "${server_args}" "${client_exe}" "${client_args}")
    endif()
  else()
    test_mercury_driver(${full_test_name} "${server_exe}" "${server_args}" "${client_exe}" "${client_args}")
  endif() 
endfunction()

#-----------------------------------------------------------------------------
# Add a test for all supported comm and protocol variants
# an additional argument/parameter list may be supplied (use "" for no args)
#
# for each comm type, we add certain options depending on need
#-----------------------------------------------------------------------------
function(add_framework_test test_name name_prefix name_suffix framework args) 
  foreach(comm ${NA_PLUGINS})
    string(TOUPPER ${comm} upper_comm)
   
    set(_distributed "0")  
    # if the communication plugin only supports running on separete nodes
    if(NA_${upper_comm}_TESTING_DISTRIBUTED)
      set(_distributed "1") 
    endif()

    # currently we pass the same args to server and client
    # support for different args to each process can be added here
    set(_test_args "--comm ${comm} ${args}")
    if(NA_${upper_comm}_TESTING_PROTOCOL)
      # add special args to test needed by certain protocols
      foreach(protocol ${NA_${upper_comm}_TESTING_PROTOCOL})
        set(_distributed "0")  
        # if the communication plugin only supports running on separete nodes
        string(TOUPPER ${protocol} upper_protocol)  
        if(NA_${upper_comm}_${upper_protocol}_TESTING_DISTRIBUTED)
          set(_distributed "1") 
        endif()
        set(test_args "${_test_args} --protocol ${protocol}")
        mercury_test_wrapper(${_distributed} ${test_name} "${name_prefix}" "${name_suffix};${comm};${protocol}" 
          "$<TARGET_FILE:${framework}_server>" "${test_args}" 
          "$<TARGET_FILE:${framework}_${test_name}>" "${test_args}"
        )
      endforeach()
    elseif(${comm} STREQUAL "verbs")
      # add special args to test needed by verbs plugin 
      set(test_args "${_test_args} --device ${VERBS_DEVICE_NAME} --iface ${VERBS_INTERFACE_NAME}")
      mercury_test_wrapper(${_distributed} ${test_name} "${name_prefix}" "${name_suffix};${comm}" 
          "$<TARGET_FILE:${framework}_server>" "${test_args}" 
          "$<TARGET_FILE:${framework}_${test_name}>" "${test_args}"
      )
    elseif(${comm} STREQUAL "mpi")
      # normal MPI test
      set(test_args "${_test_args}")
      if (MERCURY_TESTING_ENABLE_DYNAMIC_MPI)
        # stanard MPI tests are launched using the Mercury Test driver, so set distributed=0
        mercury_test_wrapper(0 ${test_name} "${name_prefix}" "${name_suffix};${comm}" 
            "$<TARGET_FILE:${framework}_server>" "${test_args}" 
            "$<TARGET_FILE:${framework}_${test_name}>" "${test_args}"
        )
      endif()
      # Static client/server version of test
      set(test_args "${_test_args} --static")
      mercury_test_wrapper(${_distributed} ${test_name} "${name_prefix}" "${name_suffix};${comm};static" 
          "$<TARGET_FILE:${framework}_server>" "${test_args}" 
          "$<TARGET_FILE:${framework}_${test_name}>" "${test_args}"
      )
      # self client/server version
      if(MERCURY_TESTING_CORESIDENT)
        set(test_args "${_test_args} --self")
        generate_test_name(full_test_name "${test_name}" "${name_prefix}" "${name_suffix};mpi;self")
        separate_arguments(_test_args UNIX_COMMAND "${test_args}")
        if (MERCURY_ENABLE_PARALLEL_TESTING)       
          add_test(
            NAME ${full_test_name}
            COMMAND ${MPIEXEC} 
              ${MPIEXEC_NUMPROC_FLAG} ${MPIEXEC_MAX_NUMPROCS} ${MPIEXEC_PREFLAGS} 
              $<TARGET_FILE:${framework}_${test_name}> ${MPIEXEC_POSTFLAGS} ${_test_args}
          )
        else()
          add_test(NAME ${full_test_name}
            COMMAND $<TARGET_FILE:${framework}_${test_name}> ${_test_args}
          )
        endif()
      endif()
    else()  
      mercury_test_wrapper(${_distributed} ${test_name} "${name_prefix}" "${name_suffix};${comm}" 
          "$<TARGET_FILE:${framework}_server>" "${test_args}" 
          "$<TARGET_FILE:${framework}_${test_name}>" "${test_args}"
      )
    endif()
  endforeach()  
endfunction()

#-----------------------------------------------------------------------------
# Add a test for all supported comm and protocol variants
# an additional argument/parameter list may be supplied (use "" for no args)
#-----------------------------------------------------------------------------
function(add_mercury_test test_name name_suffix args)
 add_framework_test(${test_name} "mercury" "${name_suffix}" "hg_test" "${args}")
endfunction()

#-----------------------------------------------------------------------------
# Add a test for all supported comm and protocol variants
# an additional argument/parameter list may be supplied (use "" for no args)
#-----------------------------------------------------------------------------
function(add_na_test test_name name_suffix args)
 add_framework_test(${test_name} "na" "${name_suffix}" "na_test" "${args}")
endfunction()

