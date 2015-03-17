#include <iostream>
#include <fstream>
#include <string>
#include <queue>
#include <tuple>
#include <utility>
#include <functional>
//
extern "C" {
#include "na.h"
#include "mercury_types.h"
#include "mercury_core.h"
#include "mercury_macros.h"
#include "mercury_proc_string.h"
#include "mercury.h"
#include "mercury_request.h"
#include "na_verbs.h"
}
//
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <netinet/in.h>
#include <arpa/inet.h>

using namespace std;

//---------------------------------------------------------------------------
// The function we want to call remotely
//---------------------------------------------------------------------------
std::tuple<double,double> hello(int a, float c) {
  std::cout << "Hello, int " << a << " float " << c << std::endl;
  std::tuple<double,double> result;
  std::get<0>(result) = 999.99;
  std::get<1>(result) = 777.77;
  return result;
}

int rpc_hello_2(float c, double *result1, double *result2) {
  std::cout << "Hello, " << " float " << c << std::endl;
  *result1 = 999.99;
  *result2 = 777.77;
  return 0;
}

std::vector<double> rpc_hello_3(int c) {
  std::cout << "Hello, " << c << std::endl;
  std::vector<double> test;
  test.push_back(999.99);
  test.push_back(777.777);
  return test;
}

//---------------------------------------------------------------------------
// Code snippets used from
// http://stackoverflow.com/questions/687490/how-do-i-expand-a-tuple-into-variadic-template-functions-arguments/1547118#1547118
//---------------------------------------------------------------------------

// tag type for indexes
template<int...> struct index_tuple {};

// tag type for one type from list of types
template<int I, typename IndexTuple, typename ... Types> struct make_indexes_impl;

// Get the type of the next type in the list
template<int I, int ... Indexes, typename T, typename ... Types>
struct make_indexes_impl<I, index_tuple<Indexes...>, T, Types...> {
  typedef typename make_indexes_impl<I + 1, index_tuple<Indexes..., I>, Types...>::type type;
};

// Create a tuple type of indices one shorter than the original
template<int I, int ... Indexes>
struct make_indexes_impl<I, index_tuple<Indexes...> > {
  typedef index_tuple<Indexes...> type;
};

// Start with empty tuple and list of types
template<typename ... Types>
struct make_indexes: make_indexes_impl<0, index_tuple<>, Types...> {};

//---------------------------------------------------------------------------
// Unpack tuple and apply to function
//---------------------------------------------------------------------------
// for each index of the tuple, get the element and put it into the function call
template<class Ret, class ... Args, int ... Indexes>
Ret apply_helper(Ret (*pf)(Args...), index_tuple<Indexes...>, std::tuple<Args...> && tup) {
  return pf(std::forward<Args>( std::get<Indexes>(tup))...);
}

// apply tuple to function using a reference to a tuple
template<class Ret, class ... Args>
Ret apply(Ret (*pf)(Args...), const std::tuple<Args...>& tup) {
  return apply_helper(pf, typename make_indexes<Args...>::type(), std::tuple<Args...>(tup));
}

// apply tuple to function using an rvalue tuple reference
template<class Ret, class ... Args>
Ret apply(Ret (*pf)(Args...), std::tuple<Args...> && tup) {
  return apply_helper(pf, typename make_indexes<Args...>::type(), std::forward<std::tuple<Args...>>(tup));
}

//---------------------------------------------------------------------------
// Template for function wrapper
//---------------------------------------------------------------------------
//---------------------------------------------------------------------------
// Generate mercury callback functions for an RPC function
//---------------------------------------------------------------------------

// wrapper which will take a function signature and function pointer
template<typename T, T* Funcptr> struct rpc_wrapper;

// specialization using variadics so we can capture any function type
template<typename Ret, typename ...Args, Ret (*Funcptr)(Args...)>
struct rpc_wrapper<Ret(Args...), Funcptr>
{
  // The type of the function we are wrapping
  typedef Ret (*functionPointer)(Args...);
  // empty constructor
  rpc_wrapper() {};
  // Function result type
  typedef Ret result_type;
  // Function input type (arguments) in form of tuple of arguments
  typedef std::tuple<Args...> input_type;
  // utility num arguments
  static const size_t nargs = sizeof...(Args);
  // utility, nth argument
  template <size_t i>
  struct arg {
    typedef typename std::tuple_element<i, std::tuple<Args...>>::type type;
  };

  static void show() { std::cout << "argcount is " << nargs << std::endl; }

  static hg_return_t cb1(hg_handle_t handle) {
    hg_return_t ret = HG_SUCCESS;
    input_type  in_struct;
    result_type out_struct;

    // Get input buffer
    ret = HG_Get_input(handle, &in_struct);
    if (ret != HG_SUCCESS) {
      fprintf(stderr, "Could not get input\n");
      return ret;
    }

    na_addr_t remote_address = HG_Get_addr(handle);
    struct na_verbs_addr *verbs_addr = (struct na_verbs_addr*)remote_address;
    connections.insert(verbs_addr);
    std::cout << "Inside templated callback " << std::endl;

    // Forward arguments to the actual function
    out_struct = apply<Ret,Args...>(Funcptr, in_struct);

    // Send response back
    ret = HG_Respond(handle, NULL, NULL, &out_struct);
    if (ret != HG_SUCCESS) {
      fprintf(stderr, "Could not respond\n");
      return ret;
    }

    HG_Free_input(handle, &in_struct);
    HG_Destroy(handle);
    return ret;
  }

  static hg_return_t cb2(const struct hg_cb_info *callback_info) {
    hg_return_t       ret = HG_SUCCESS;
    hg_handle_t    handle = callback_info->handle;
    hg_request_t *request = (hg_request_t *) callback_info->arg;
    result_type  out_struct;

    std::cout << "Inside rpc_hello_cb " << std::endl;

    // Get output
    ret = HG_Get_output(handle, &out_struct);
    if (ret != HG_SUCCESS) {
      fprintf(stderr, "Could not get output\n");
      goto done;
    }

    std::cout << std::get<0>(out_struct) << " " << std::get<1>(out_struct) << std::endl;
    // Get output parameters
//    printf("rpc_hello returned: %d %f %f \n",
//        std::get<0>(out_struct),
///        std::get<1>(out_struct),
//        std::get<2>(out_struct)  );

    // Free request
    ret = HG_Free_output(handle, &out_struct);
    if (ret != HG_SUCCESS) {
      fprintf(stderr, "Could not free output\n");
      goto done;
    }

    hg_request_complete(request);
    done: return ret;
  }

  static hg_return_t hg_proc_input(hg_proc_t proc, void *data) {
    return hg_proc_memcpy(proc, data, sizeof(input_type));
  }
  static hg_return_t hg_proc_output(hg_proc_t proc, void *data) {
    return hg_proc_memcpy(proc, data, sizeof(result_type));
  }
};

#define MERCURY_DECLARE_CXX(func_name) \
 typedef rpc_wrapper<decltype(func_name), func_name> BOOST_PP_CAT(wrapped_, func_name); \
 BOOST_PP_CAT(wrapped_, func_name) BOOST_PP_CAT(rpc_wrapped_, func_name)();

#define MERCURY_REGISTER_CXX(hg_class, func_name, id) \
  id = HG_Register(hg_class, #func_name, \
      rpc_wrapper<decltype(func_name), func_name>::hg_proc_input, \
      rpc_wrapper<decltype(func_name), func_name>::hg_proc_output, \
      rpc_wrapper<decltype(func_name), func_name>::cb1);

//---------------------------------------------------------------------------
// RPC hello arg types
//---------------------------------------------------------------------------
/*
#define MERCURY_REGISTER_CXX(hg_class, func_name, \
    in_tuple_type_name, out_tuple_type_name, rpc_cb) \
    HG_Register(hg_class, func_name, \
    BOOST_PP_CAT(hg_proc_,in_tuple_type_name),\
    BOOST_PP_CAT(hg_proc_,out_tuple_type_name), rpc_cb)
*/
/*
typedef std::tuple<int, float>          hello_argType;
typedef std::tuple<int, double, double> hello_retType;

#define HG_GEN_TUPLE_PROC(tuple_type_name) \
static HG_INLINE hg_return_t \
    BOOST_PP_CAT(hg_proc_, tuple_type_name) \
    (hg_proc_t proc, void *data) \
{   \
  return hg_proc_memcpy(proc, data, sizeof(tuple_type_name)); \
}
HG_GEN_TUPLE_PROC(hello_argType)
HG_GEN_TUPLE_PROC(hello_retType)
*/

/*
//---------------------------------------------------------------------------
// RPC hello
//---------------------------------------------------------------------------
//---------------------------------------------------------------------------
*/
