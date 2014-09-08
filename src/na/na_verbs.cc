
extern "C" {

#include "na_verbs.h"
#include "na_private.h"
#include "na_error.h"

#include "mercury_hash_table.h"
#include "mercury_queue.h"
#include "mercury_thread.h"
#include "mercury_thread_mutex.h"
#include "mercury_time.h"
#include "mercury_atomic.h"

};

#include <stdlib.h>
#include <string.h>
#include <poll.h>

#include "MercuryController.h"

#if RDMAHELPER_LOG4CXX_LOGGING
  using namespace log4cxx;
  using namespace log4cxx::helpers;

  //static log4cxx::LoggerPtr log_logger_(log4cxx::Logger::getLogger( "jb." ));
  static int log4cxx_initialized = 0;

  //LOG_DECLARE_FILE("jb");
  #include "log4cxx/basicconfigurator.h"
  #include <log4cxx/fileappender.h>
  #include <log4cxx/simplelayout.h>
#endif

#include <ramdisk/include/services/common/RdmaClient.h>
#include <ramdisk/include/services/common/RdmaDevice.h>
#include <ramdisk/include/services/common/RdmaError.h>
#include <ramdisk/include/services/common/RdmaCompletionQueue.h>
#include <ramdisk/include/services/MessageUtility.h>
#include <ramdisk/include/services/ServicesConstants.h>

#include <boost/regex.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/throw_exception.hpp>
#include <boost/tokenizer.hpp>

#include <thread>
#include <mutex>
#include <chrono>
//
std::mutex verbs_completion_map_mutex;
using namespace bgcios;
using namespace bgcios::stdio;
using namespace std::placeholders;

/*
#undef LOG_CIOS_DEBUG_MSG
#undef LOG_DEBUG_MSG
#define LOG_CIOS_DEBUG_MSG(x) std::cout << x << std::endl;
#define LOG_DEBUG_MSG(x) std::cout << x << std::endl;
*/

#define START_END_DEBUG 1
#ifdef START_END_DEBUG
  #define FUNC_START_DEBUG_MSG LOG_DEBUG_MSG("**************** Enter " << __func__ << " ****************");
  #define FUNC_END_DEBUG_MSG   LOG_DEBUG_MSG("################ Exit  " << __func__ << " ################");
#else
  #define FUNC_START_DEBUG_MSG
  #define FUNC_END_DEBUG_MSG
#endif

static int counter = 0;
#include "cscs_messages.h"
static uint64_t          rdma_put_ID = 10000;
static uint64_t          rdma_get_ID = 20000;

/*
 * This holds the destination address for any given operation.
 * All send/receive operations take an abstract na_addr_t
 * as the destination address. Here we define verbs related ids
 * that we can use to identify where a message will go
 */
struct na_verbs_addr {
    na_verbs_addr() : client(static_cast<RdmaClient*>(NULL)), qp_id(0), self(NA_FALSE) {}
    // A server can identify a client by its queue pair ID
    // this is used in MercuryController to lookup the client object for send/recv ops
    // a value of 0 indicates this is not the server, so use the client object instead
    uint32_t      qp_id;
    // A client is only connected to one server so just hold onto this object
    RdmaClientPtr client;
    // self flag, true if this address is local, false if remote
    na_bool_t     self;

};

typedef std::map<uint64_t,na_verbs_op_id*> OperationMap;

struct na_verbs_private_data
{
    // flag for client/server
    na_bool_t server;
    // the server uses the controller
    MercuryControllerPtr     controller;
    // the client uses these
    RdmaClientPtr            client;
    RdmaProtectionDomainPtr  domain;
    RdmaCompletionChannelPtr completionChannel;
    RdmaCompletionQueuePtr   completionQ;
    //
    // store na_verbs_op_id objects using a map referenced by verbs work request ID
    OperationMap *WorkRequestCompletionMap;
    // store na_verbs_op_id objects using a map referenced by TAG
    OperationMap *ReceiveTagCompletionMap;
    // store na_verbs_op_id for unexpected receives
    std::deque<na_verbs_op_id*> *UnexpectedOps;
    // store received unexpeced messages that arrive before mercury has posted them
    std::deque<na_verbs_op_id*> *EarlyUnexpectedOps;
    //
    char *listen_addr; /* Server listen_addr */

    hg_thread_mutex_t test_unexpected_mutex; /* Mutex */
};

/* ************************************************* */
/* Static NA VERBS utility functions                 */
/* ************************************************* */
unsigned int na_verbs_get_port(na_class_t *na_class)
{
  na_verbs_private_data *pd = NA_VERBS_PRIVATE_DATA(na_class);
  int na_test_verbs_port_g = pd->controller->getPort();
  std::cout << na_test_verbs_port_g << std::endl;
  return na_test_verbs_port_g;
}

/* ************************************************* */
/* Static NA VERBS Class function Prototypes         */
/* ************************************************* */

static na_class_t*
na_verbs_initialize(const struct na_info *na_info, na_bool_t listen);

static na_bool_t
na_verbs_check_protocol(const char *protocol);

static na_return_t
na_verbs_finalize(na_class_t *in_na_class);

static na_return_t
na_verbs_addr_lookup(na_class_t   *in_na_class,
    na_context_t *in_context,
    na_cb_t       in_callback,
    void         *in_arg,
    const char   *in_name,
    na_op_id_t   *out_opid);

static na_return_t
na_verbs_addr_self(
        na_class_t *na_class,
        na_addr_t  *addr
        );

static na_return_t
na_verbs_addr_free(na_class_t   *in_na_class,
    na_addr_t     in_addr);

/* addr_is_self */
static na_bool_t
na_verbs_addr_is_self(
        na_class_t *na_class,
        na_addr_t   addr
        );

static na_return_t
na_verbs_addr_to_string(na_class_t   *in_na_class,
    char         *inout_buf,
    na_size_t     in_buf_size,
    na_addr_t     in_addr);

static na_size_t
na_verbs_msg_get_max_expected_size(na_class_t *in_na_class);

static na_size_t
na_verbs_msg_get_max_unexpected_size(na_class_t *in_na_class);

static na_tag_t
na_verbs_msg_get_maximum_tag(na_class_t  *in_na_class);

static na_return_t
na_verbs_msg_send_unexpected(na_class_t     *in_na_class,
    na_context_t *in_context,
    na_cb_t         in_callback,
    void           *in_arg,
    const void     *in_buf,
    na_size_t       in_buf_size,
    na_addr_t       in_destination,
    na_tag_t        in_tag,
    na_op_id_t     *out_opid);

static na_return_t
na_verbs_msg_recv_unexpected(na_class_t     *in_na_class,
    na_context_t *in_context,
    na_cb_t         in_callback,
    void           *in_user_context,
    void           *in_buf,
    na_size_t       in_buf_size,
    na_op_id_t     *out_opid);

static na_return_t
na_verbs_msg_send_expected(na_class_t  *in_na_class,
    na_context_t *in_context,
    na_cb_t      in_callback,
    void        *in_user_context,
    const void  *in_buf,
    na_size_t    in_buf_size,
    na_addr_t    in_dest,
    na_tag_t     in_tag,
    na_op_id_t  *out_id);

static na_return_t
na_verbs_msg_recv_expected(na_class_t     *in_na_class,
    na_context_t *in_context,
    na_cb_t         in_callback,
    void           *in_arg,
    void           *in_buf,
    na_size_t       in_buf_size,
    na_addr_t       in_source,
    na_tag_t        in_tag,
    na_op_id_t     *out_id);

static na_return_t
na_verbs_mem_handle_create(na_class_t       *in_na_class,
    void             *in_buf,
    na_size_t         in_buf_size,
    unsigned long     in_flags,
    na_mem_handle_t  *out_mem_handle);

static na_return_t
na_verbs_mem_handle_free(na_class_t       *in_na_class,
    na_mem_handle_t   in_mem_handle);

static na_return_t
na_verbs_mem_register(na_class_t        *in_na_class,
    na_mem_handle_t    in_mem_handle);

static na_return_t
na_verbs_mem_deregister(na_class_t      *in_na_class,
    na_mem_handle_t  in_mem_handle);

static na_size_t
na_verbs_mem_handle_get_serialize_size(na_class_t     *in_na_class,
    na_mem_handle_t in_mem_handle);

static na_return_t
na_verbs_mem_handle_serialize(na_class_t        *in_na_class,
    void              *in_buf,
    na_size_t          in_buf_size,
    na_mem_handle_t    in_mem_handle);

static na_return_t
na_verbs_mem_handle_deserialize(na_class_t      *in_na_class,
    na_mem_handle_t *in_mem_handle,
    const void      *in_buf,
    na_size_t        in_buf_size);

static na_return_t
na_verbs_put(na_class_t         *in_na_class,
    na_context_t *in_context,
    na_cb_t             in_callback,
    void               *in_arg,
    na_mem_handle_t     in_local_mem_handle,
    na_offset_t         in_local_offset,
    na_mem_handle_t     in_remote_mem_handle,
    na_offset_t         in_remote_offset,
    na_size_t           in_data_size,
    na_addr_t           in_remote_addr,
    na_op_id_t         *out_opid);

static na_return_t
na_verbs_get(na_class_t         *in_na_class,
    na_context_t *in_context,
    na_cb_t             in_callback,
    void               *in_arg,
    na_mem_handle_t     in_local_mem_handle,
    na_offset_t         in_local_offset,
    na_mem_handle_t     in_remote_mem_handle,
    na_offset_t         in_remote_offset,
    na_size_t           in_data_size,
    na_addr_t           in_remote_addr,
    na_op_id_t         *out_opid);

static na_return_t
na_verbs_progress(na_class_t     *in_na_class,
    na_context_t *in_context,
    unsigned int    in_timeout);

static na_return_t
na_verbs_cancel(na_class_t    *in_na_class,
    na_context_t *in_context,
    na_op_id_t     in_opid);


/************************************/
/* completion handler */
/************************************/
static na_return_t
na_verbs_complete(struct na_verbs_op_id *na_verbs_op_id);

/*******************/
/* Local Variables */
/*******************/
static const na_class_t na_verbs_class_g = {
    NULL,                                   /* private_data */
    na_verbs_finalize,                      /* finalize */
    NULL,                                   /* context_create */
    NULL,                                   /* context_destroy */
    na_verbs_addr_lookup,                   /* addr_lookup */
    na_verbs_addr_free,                     /* addr_free */
    na_verbs_addr_self,                     /* addr_self */
    NULL,                                   /* addr_dup */
    na_verbs_addr_is_self,                  /* addr_is_self */
    na_verbs_addr_to_string,                /* addr_to_string */
    na_verbs_msg_get_max_expected_size,     /* msg_get_max_expected_size */
    na_verbs_msg_get_max_unexpected_size,   /* msg_get_max_expected_size */
    na_verbs_msg_get_maximum_tag,           /* msg_get_maximum_tag */
    na_verbs_msg_send_unexpected,           /* msg_send_unexpected */
    na_verbs_msg_recv_unexpected,           /* msg_recv_unexpected */
    na_verbs_msg_send_expected,             /* msg_send_expected */
    na_verbs_msg_recv_expected,             /* msg_recv_expected */
    na_verbs_mem_handle_create,             /* mem_handle_create */
    NULL,                                   /* mem_handle_create_segment - This should be supported, but isn't yet implemented here*/
    na_verbs_mem_handle_free,               /* mem_handle_free */
    na_verbs_mem_register,                  /* mem_register */
    na_verbs_mem_deregister,                /* mem_deregister */
    na_verbs_mem_handle_get_serialize_size, /* mem_handle_get_serialize_size */
    na_verbs_mem_handle_serialize,          /* mem_handle_serialize */
    na_verbs_mem_handle_deserialize,        /* mem_handle_deserialize */
    na_verbs_put,                           /* put */
    na_verbs_get,                           /* get */
    na_verbs_progress,                      /* progress */
    na_verbs_cancel                         /* cancel */
};

extern "C" {
  static const char na_verbs_name_g[] = "verbs";

  struct na_class_info na_verbs_info_g  = {
      na_verbs_name_g,
      na_verbs_check_protocol,
      na_verbs_initialize
  };
};

/*---------------------------------------------------------------------------*/
#if RDMAHELPER_LOG4CXX_LOGGING
void init_log4cxx()
{
  if (log4cxx_initialized) return;

  // Set up a simple configuration that logs on the console.
  BasicConfigurator::configure();
  LOG4CXX_INFO(log_logger_, "Entering application.");
  bgcios::setLoggingLevel("jb.", 'D');
  log4cxx_initialized = true;
}
#else
void init_log4cxx() {}
#endif
/*---------------------------------------------------------------------------*/
na_return_t poll_cq(na_verbs_private_data *pd, RdmaCompletionChannelPtr channel);
na_return_t poll_cq_non_blocking(na_verbs_private_data *pd, RdmaCompletionChannelPtr channel);
/*---------------------------------------------------------------------------*/
int handle_verbs_completion(struct ibv_wc *completion, na_verbs_private_data *pd, RdmaClientPtr client);
/*---------------------------------------------------------------------------*/
na_return_t on_completion_wr(na_verbs_private_data *pd, uint64_t wr_id);
na_return_t on_completion_tag(na_verbs_private_data *pd, uint64_t wr_id);
/*---------------------------------------------------------------------------*/

/********************/
/* Plugin callbacks */
/********************/

/*---------------------------------------------------------------------------*/

static void
na_verbs_release(struct na_cb_info *callback_info, void *arg)
{
  FUNC_START_DEBUG_MSG
  struct na_verbs_op_id *na_verbs_op_id = (struct na_verbs_op_id *) arg;
  if (na_verbs_op_id && !na_verbs_op_id->completed) {
    NA_LOG_ERROR("Releasing resources from an uncompleted operation");
  }
  LOG_DEBUG_MSG("Freeing callback_info " << callback_info);
  free(callback_info);
  LOG_DEBUG_MSG("Freeing na_verbs_op_id");
  free(na_verbs_op_id);
  FUNC_END_DEBUG_MSG
}

/*---------------------------------------------------------------------------*/
static na_bool_t
na_verbs_check_protocol(const char *protocol)
{
  FUNC_START_DEBUG_MSG
  na_bool_t accept = NA_FALSE;
  if (strstr(protocol, "rdma@") != 0) {
    accept = NA_TRUE;
  }
  FUNC_END_DEBUG_MSG
  return accept;
}

/*---------------------------------------------------------------------------*/
static na_class_t *
na_verbs_initialize(const struct na_info *na_info, na_bool_t listen)
{
  init_log4cxx();
  FUNC_START_DEBUG_MSG
  na_class_t *na_class = NULL;
  na_bool_t error_occurred = NA_FALSE;
  na_verbs_private_data *pd = NULL;

  na_class = (na_class_t *) malloc(sizeof(na_class_t));
  if (!na_class) {
    NA_LOG_ERROR("Could not allocate NA class");
    error_occurred = NA_TRUE;
    goto done;
  }
  // copy contents into class var
  *na_class = na_verbs_class_g;
  //
  na_class->private_data = malloc(sizeof(struct na_verbs_private_data));
  if (!na_class->private_data) {
    NA_LOG_ERROR("Could not allocate NA private data class");
    error_occurred = NA_TRUE;
    goto done;
  }
  memset(na_class->private_data,0,sizeof(struct na_verbs_private_data));
  pd = NA_VERBS_PRIVATE_DATA(na_class);
  pd->WorkRequestCompletionMap = new OperationMap();
  pd->ReceiveTagCompletionMap  = new OperationMap();
  pd->UnexpectedOps            = new std::deque<na_verbs_op_id*>();
  pd->EarlyUnexpectedOps       = new std::deque<na_verbs_op_id*>();
  pd->server = listen;

  // setup all the internal objects
  if (pd->server) {
    //
    // the na_info contains the device/interface we need to use in string form in the protocol string
    //
    static const boost::regex addr_port_re( ".*@(.*)/(.*)" );
    boost::smatch matches;
    if ( ! boost::regex_match( std::string(na_info->protocol_name), matches, addr_port_re ) ) {
      LOG_ERROR_MSG( "device/interface '" <<  na_info->protocol_name << "' is not valid" );
    }
    std::string server_dev   = matches[1];
    std::string server_iface = matches[2];
    //
    int port = na_info->port;
    port = 0;
    //
    pd->controller = MercuryControllerPtr(
      new MercuryController(server_dev.c_str(), server_iface.c_str(), 0)
    );
    if (!pd->controller) {
      NA_LOG_ERROR("VERBS_initialize() failed");
      error_occurred = NA_TRUE;
      goto done;
    }

    //
    auto completion_function = std::bind( &handle_verbs_completion, _1, pd, _2 );
    pd->controller->setCompletionFunction(completion_function);

    // call function to start listening in server mode
    std::cout << "Controller listening for connections " << std::endl;
    pd->controller->startup();

  }
  else {
    // on the client, we don't do anything for now
    LOG_INFO_MSG("Client init - no action for now");
  }

  done:
  if (error_occurred) {
    // TODO clean stuff
  }
  FUNC_END_DEBUG_MSG
  return na_class;
}

/*---------------------------------------------------------------------------*/
static na_return_t
na_verbs_finalize(na_class_t *na_class)
{
  FUNC_START_DEBUG_MSG
  na_return_t                       ret = NA_SUCCESS;
  na_verbs_private_data             *pd = NA_VERBS_PRIVATE_DATA(na_class);
  // we must be careful, the registered memory region must not go out of scope
  // until the send completes, so we must store the object outside of this function

  // release all the smart pointers that are holding our objects
  if (pd->server) {
    // if the connection has not yet been closed

    if (pd->controller) {
      // don't exit until all messages have completed
      while (!pd->ReceiveTagCompletionMap->empty() || !pd->WorkRequestCompletionMap->empty()) {
        LOG_DEBUG_MSG("Server Polling before disconnect RC " << pd->ReceiveTagCompletionMap->size() << " WR " << pd->WorkRequestCompletionMap->size());
        pd->controller->eventMonitor(0);
      }

//      na_return_t val = NA_SUCCESS;
//      while (val==NA_SUCCESS && !pd->controller->isTerminated()) {
//       LOG_DEBUG_MSG("Poll eventmonitor");
//      }
      LOG_DEBUG_MSG("Finalizing controller");
      pd->controller.reset();
    }
  }
  else{
    if (pd->client) {
      // don't exit until all messages have completed
      while (!pd->ReceiveTagCompletionMap->empty() || !pd->WorkRequestCompletionMap->empty()) {
        LOG_DEBUG_MSG("Client Polling before disconnect RC " << pd->ReceiveTagCompletionMap->size() << " WR " << pd->WorkRequestCompletionMap->size());
        poll_cq_non_blocking(pd, pd->completionChannel);
      }
      pd->client->disconnect(true);
      na_return_t val = NA_SUCCESS;
      pd->controller.reset();
      pd->client.reset();
    }
    if (pd->domain) {
      pd->domain.reset();
    }
    if (pd->completionQ) {
      pd->completionQ.reset();
    }
    if (pd->completionChannel) {
      pd->completionChannel.reset();
    }
  }

  // releae other structures
  delete pd->WorkRequestCompletionMap;
  delete pd->ReceiveTagCompletionMap;
  delete pd->UnexpectedOps;
  delete pd->EarlyUnexpectedOps;
  free(na_class->private_data);
  free(na_class);

  FUNC_END_DEBUG_MSG
  return ret;
}

/*---------------------------------------------------------------------------*/
void NA_VERBS_Get_rdma_device_address(const char *devicename, const char *iface, char *hostname)
{
  init_log4cxx();
  FUNC_START_DEBUG_MSG
  // Find the address of the I/O link device.
  RdmaDevicePtr linkDevice;
  try {
    linkDevice = RdmaDevicePtr(new RdmaDevice(devicename, iface));
  }
  catch (bgcios::RdmaError& e) {
    std::cout << "Problem initializing device " << std::endl;
    LOG_ERROR_MSG("error opening InfiniBand device: " << e.what());
  }
  LOG_DEBUG_MSG("Created InfiniBand device for " << linkDevice->getDeviceName() << " using interface " << linkDevice->getInterfaceName());

  std::stringstream temp;
  in_addr_t addr = linkDevice->getAddress();
  temp
  << (int)((uint8_t*)&addr)[0] << "."
  << (int)((uint8_t*)&addr)[1] << "."
  << (int)((uint8_t*)&addr)[2] << "."
  << (int)((uint8_t*)&addr)[3] << std::ends;
  strcpy(hostname, temp.str().c_str());
  //
  LOG_DEBUG_MSG("Generated hostname string " << hostname);

  // print device info for debugging
  //linkDevice->getDeviceInfo(true);

  FUNC_END_DEBUG_MSG
}

/*---------------------------------------------------------------------------*/
// Make a connection to the server and store the information for use later
/*---------------------------------------------------------------------------*/

static na_return_t
na_verbs_addr_lookup(na_class_t NA_UNUSED *na_class, na_context_t *context,
    na_cb_t callback, void *arg, const char *name, na_op_id_t *op_id)
{
  init_log4cxx();
  FUNC_START_DEBUG_MSG
  struct na_verbs_op_id *na_verbs_op_id = NULL;
  struct na_verbs_addr *na_verbs_addr = NULL;
  na_return_t ret = NA_SUCCESS;

  char hostname[512];
  int port_number = 0;

  LOG_DEBUG_MSG("received an address lookup for : " << name);

  //
  // to connect to server we get the address from the string passed
  //
  static const boost::regex addr_port_re( "rdma@(.*)/(.*)://(.*):([0-9]*).*$" );
  boost::smatch matches;
  std::string search = name;
  if ( ! boost::regex_match( search, matches, addr_port_re ) ) {
    LOG_ERROR_MSG( "host:device:port '" <<  name << "' is not valid" );
  }
  std::string server_dev   = matches[1];
  std::string server_iface = matches[2];
  std::string server_addr  = matches[3];
  std::string server_port  = matches[4];
  //
  LOG_DEBUG_MSG("(client) Server address " << server_addr.c_str() << ":" <<  server_port.c_str() << " device "
      << server_dev.c_str() << " / interface " << server_iface.c_str() << ".");

  // Find the address of our local link device.
  NA_VERBS_Get_rdma_device_address(server_dev.c_str(), server_iface.c_str(), hostname);

  na_verbs_private_data *pd = NA_VERBS_PRIVATE_DATA(na_class);
  // Create an RDMA client object
  try {
    std::stringstream port;
    port << port_number << std::ends;
    pd->client = RdmaClientPtr(
        new bgcios::RdmaClient(hostname, server_port/*port.str().c_str()*/, server_addr, server_port)
    );
    LOG_DEBUG_MSG("(client) RdmaClient object created");
  }
  catch (bgcios::RdmaError& e) {
    LOG_ERROR_MSG("(client) error creating RDMA client object: " << e.what());
    return NA_PROTOCOL_ERROR;
  }
  LOG_DEBUG_MSG("(client) calling connect ");

  try {
    pd->domain = RdmaProtectionDomainPtr(
        new RdmaProtectionDomain(pd->client->getContext()));
  }
  catch (bgcios::RdmaError& e) {
    LOG_ERROR_MSG("error constructing protection domain: " << e.what());
    return NA_PROTOCOL_ERROR;
  }
  LOG_DEBUG_MSG("(client) created completion protection domain");
  try {
    pd->completionChannel = RdmaCompletionChannelPtr(
        new RdmaCompletionChannel(pd->client->getContext(), false));
  }
  catch (bgcios::RdmaError& e) {
    LOG_ERROR_MSG("error constructing completion channel: " << e.what());
    return NA_PROTOCOL_ERROR;
  }
  LOG_DEBUG_MSG("(client) created completion channel using fd " << pd->completionChannel->getChannelFd());

  pd->completionQ = RdmaCompletionQueuePtr(
      new RdmaCompletionQueue(
          pd->client->getContext(),
          RdmaCompletionQueue::MaxQueueSize,
          pd->completionChannel->getChannel()));

  // Create a memory pool for pinned buffers
  RdmaRegisteredMemoryPoolPtr _memoryPool = std::make_shared<RdmaRegisteredMemoryPool>(pd->domain, 32, 32);
  pd->client->setMemoryPoold(_memoryPool);


  // make a connection
  LOG_DEBUG_MSG("(client) calling makepeer ");
  pd->client->makePeer(pd->domain, pd->completionQ);

  LOG_DEBUG_MSG("(client) finished connect ");

  // allocate the address information for storing details
  // we will use with future traffic to this destination
  na_verbs_addr = new struct na_verbs_addr();
  memset(na_verbs_addr,0,sizeof(na_verbs_addr));
  if (!na_verbs_addr) {
    NA_LOG_ERROR("Could not allocate verbs addr");
    ret = NA_NOMEM_ERROR;
    goto done;
  }

  // Allocate op_id
  // Our connection has completed at this point, so we can call na_complete
  na_verbs_op_id = (struct na_verbs_op_id *) malloc(sizeof(struct na_verbs_op_id));
  if (!na_verbs_op_id) {
    NA_LOG_ERROR("Could not allocate NA verbs operation ID");
    ret = NA_NOMEM_ERROR;
    goto done;
  }
  na_verbs_op_id->context   = context;
  na_verbs_op_id->type      = NA_CB_LOOKUP;
  na_verbs_op_id->callback  = callback;
  na_verbs_op_id->arg       = arg;
  na_verbs_op_id->completed = NA_TRUE;

  //
  // Only the client ever connects to the server, so store the na_addr
  // details here. qp_id is zero as we are a client not the server
  //
  LOG_DEBUG_MSG("(client) filling na_addr ");
  na_verbs_addr->client = pd->client;
  // and store this info in the op_id as well
  na_verbs_op_id->verbs_addr = na_verbs_addr;

  FUNC_END_DEBUG_MSG

  // the connection has been completed, so call completion
  return na_verbs_complete(na_verbs_op_id);

  done:
  FUNC_END_DEBUG_MSG
  return ret;
}

/*---------------------------------------------------------------------------*/
static na_return_t
na_verbs_addr_free(na_class_t NA_UNUSED *na_class, na_addr_t addr)
{
  FUNC_START_DEBUG_MSG
  struct na_verbs_addr *na_verbs_addr = (struct na_verbs_addr *) addr;

  if (na_verbs_addr) {
    if (na_verbs_addr->client) {
      // remove the client smart pointer reference and trigger the destructor
      na_verbs_addr->client.reset(static_cast<RdmaClient*>(NULL));
    }
    delete na_verbs_addr;
  }
  //
  na_return_t ret = NA_SUCCESS;
  FUNC_END_DEBUG_MSG
  return ret;
  }

/*---------------------------------------------------------------------------*/
static na_return_t na_verbs_addr_self(na_class_t NA_UNUSED *na_class,
    na_addr_t *addr) {
  struct na_verbs_addr *na_verbs_addr = NULL;
  na_return_t ret = NA_SUCCESS;
  FUNC_START_DEBUG_MSG

  /* Allocate addr */
  na_verbs_addr = new struct na_verbs_addr();
  if (!na_verbs_addr) {
    NA_LOG_ERROR("Could not allocate verbs addr");
    ret = NA_NOMEM_ERROR;
    goto done;
  }

  na_verbs_addr->self  = NA_TRUE;
  *addr = (na_addr_t) na_verbs_addr;

done:
  if (ret != NA_SUCCESS) {
    delete na_verbs_addr;
  }
  FUNC_END_DEBUG_MSG
  return ret;
}

/*---------------------------------------------------------------------------*/
// No idea what this fuction is for yet
//
static na_bool_t
na_verbs_addr_is_self(na_class_t NA_UNUSED *na_class, na_addr_t addr)
{
    struct na_verbs_addr *na_verbs_addr = (struct na_verbs_addr *) addr;
    FUNC_START_DEBUG_MSG
    FUNC_END_DEBUG_MSG
    return na_verbs_addr->self;
}

/*---------------------------------------------------------------------------*/
static na_return_t
na_verbs_addr_to_string(na_class_t NA_UNUSED *na_class, char *buf,
    na_size_t buf_size, na_addr_t addr)
{
  FUNC_START_DEBUG_MSG
  struct na_verbs_addr *na_verbs_addr = NULL;
  const char *verbs_rev_addr;
  na_return_t ret = NA_SUCCESS;

  na_verbs_addr = (struct na_verbs_addr *) addr;
  LOG_DEBUG_MSG("Address translates to qp " << na_verbs_addr->qp_id << " and client ptr " << na_verbs_addr->client);

  FUNC_END_DEBUG_MSG
  return ret;
}

/*---------------------------------------------------------------------------*/
static na_size_t
na_verbs_msg_get_max_expected_size(na_class_t NA_UNUSED *na_class)
{
  FUNC_START_DEBUG_MSG
  na_size_t max_expected_size = NA_VERBS_EXPECTED_SIZE;

  FUNC_END_DEBUG_MSG
  return max_expected_size;
}

/*---------------------------------------------------------------------------*/
static na_size_t
na_verbs_msg_get_max_unexpected_size(na_class_t NA_UNUSED *na_class)
{
  FUNC_START_DEBUG_MSG
  na_size_t max_unexpected_size = NA_VERBS_UNEXPECTED_SIZE;

  FUNC_END_DEBUG_MSG
  return max_unexpected_size;
}

/*---------------------------------------------------------------------------*/
static na_tag_t
na_verbs_msg_get_maximum_tag(na_class_t NA_UNUSED *na_class)
{
  FUNC_START_DEBUG_MSG
  na_tag_t max_tag = NA_VERBS_MAX_TAG;

  FUNC_END_DEBUG_MSG
  return max_tag;
}

/*---------------------------------------------------------------------------*/
template<typename T>
struct NullDeleter {
  void operator()(T*) {}
};

/*---------------------------------------------------------------------------*/
static na_return_t na_verbs_msg_send(
    na_class_t    *na_class,
    na_context_t  *context,
    na_cb_t        callback,
    void          *arg,
    const void    *buf,
    na_size_t      buf_size,
    na_addr_t      destination,
    na_tag_t       tag,
    na_op_id_t    *out_opid,
    na_cb_type     cb_type,
    uint8_t        expected_flag)
{
  FUNC_START_DEBUG_MSG
  uint64_t               *verbs_context = (uint64_t *) context->plugin_context;
  na_size_t              verbs_buf_size = buf_size;
  struct na_verbs_addr   *na_verbs_addr = (struct na_verbs_addr*) destination;
  struct na_verbs_op_id *na_verbs_op_id = NULL;
  na_return_t                       ret = NA_SUCCESS;
  na_verbs_private_data             *pd = NA_VERBS_PRIVATE_DATA(na_class);
  // we must be careful, the registered memory region must not go out of scope
  // until the send completes, so we must store the object outside of this function
  CSCS_user_message::UserRDMA_message *msg;
  RdmaClientPtr                        client;
  RdmaMemoryRegion                    *region;

  // Allocate op_id
  na_verbs_op_id = (struct na_verbs_op_id *) malloc(sizeof(struct na_verbs_op_id));
  if (!na_verbs_op_id) {
    NA_LOG_ERROR("Could not allocate NA VERBS operation ID");
    ret = NA_NOMEM_ERROR;
    goto done;
  }
  na_verbs_op_id->context                    = context;
  na_verbs_op_id->type                       = cb_type;
  na_verbs_op_id->callback                   = callback;
  na_verbs_op_id->arg                        = arg;
  na_verbs_op_id->completed                  = NA_FALSE;
  na_verbs_op_id->info.send.wr_id            = 0;
  na_verbs_op_id->info.send.rdmaMemRegionPtr = 0;

  // In future versions we will ....
  // expected or unexpected, wrap unexpected messages in a standard bgcios type message header,
  // expected ones have matching receives, so we can do RDMA from buf to buf
  if (expected_flag==CSCS_user_message::UnexpectedMessage) {
    // TBD
  }
  // not using these, but will when we switch to a direct buffer->buffer transfer
  if (pd->server) {
    if (!na_verbs_addr) throw std::runtime_error("Destination of send was not valid");
    client = pd->controller->getClient(na_verbs_addr->qp_id);
    region = client->getFreeRegion();
    //region = RdmaMemoryRegionPtr(new RdmaMemoryRegion(pd->controller->getProtectionDomain(), buf, buf_size));
  }
  else{
    client = pd->client;
    region = client->getFreeRegion();
    //region = RdmaMemoryRegionPtr(new RdmaMemoryRegion(pd->domain, buf, buf_size));
  }

  //
  // use a standard bgcios message structure, copying our buffer into it
  //
  msg = (CSCS_user_message::UserRDMA_message *)region->getAddress();
  initHeader(&msg->header);
  msg->header.service    = bgcios::SysioUserService;
  msg->header.length     = bgcios::ImmediateMessageSize;  // Amount of data in message (including this header).
  msg->header.type       = expected_flag;                 // Content of message.
  msg->header.rank       = 0;                             // Rank message is associated with.
  msg->header2.tag       = tag;

  memcpy(msg->MessageData, buf, buf_size);
  // it is possible for the request to complete while this thread is still executing the
  // next few instructions, to prevent the completion queue being accessed whilst we are
  // still adding the wr_id to it, we lock just before we issue the request and release
  // after we have added the wr_id to the map
  {
    na_verbs_op_id->wr_id = (uint64_t)region;
    std::shared_ptr<RdmaMemoryRegion> temp(region, NullDeleter<RdmaMemoryRegion>() );
    client->postSend(temp, true, false, 0); // wr_id is region address
    na_verbs_op_id->info.send.wr_id = na_verbs_op_id->wr_id;
    LOG_DEBUG_MSG("SEND has TAG value " << tag);

/*
    // these will always have matching receives, so we can simply send a buffer directly
    region = RdmaMemoryRegionPtr(new RdmaMemoryRegion(pd->controller->getProtectionDomain(), buf, buf_size));
    if (na_verbs_addr->qp_id==0) {
      LOG_ERROR_MSG("Serious error, qp is zero - cannot send to destination without address")
    }
    else {
      LOG_DEBUG_MSG("SUCCESS, qp is valid, sending to remote client from server : qp = " << na_verbs_addr->qp_id);
    }
    na_verbs_op_id->wr_id = dest->postSend(region, true); // signaled = true
*/

//  na_verbs_op_id->info.send.rdmaMemRegionPtr = new RdmaMemoryRegionPtr();
//  *(RdmaMemoryRegionPtr*)(na_verbs_op_id->info.send.rdmaMemRegionPtr) = region;

    //
    // add wr_id to our map for checking on completions later
    //
    (*pd->WorkRequestCompletionMap)[na_verbs_op_id->wr_id] = na_verbs_op_id;
    LOG_DEBUG_MSG("wr_id for send added to WR completion map " << na_verbs_op_id->wr_id << " Entries " <<  (*pd->WorkRequestCompletionMap).size());
  }
  // Assign op_id
  *out_opid = (na_op_id_t) na_verbs_op_id;

  done:
  if (ret != NA_SUCCESS) {
    free(na_verbs_op_id);
  };
  FUNC_END_DEBUG_MSG
  return ret;
}

/*---------------------------------------------------------------------------*/
static na_return_t
na_verbs_msg_send_unexpected(na_class_t *na_class,
    na_context_t  *context,
    na_cb_t        callback,
    void          *arg,
    const void    *buf,
    na_size_t      buf_size,
    na_addr_t      destination,
    na_tag_t       tag,
    na_op_id_t    *out_opid)
{
  na_return_t ret;
  FUNC_START_DEBUG_MSG
  ret = na_verbs_msg_send(
      na_class, context, callback, arg, buf, buf_size, destination, tag, out_opid,
      NA_CB_SEND_UNEXPECTED, CSCS_user_message::UnexpectedMessage);
  FUNC_END_DEBUG_MSG
  return ret;
}
/*---------------------------------------------------------------------------*/
static na_return_t
na_verbs_msg_send_expected(na_class_t *na_class,
    na_context_t  *context,
    na_cb_t        callback,
    void          *arg,
    const void    *buf,
    na_size_t      buf_size,
    na_addr_t      destination,
    na_tag_t       tag,
    na_op_id_t    *out_opid)
{
  na_return_t ret;
  FUNC_START_DEBUG_MSG
  ret = na_verbs_msg_send(
      na_class, context, callback, arg, buf, buf_size, destination, tag, out_opid,
      NA_CB_SEND_EXPECTED, CSCS_user_message::ExpectedMessage);
  FUNC_END_DEBUG_MSG
  return ret;
}

/*---------------------------------------------------------------------------*/
static na_return_t na_verbs_msg_recv(
  na_class_t    *na_class,
  na_context_t  *context,
  na_cb_t        callback,
  void          *arg,
  const void    *buf,
  na_size_t      buf_size,
  na_addr_t      source,
  na_tag_t       tag,
  na_op_id_t    *out_opid,
  na_cb_type     cb_type,
  uint8_t        expected_flag)
{
  FUNC_START_DEBUG_MSG
  uint64_t               *verbs_context = (uint64_t *) context->plugin_context;
  na_size_t              verbs_buf_size = buf_size;
  struct na_verbs_addr   *na_verbs_addr = (struct na_verbs_addr*) source;
  struct na_verbs_op_id *na_verbs_op_id = NULL;
  na_return_t                       ret = NA_SUCCESS;
  na_verbs_private_data             *pd = NA_VERBS_PRIVATE_DATA(na_class);
  // we must be careful, the registered memory region must not go out of scope
  // until the send completes, so we must store the message/object outside of this function
  CSCS_user_message::UserRDMA_message *msg;
  RdmaClientPtr                        client;

  struct verbs_expected_info *expected_info = NULL;

  // Allocate na_op_id
  na_verbs_op_id = (struct na_verbs_op_id *) malloc(sizeof(struct na_verbs_op_id));
  if (!na_verbs_op_id) {
    NA_LOG_ERROR("Could not allocate NA VERBS operation ID");
    ret = NA_NOMEM_ERROR;
    goto done;
  }
  na_verbs_op_id->context               = context;
  na_verbs_op_id->type                  = cb_type;
  na_verbs_op_id->callback              = callback;
  na_verbs_op_id->arg                   = arg;
  na_verbs_op_id->completed             = NA_FALSE;
  na_verbs_op_id->info.recv.buf_size    = buf_size;
  na_verbs_op_id->info.recv.buf         = (void*)buf;
  na_verbs_op_id->info.recv.tag         = tag;

  if (na_verbs_addr) {
    LOG_DEBUG_MSG("Receive expected=" << expected_flag << " has na_addr qp:" << na_verbs_addr->qp_id);
  }

  // In future versions we will ....
  // expected or unexpected, wrap unexpected messages in a standard bgcios type message header,
  // expected ones have matching receives, so we can do RDMA from buf to buf
  // not using these, but will when we switch to a direct buffer->buffer transfer
  if (pd->server) {
    if (na_verbs_addr) {
      client = pd->controller->getClient(na_verbs_addr->qp_id);
      if (client==NULL) {
        printf("dest is NULL but qp->id is %d\n\n",na_verbs_addr->qp_id);
      }
    }
    else {
      printf("Received a null address in receive\n");
    }
    //region = RdmaMemoryRegionPtr(new RdmaMemoryRegion(pd->controller->getProtectionDomain(), buf, buf_size));
  }
  else{
    client = pd->client;
    if (client==NULL) {
      printf("client is NULL but we should be client \n\n");
    }
    //region = RdmaMemoryRegionPtr(new RdmaMemoryRegion(pd->domain, buf, buf_size));
  }
  //
  // post receive : use a standard bgcios message structure
  //
  {
    if (expected_flag==CSCS_user_message::UnexpectedMessage) {
      if (client!=NULL) {
        LOG_DEBUG_MSG("Surprise! we didn't expect this");
        throw std::runtime_error("Nobody expects the Spanish Inquisition!");
      }
      // if a message was received before mercury managed to pre-post
      // get it from here and call completion immediately.
      if (pd->EarlyUnexpectedOps->size()>0) {
        struct na_verbs_op_id *early_op_id = pd->EarlyUnexpectedOps->front();
        LOG_DEBUG_MSG("Early message retrieved with wr_id " << early_op_id->wr_id);
        memcpy(na_verbs_op_id->info.recv.buf, early_op_id->info.recv.buf, na_verbs_op_id->info.recv.buf_size);
        na_verbs_op_id->info.recv.tag = early_op_id->info.recv.tag;
        pd->EarlyUnexpectedOps->pop_front();
        FUNC_END_DEBUG_MSG
        return na_verbs_complete(na_verbs_op_id);
      }

      // make sure all clients have a pre-posted receive in their queues
      pd->controller->for_each_client(
        [pd](MercuryController::ClientMapPair _client) {
          if (_client.second->getNumWaitingRecv()==0) {
            LOG_DEBUG_MSG("Posting a receive to client with qp_id " << _client.second->getQpNum());
            RdmaMemoryRegion* region = _client.second->getFreeRegion();
            std::shared_ptr<RdmaMemoryRegion> temp(region, NullDeleter<RdmaMemoryRegion>() );
            _client.second->postRecvRegionAsID(temp, (uint64_t)region->getAddress(), region->getLength());
          }
          else {
            LOG_DEBUG_MSG("Already waiting client with qp_id " << _client.second->getQpNum());
          }
        }
      );
      // store the unexpected op until it completes and is retrieved
      pd->UnexpectedOps->push_back(na_verbs_op_id);
    }
    else {
      if (client==NULL) {
        LOG_ERROR_MSG("Cannot post an expected message with no client");
      }
      LOG_DEBUG_MSG("RECV (ExpectedMessage) TAG value " << tag);

      RdmaMemoryRegion* region;
      if (pd->server) {
        region = client->getFreeRegion();
      }
      else {
        region = client->getFreeRegion();
      }
      std::lock_guard<std::mutex> lock(verbs_completion_map_mutex);
      std::shared_ptr<RdmaMemoryRegion> temp(region, NullDeleter<RdmaMemoryRegion>() );
      na_verbs_op_id->wr_id = client->postRecvRegionAsID(temp, (uint64_t)region->getAddress(), region->getLength());
      //
      // add wr_id to our map for checking on completions later
      //
      (*pd->ReceiveTagCompletionMap)[na_verbs_op_id->wr_id] = na_verbs_op_id;
      LOG_DEBUG_MSG("wr_id for recv expected added to Receive completion map " << na_verbs_op_id->wr_id << " Entries " << (*pd->ReceiveTagCompletionMap).size());
    }

  }

/*

  if (pd->server) {
    // lets do an RDMA send. Post this as we know there's a matching receive
    region = RdmaMemoryRegionPtr(
        new RdmaMemoryRegion(pd->controller->getProtectionDomain(), buf, buf_size));
    std::cout <<"Wiping memory buffer before posting receive " << std::endl;
    memset(region->getAddress(), 0, region->getLength());
    na_verbs_op_id->wr_id = pd->controller->getServer()->postRecv(region);
  }
  else {
    // lets do an RDMA send. Post this as we know there's a matching receive
    region = RdmaMemoryRegionPtr(
        new RdmaMemoryRegion(pd->domain, buf, buf_size));
    std::cout <<"Wiping memory buffer before posting receive " << std::endl;
    memset(region->getAddress(), 0, region->getLength());
    if (pd->client->postRecv(region)) {
      LOG_ERROR_MSG("Post Recv failed in recv expected");
    }
    else {
      // the RdmaConnection class uses the local key as the wr_id
      na_verbs_op_id->wr_id = (uint64_t)region->getLocalKey();
    }
    // the memory region will go out of scope, so we must keep a copy until the recv completes
    na_verbs_op_id->info.recv.rdmaMemRegionPtr = new RdmaMemoryRegionPtr();
    *(RdmaMemoryRegionPtr*)(na_verbs_op_id->info.recv.rdmaMemRegionPtr) = region;
  }
*/

   // Assign op_id
  *out_opid = (na_op_id_t) na_verbs_op_id;

  done:
  if (ret != NA_SUCCESS) {
    free(na_verbs_op_id);
  }
  free(expected_info);
  FUNC_END_DEBUG_MSG
  return ret;
}

/*---------------------------------------------------------------------------*/
static na_return_t
na_verbs_msg_recv_unexpected(na_class_t *na_class, na_context_t *context,
    na_cb_t callback, void *arg, void *buf, na_size_t buf_size,
    na_op_id_t *op_id)
{
  na_return_t ret;
  FUNC_START_DEBUG_MSG
  ret = na_verbs_msg_recv(
    na_class, context, callback, arg, buf, buf_size, NULL, 0, op_id,
    NA_CB_RECV_UNEXPECTED, CSCS_user_message::UnexpectedMessage);
  FUNC_END_DEBUG_MSG
  return ret;
}

/*---------------------------------------------------------------------------*/
static na_return_t
na_verbs_msg_recv_expected(na_class_t NA_UNUSED *na_class, na_context_t *context,
    na_cb_t callback, void *arg, void *buf, na_size_t buf_size,
    na_addr_t source, na_tag_t tag, na_op_id_t *op_id)
{
  na_return_t ret;
  FUNC_START_DEBUG_MSG
  ret = na_verbs_msg_recv(
    na_class, context, callback, arg, buf, buf_size, source, tag, op_id,
    NA_CB_RECV_EXPECTED, CSCS_user_message::ExpectedMessage);
  FUNC_END_DEBUG_MSG
  return ret;
}

/*---------------------------------------------------------------------------*/
static na_return_t
na_verbs_mem_handle_create(na_class_t NA_UNUSED *na_class, void *buf,
    na_size_t buf_size, unsigned long flags, na_mem_handle_t *mem_handle)
{
  FUNC_START_DEBUG_MSG
  na_return_t ret = NA_SUCCESS;
  //
  na_verbs_memhandle *handle = (na_verbs_memhandle*)(calloc(1, sizeof(struct na_verbs_memhandle)));
  handle->address   = buf;
  handle->bytes     = buf_size;
  handle->memkey    = 0;
  handle->memregion = NULL;
  //
  *mem_handle = (na_mem_handle_t*)handle;
  LOG_DEBUG_MSG("Mem Handle : address " << handle->address << " length " << handle->bytes << " key " << handle->memkey);
  ret = na_verbs_mem_register(na_class, *mem_handle);

  FUNC_END_DEBUG_MSG
  return ret;
}

/*---------------------------------------------------------------------------*/
static na_return_t
na_verbs_mem_handle_free(na_class_t NA_UNUSED *na_class, na_mem_handle_t mem_handle)
{
  FUNC_START_DEBUG_MSG
  na_verbs_memhandle *handle = NA_VERBS_MEM_PTR(mem_handle);
  na_return_t            ret = NA_SUCCESS;

  // take care of any stray registrations
  if (handle->memregion) {
    RdmaMemoryRegionPtr *ptr = (RdmaMemoryRegionPtr *)(handle->memregion);
    delete ptr;
    handle->memregion = NULL;
  }
  free(handle);

  FUNC_END_DEBUG_MSG
  return ret;
}

/*---------------------------------------------------------------------------*/
static na_return_t
na_verbs_mem_register(na_class_t *na_class, na_mem_handle_t mem_handle)
{
  FUNC_START_DEBUG_MSG
  na_verbs_memhandle  *handle = NA_VERBS_MEM_PTR(mem_handle);
  na_return_t             ret = NA_SUCCESS;
  na_verbs_private_data   *pd = NA_VERBS_PRIVATE_DATA(na_class);
  RdmaProtectionDomainPtr pdp;

  if (pd->server) {
    pdp = pd->controller->getProtectionDomain();
  }
  else{
    pdp = pd->domain;
  }
  if (!handle->memregion) {
  handle->memregion = new RdmaMemoryRegionPtr(new RdmaMemoryRegion(pdp, handle->address, handle->bytes));
  handle->memkey    = (*((RdmaMemoryRegionPtr*)(handle->memregion)))->getLocalKey();
  LOG_DEBUG_MSG("Mem Handle : address " << handle->address << " length " << handle->bytes << " key " << handle->memkey);
  counter++;
  LOG_DEBUG_MSG("Register counter is " << counter);
  }
  FUNC_END_DEBUG_MSG
  return ret;
}

/*---------------------------------------------------------------------------*/
static na_return_t
na_verbs_mem_deregister(na_class_t *na_class, na_mem_handle_t mem_handle)
{
  FUNC_START_DEBUG_MSG
  na_verbs_memhandle  *handle = NA_VERBS_MEM_PTR(mem_handle);
  na_return_t             ret = NA_SUCCESS;
  na_verbs_private_data   *pd = NA_VERBS_PRIVATE_DATA(na_class);

  LOG_DEBUG_MSG("Mem Handle : address " << handle->address << " length " << handle->bytes << " key " << handle->memkey);
  handle->memkey = 0;
  // this should destroy the shared pointer, and the region at the same time
  if (handle->memregion) {
    RdmaMemoryRegionPtr *ptr = (RdmaMemoryRegionPtr *)(handle->memregion);
    delete ptr;
    handle->memregion = NULL;
    counter--;
    LOG_DEBUG_MSG("Register counter is " << counter);
  }

  FUNC_END_DEBUG_MSG
  return ret;
}

/*---------------------------------------------------------------------------*/
static na_size_t
na_verbs_mem_handle_get_serialize_size(na_class_t NA_UNUSED *na_class,
    na_mem_handle_t mem_handle)
{
  na_verbs_memhandle  *handle = NA_VERBS_MEM_PTR(mem_handle);
  FUNC_START_DEBUG_MSG
//  LOG_DEBUG_MSG("Mem Handle : address " << handle->address << " length " << handle->bytes << " key " << handle->memkey);
  FUNC_END_DEBUG_MSG
  return sizeof(struct na_verbs_memhandle);
}

/*---------------------------------------------------------------------------*/
static na_return_t
na_verbs_mem_handle_serialize(na_class_t NA_UNUSED *na_class, void *buf,
    na_size_t buf_size, na_mem_handle_t mem_handle)
{
  FUNC_START_DEBUG_MSG
  na_verbs_memhandle *handle = NA_VERBS_MEM_PTR(mem_handle);
  na_return_t            ret = NA_SUCCESS;
  memcpy(buf, handle, sizeof(struct na_verbs_memhandle));
  LOG_DEBUG_MSG("Mem Handle : address " << handle->address << " length " << handle->bytes << " key " << handle->memkey);
  // make sure no object pointer is sent, by zeroing it out
  FUNC_END_DEBUG_MSG
  return ret;
}

/*---------------------------------------------------------------------------*/
static na_return_t
na_verbs_mem_handle_deserialize(na_class_t NA_UNUSED *na_class,
    na_mem_handle_t *mem_handle, const void *buf, na_size_t buf_size)
{
  FUNC_START_DEBUG_MSG
  na_return_t ret = NA_SUCCESS;
  //
  na_verbs_memhandle *handle = (na_verbs_memhandle*)(malloc(sizeof(struct na_verbs_memhandle)));
  memcpy(handle, buf, sizeof(struct na_verbs_memhandle));
  // make sure no object pointer is used, by zeroing it out
  handle->memregion = NULL;
  LOG_DEBUG_MSG("Mem Handle : address " << handle->address << " length " << handle->bytes << " key " << handle->memkey);
  //
  *mem_handle = (na_mem_handle_t*)handle;

  FUNC_END_DEBUG_MSG
  return ret;
}

/*---------------------------------------------------------------------------*/
static na_return_t
na_verbs_put(
    na_class_t *na_class,
    na_context_t *context,
    na_cb_t callback,
    void *arg,
    na_mem_handle_t local_mem_handle,
    na_offset_t local_offset,
    na_mem_handle_t remote_mem_handle,
    na_offset_t remote_offset,
    na_size_t length,
    na_addr_t remote_addr,
    na_op_id_t *out_opid)
{
  FUNC_START_DEBUG_MSG
  na_verbs_private_data      *pd = NA_VERBS_PRIVATE_DATA(na_class);
  na_verbs_memhandle     *local  = NA_VERBS_MEM_PTR(local_mem_handle);
  na_verbs_memhandle     *remote = NA_VERBS_MEM_PTR(remote_mem_handle);
  na_verbs_addr   *na_verbs_addr = (struct na_verbs_addr*)remote_addr;
  na_return_t                ret = NA_SUCCESS;
  na_verbs_op_id *na_verbs_op_id = NULL;
  static uint64_t          reqID = 10000;
  RdmaClientPtr           client;

  if (pd->server) {
    // Find the connection that received a message.
    LOG_DEBUG_MSG("Server making RDMA put");
    client = pd->controller->getClient(na_verbs_addr->qp_id);
  }
  else {
    LOG_DEBUG_MSG("Client making RDMA put");
    client = pd->client;
  }
  //  postRdmaWrite(uint64_t reqID, uint32_t remoteKey, uint64_t remoteAddr,
  //      uint32_t localKey,  uint64_t localAddr,
  //      ssize_t length, int flags)
  LOG_DEBUG_MSG("Mem local  Handle : address " << local->address << " length " << local->bytes << " key " << local->memkey);
  LOG_DEBUG_MSG("Mem remote Handle : address " << remote->address << " length " << remote->bytes << " key " << remote->memkey);
  {
    std::lock_guard<std::mutex> lock(verbs_completion_map_mutex);
    client->postRdmaWrite(rdma_put_ID,
        remote->memkey, (uint64_t)remote->address + remote_offset,
        local->memkey, (uint64_t)local->address + local_offset,
        length, IBV_SEND_SIGNALED);

    // Allocate na_op_id
    na_verbs_op_id = (struct na_verbs_op_id *) malloc(sizeof(struct na_verbs_op_id));
    if (!na_verbs_op_id) {
      NA_LOG_ERROR("Could not allocate NA VERBS operation ID");
      ret = NA_NOMEM_ERROR;
      goto done;
    }
    na_verbs_op_id->context               = context;
    na_verbs_op_id->type                  = NA_CB_PUT;
    na_verbs_op_id->callback              = callback;
    na_verbs_op_id->arg                   = arg;
    na_verbs_op_id->completed             = NA_FALSE;
    na_verbs_op_id->wr_id                 = rdma_put_ID;
    //
    rdma_put_ID++;
    //
    // add wr_id to our map for checking on completions later
    //
    std::cout << "Adding put wr_id to completion map " << na_verbs_op_id->wr_id << "\n";
    (*pd->WorkRequestCompletionMap)[na_verbs_op_id->wr_id] = na_verbs_op_id;
    LOG_DEBUG_MSG("wr_id for put added to WR completion map " << na_verbs_op_id->wr_id << " Entries " <<  (*pd->WorkRequestCompletionMap).size());
  }
  std::cout << "UN-3-Locking Mutex " << std::endl;

  // Assign op_id
  *out_opid = (na_op_id_t) na_verbs_op_id;

  done:
  if (ret != NA_SUCCESS) {
    free(na_verbs_op_id);
  }
  FUNC_END_DEBUG_MSG
  return ret;
}
/*---------------------------------------------------------------------------*/
static na_return_t
na_verbs_get(
    na_class_t *na_class,
    na_context_t *context,
    na_cb_t callback,
    void *arg,
    na_mem_handle_t local_mem_handle,
    na_offset_t local_offset,
    na_mem_handle_t remote_mem_handle,
    na_offset_t remote_offset,
    na_size_t length,
    na_addr_t remote_addr,
    na_op_id_t *out_opid)
{
  FUNC_START_DEBUG_MSG
  na_verbs_private_data      *pd = NA_VERBS_PRIVATE_DATA(na_class);
  na_verbs_memhandle     *local  = NA_VERBS_MEM_PTR(local_mem_handle);
  na_verbs_memhandle     *remote = NA_VERBS_MEM_PTR(remote_mem_handle);
  na_verbs_addr   *na_verbs_addr = (struct na_verbs_addr*)remote_addr;
  na_return_t                ret = NA_SUCCESS;
  na_verbs_op_id *na_verbs_op_id = NULL;
  RdmaClientPtr           client;

  if (pd->server) {
    // Find the connection that received a message.
    LOG_DEBUG_MSG("Server making RDMA get");
    client = pd->controller->getClient(na_verbs_addr->qp_id);
  }
  else {
    LOG_DEBUG_MSG("Client making RDMA get");
    client = pd->client;
  }
  //  postRdmaWrite(uint64_t reqID, uint32_t remoteKey, uint64_t remoteAddr,
  //      uint32_t localKey,  uint64_t localAddr,
  //      ssize_t length, int flags)
  LOG_DEBUG_MSG("Mem local  Handle : address " << local->address << " length " << local->bytes << " key " << local->memkey);
  LOG_DEBUG_MSG("Mem remote Handle : address " << remote->address << " length " << remote->bytes << " key " << remote->memkey);
  {
    std::lock_guard<std::mutex> lock(verbs_completion_map_mutex);

    client->postRdmaRead(rdma_get_ID,
        remote->memkey, (uint64_t)remote->address + remote_offset,
        local->memkey, (uint64_t)local->address + local_offset,
        length);

    // Allocate na_op_id
    na_verbs_op_id = (struct na_verbs_op_id *) malloc(sizeof(struct na_verbs_op_id));
    if (!na_verbs_op_id) {
      NA_LOG_ERROR("Could not allocate NA VERBS operation ID");
      ret = NA_NOMEM_ERROR;
      goto done;
    }
    na_verbs_op_id->context               = context;
    na_verbs_op_id->type                  = NA_CB_GET;
    na_verbs_op_id->callback              = callback;
    na_verbs_op_id->arg                   = arg;
    na_verbs_op_id->completed             = NA_FALSE;
    na_verbs_op_id->wr_id                 = rdma_get_ID;
    //
    rdma_get_ID++;
    //

    //
    // add wr_id to our map for checking on completions later
    //
    (*pd->WorkRequestCompletionMap)[na_verbs_op_id->wr_id] = na_verbs_op_id;
    LOG_DEBUG_MSG("wr_id for get added to WR completion map " << na_verbs_op_id->wr_id << " Entries " <<  (*pd->WorkRequestCompletionMap).size());
  }

  // Assign op_id
  *out_opid = (na_op_id_t) na_verbs_op_id;

  done:
  if (ret != NA_SUCCESS) {
    free(na_verbs_op_id);
  }
  FUNC_END_DEBUG_MSG
  return ret;
}

/*---------------------------------------------------------------------------*/
static na_return_t na_verbs_progress(na_class_t *na_class,
    na_context_t *context, unsigned int timeout)
{
//  FUNC_START_DEBUG_MSG
  double remaining = timeout / 1000; /* Convert timeout in ms into seconds */
  na_return_t ret = NA_SUCCESS;
  bool done = false;

  auto start_time = std::chrono::high_resolution_clock::now();

  na_verbs_private_data *pd = NA_VERBS_PRIVATE_DATA(na_class);

  while (!done) {
    if (pd->server) {
//      pd->controller->eventMonitor(0);
//      if (pd->controller->isTerminated()) {
//        ret = poll_cq_non_blocking(pd, pd->controller->GetCompletionChannel());
//        std::cout << "Non blocking Poll because we are terminated " << ret << std::endl;
//      } else {
//        LOG_DEBUG_MSG("Poll completion and event channel");
        pd->controller->eventMonitor(0);
//        ret = poll_cq(pd, pd->controller->GetCompletionChannel());
//      }

      // Monitor for events on all of the channels until told to stop.
      //    LOG_DEBUG_MSG("Poll eventmonitor");
      //pd->controller->eventMonitor(0);

    }
    else
    {
      // LOG_DEBUG_MSG("starting to poll CQ on client with timeout " << timeout);
      ret = poll_cq(pd, pd->completionChannel);
//      ret = poll_cq_non_blocking(pd, pd->completionChannel);
    }
    auto t2 = std::chrono::high_resolution_clock::now();
    auto msec = std::chrono::duration_cast < std::chrono::milliseconds > (t2 - start_time).count();
    // if (ret==NA_SUCCESS) done = true;
    // if (msec>timeout) done=true;
    done = true;
  }
  done:
//  FUNC_END_DEBUG_MSG
  return ret;
}

/*---------------------------------------------------------------------------*/
static na_return_t
na_verbs_complete(struct na_verbs_op_id *na_verbs_op_id)
{
  FUNC_START_DEBUG_MSG
  struct na_cb_info *callback_info = NULL;
  na_return_t ret = NA_SUCCESS;

  /* Mark op id as completed */
  na_verbs_op_id->completed = NA_TRUE;

  /* Allocate callback info */
  callback_info = (struct na_cb_info *) malloc(sizeof(struct na_cb_info));
  if (!callback_info) {
    NA_LOG_ERROR("Could not allocate callback info");
    ret = NA_NOMEM_ERROR;
    goto done;
  }
  callback_info->arg = na_verbs_op_id->arg;
  callback_info->ret = ret;
  callback_info->type = na_verbs_op_id->type;

  switch (na_verbs_op_id->type) {
    case NA_CB_LOOKUP:
      // we made a connection, store the connection info we will use to communicate
      callback_info->info.lookup.addr = na_verbs_op_id->verbs_addr;
      break;
    case NA_CB_SEND_UNEXPECTED:
      // data has gone, nothing to do
      break;
    case NA_CB_SEND_EXPECTED:
      // data has gone, nothing to do
      break;
    case NA_CB_RECV_UNEXPECTED:
    {
      LOG_DEBUG_MSG("inside NA_CB_RECV_UNEXPECTED, copying na_addr into callback");

      struct na_verbs_info_recv *unexpected_info = (struct na_verbs_info_recv *)&na_verbs_op_id->info.recv;

      callback_info->info.recv_unexpected.actual_buf_size = unexpected_info->buf_size;
      callback_info->info.recv_unexpected.source          = na_verbs_op_id->verbs_addr;
      callback_info->info.recv_unexpected.tag             = unexpected_info->tag;
      LOG_DEBUG_MSG("CALLBACK TAG value " << unexpected_info->tag);
    }
    break;
    case NA_CB_RECV_EXPECTED:
      // Check buf_size and actual_size
/*
      if (na_verbs_op_id->info.recv_expected.actual_size !=
          na_verbs_op_id->info.recv_expected.buf_size) {
        printf("size expected %d and received %d",
            na_verbs_op_id->info.recv_expected.buf_size,
            na_verbs_op_id->info.recv_expected.actual_size);
        NA_LOG_ERROR("Buffer size and actual transfer size do not match");
        ret = NA_SIZE_ERROR;
        goto done;
      }
*/
      break;
    case NA_CB_PUT:
      /* Transfer is now done so free RMA info */
//      free(na_verbs_op_id->info.put.rma_info);
      na_verbs_op_id->info.put.rma_info = NULL;
      break;
    case NA_CB_GET:
      /* Transfer is now done so free RMA info */
//      free(na_verbs_op_id->info.get.rma_info);
      na_verbs_op_id->info.get.rma_info = NULL;
      break;
    default:
      NA_LOG_ERROR("Operation not supported");
      ret = NA_INVALID_PARAM;
      break;
  }

  LOG_DEBUG_MSG("calling completion add ");
  ret = na_cb_completion_add(na_verbs_op_id->context, na_verbs_op_id->callback,
      callback_info, &na_verbs_release, na_verbs_op_id);
  if (ret != NA_SUCCESS) {
    NA_LOG_ERROR("Could not add callback to completion queue");
    goto done;
  }

  done:
  if (ret != NA_SUCCESS) {
    free(callback_info);
  }
  FUNC_END_DEBUG_MSG
  return ret;
}

/*---------------------------------------------------------------------------*/
static na_return_t
na_verbs_cancel(na_class_t NA_UNUSED *na_class, na_context_t *context,
    na_op_id_t op_id)
{
  FUNC_START_DEBUG_MSG
  struct na_verbs_op_id *na_verbs_op_id = (struct na_verbs_op_id *) op_id;
  //    verbs_context_id *verbs_context = (verbs_context_id *) context->plugin_context;
  na_return_t ret = NA_SUCCESS;

  /* TODO correct */
  //    VERBS_cancel(na_verbs_op_id->info.send_expected.op_id, *verbs_context);

  FUNC_END_DEBUG_MSG
  return ret;
}
/*---------------------------------------------------------------------------*/

/*---------------------------------------------------------------------------*/
na_return_t poll_cq_non_blocking(na_verbs_private_data *pd, RdmaCompletionChannelPtr channel)
{
  const int compChannel  = 0;
  const int numFds       = 1;
  na_return_t        ret = NA_SUCCESS;
  pollfd pollInfo[numFds];
  int polltimeout = 0;

  pollInfo[compChannel].fd = channel->getChannelFd();
  pollInfo[compChannel].events = POLLIN;
  pollInfo[compChannel].revents = 0;

  int rc = poll(pollInfo, 1, polltimeout);

  // There was no data so try again.
  if (rc == 0)
  {
    return NA_SUCCESS;
  }

  // There was an error so log the failure and try again.
  if (rc == -1)
  {
    int err = errno;
    if (err == EINTR)
    {
      LOG_CIOS_TRACE_MSG("poll returned EINTR, continuing ...");
      return NA_SUCCESS;
    }
    LOG_ERROR_MSG("error polling socket descriptors: " << bgcios::errorString(err));
    return NA_PROTOCOL_ERROR;
  }

  // Check for an event on the completion channel.
  if (pollInfo[compChannel].revents & POLLIN)
  {
    LOG_CIOS_TRACE_MSG("input event available on data channel");
    ret = poll_cq(pd, channel);
    pollInfo[compChannel].revents = 0;
  }
  return ret;
}
/*---------------------------------------------------------------------------*/
na_return_t poll_cq(na_verbs_private_data *pd, RdmaCompletionChannelPtr channel)
{
  struct ibv_cq *cq;
  struct ibv_wc  completion;
  void          *ctx;
  bool           completions = false;

  FUNC_START_DEBUG_MSG

  // create a function object with some parameters already bound
  using namespace std::placeholders;
  auto completion_function = std::bind( handle_verbs_completion, _1, pd, _2 );

  if (pd->server) {
    pd->controller->setCompletionFunction(completion_function);
    pd->controller->eventMonitor(1);
  }
  else {
    if (ibv_get_cq_event(channel->getChannel(), &cq, &ctx)==0) {
      ibv_ack_cq_events(cq, 1);
      if (ibv_req_notify_cq(cq, 0)!=0) {
        LOG_ERROR_MSG("ack cq event failed");
      };
    }
    else {
      LOG_ERROR_MSG("ibv_get_cq_event failed");
    }
    //
    // retrieve all completions one by one and trigger their completion handlers
    //
    while (!completions) {
      while (ibv_poll_cq(cq, 1, &completion))
      {
        completions = true;
        LOG_DEBUG_MSG("Poll CQ completing for work request " << completion.wr_id);
        completion_function(&completion, pd->client);
      }
    }
  }

  FUNC_END_DEBUG_MSG
  return completions ? NA_SUCCESS : NA_PROTOCOL_ERROR;
}
/*---------------------------------------------------------------------------*/
/*---------------------------------------------------------------------------*/
int handle_verbs_completion(struct ibv_wc *completion, na_verbs_private_data *pd, RdmaClientPtr client)
{
  FUNC_START_DEBUG_MSG
  int wc_q = 0;
  // Check the status in the completion queue entry.
  if (completion->status != IBV_WC_SUCCESS)
  {
    LOG_ERROR_MSG("failed work completion, status '" << ibv_wc_status_str(completion->status) << "' for operation "
        << RdmaCompletionQueue::wc_opcode_str(completion->opcode) <<  completion->opcode );
    return NA_PROTOCOL_ERROR;
  }

  std::lock_guard<std::mutex> lock(verbs_completion_map_mutex);
  //
  switch (completion->opcode)
  {
    case IBV_WC_SEND:
    {
      LOG_CIOS_TRACE_MSG("send operation completed successfully for queue pair " << completion->qp_num);
      int numRecv = client->decrementWaitingSend();
      LOG_DEBUG_MSG("Client waiting recv counter decremented and is now " << numRecv)
      wc_q = 1;
      break;
    }

    case IBV_WC_RECV:
    {
      LOG_CIOS_TRACE_MSG("receive operation completed successfully for queue pair " << completion->qp_num << " (received " << completion->byte_len << " bytes)");

      // decrement the counter we're tracking
      int numRecv = client->decrementWaitingRecv();
      LOG_DEBUG_MSG("Client waiting recv counter decremented and is now " << numRecv)

      // Handle the message.
      RdmaMemoryRegion *region = (RdmaMemoryRegion *)completion->wr_id;
      bgcios::MessageHeader            *msghdr = (bgcios::MessageHeader *)region->getAddress();
      CSCS_user_message::UserRDMA_message *msg = (CSCS_user_message::UserRDMA_message *)(msghdr);
      na_verbs_op_id                    *op_id = NULL;
      struct na_verbs_addr      *na_verbs_addr = NULL;

      LOG_DEBUG_MSG("received " << (msghdr->type) << " from client " << bgcios::printHeader(*msghdr).c_str());
      switch (msghdr->type)
      {
        case CSCS_user_message::UnexpectedMessage:
        case CSCS_user_message::ExpectedMessage:
          if (msghdr->type==CSCS_user_message::UnexpectedMessage) {
            LOG_DEBUG_MSG("received UnexpectedMessage, fetching unexpected receive");
            // for an unexpected message we must get the na_op_id to use for completion
            if (pd->UnexpectedOps->size()>0) {
              op_id = pd->UnexpectedOps->front();
              pd->UnexpectedOps->pop_front();
              // put this into the map where it will be fetched below : @todo tidy this
              (*pd->ReceiveTagCompletionMap)[completion->wr_id] = op_id;
              wc_q = 0;
            }
            else {
              LOG_WARN_MSG("Unexpected arrived before it has been posted - storing data until ready, wr_id = " << completion->wr_id);
              //
              // The buffer and na_op_id have not been assigned because the connection was made and an unexpected
              // receive has arrived before mercury server posted one.
              // Allocate a temp na_op_id until the receive is posted when it must be copied
              // into the real buffer that mercury sends in.
              //
              op_id = (struct na_verbs_op_id *) malloc(sizeof(struct na_verbs_op_id));
              if (!op_id) {
                NA_LOG_ERROR("Could not allocate NA VERBS operation ID");
                throw std::bad_alloc();
              }
              op_id->context               = 0;
              op_id->type                  = NA_CB_RECV_UNEXPECTED;
              op_id->callback              = 0;
              op_id->arg                   = 0;
              op_id->completed             = NA_TRUE;
              op_id->info.recv.buf_size    = CSCS_UserMessageDataSize;
              op_id->info.recv.buf         = malloc(CSCS_UserMessageDataSize);
              op_id->info.recv.tag         = 0;
              op_id->wr_id                 = completion->wr_id;
              //
              pd->EarlyUnexpectedOps->push_front(op_id);
              wc_q = 2; // do not call completion at end of this function
            }
          }
          else {
            LOG_DEBUG_MSG("received ExpectedMessage, fetching receive");
            //
            if (pd->ReceiveTagCompletionMap->find(completion->wr_id)!=pd->ReceiveTagCompletionMap->end()) {
              LOG_DEBUG_MSG("Found the work request ID in the Receive TAG completion map " << completion->wr_id << " Entries " << pd->ReceiveTagCompletionMap->size());
              op_id = (*pd->ReceiveTagCompletionMap)[completion->wr_id];
              wc_q = 0;
            }
            else {
              printf("Did not find completion %llu, throwing exception instead\n",completion->wr_id);
              throw std::runtime_error("Failed to find verbs op_id in completion list");
            }
          }
          //
          // At this point we should have a valid op_id pointer
          //
          if (op_id->info.recv.buf_size<CSCS_UserMessageDataSize) {
            throw std::runtime_error("Receive buffer was too small for unexpected message");
          }
          //
          // Copy the contents of the message into the buffer given during the receive call
          //
          memcpy(op_id->info.recv.buf, msg->MessageData, CSCS_UserMessageDataSize);
          op_id->info.recv.tag = msg->header2.tag;
          LOG_DEBUG_MSG("RECEIVED TAG value " << op_id->info.recv.tag);

          // unexpected messages need to tell mercury who sent them, so create an address object
          if (msghdr->type==CSCS_user_message::UnexpectedMessage) {
            // allocate the address information for storing details
            // we will use with future traffic to this destination
            na_verbs_addr = new struct na_verbs_addr();
            if (!na_verbs_addr) {
              throw std::bad_alloc();
            }
            LOG_DEBUG_MSG("filling na_addr during unexpected message - qp is " << completion->qp_num);
            na_verbs_addr->qp_id  = completion->qp_num;
            op_id->verbs_addr     = na_verbs_addr;
          }
          break;

        default:
          printf("unsupported message type %d received from client %s\n", msghdr->type, bgcios::printHeader(*msghdr).c_str());
          break;
      }

      break;
    }

    case IBV_WC_RDMA_READ:
    {
      LOG_CIOS_DEBUG_MSG("rdma read operation completed successfully for queue pair " << completion->qp_num);
      wc_q = 1;
      break;
    }

    case IBV_WC_RDMA_WRITE:
    {
      LOG_CIOS_DEBUG_MSG("rdma write operation completed successfully for queue pair " << completion->qp_num);
      wc_q = 1;
      break;
    }

    default:
    {
      LOG_ERROR_MSG("unsupported operation " << completion->opcode << " in work completion");
      break;
    }
  }
  na_return_t ret = NA_PROTOCOL_ERROR;
  if (wc_q == 1) {
    ret = on_completion_wr(pd, completion->wr_id);
  }
  else if (wc_q == 0) {
    ret = on_completion_tag(pd, completion->wr_id);
  }
  else {
    // this was a new connection unexpected message and we must store it until mercury posts the receive
    // do not call completion because mercury has not yet given this message an ID
  }
  FUNC_END_DEBUG_MSG
  return ret;
}
/*---------------------------------------------------------------------------*/
na_return_t on_completion_wr(na_verbs_private_data *pd, uint64_t wr_id)
{
  na_return_t ret = NA_SUCCESS;
  na_verbs_op_id * op_id = NULL;
  FUNC_START_DEBUG_MSG
  {
    if (pd->WorkRequestCompletionMap->find(wr_id)!=pd->WorkRequestCompletionMap->end()) {
      LOG_DEBUG_MSG("Found the work request ID in the WR completion map " << wr_id << " Entries " << pd->WorkRequestCompletionMap->size());
      op_id = (*pd->WorkRequestCompletionMap)[wr_id];
      (*pd->WorkRequestCompletionMap).erase(wr_id);
      ret = na_verbs_complete(op_id);
    }
    else {
      LOG_ERROR_MSG("Could not locate work request in WR completion map " << wr_id);
      // due to race conditions, it is possible for the thread to complete before the work request has been
      // added to the completion map!
      ret = NA_PROTOCOL_ERROR;
    }
  }
  FUNC_END_DEBUG_MSG
  return ret;
}
/*---------------------------------------------------------------------------*/
na_return_t on_completion_tag(na_verbs_private_data *pd, uint64_t wr_id)
{
  na_return_t ret = NA_SUCCESS;
  na_verbs_op_id * op_id = NULL;
  FUNC_START_DEBUG_MSG

//  std::lock_guard<std::mutex> lock(verbs_completion_map_mutex);
  //
  if (pd->ReceiveTagCompletionMap->find(wr_id)!=pd->ReceiveTagCompletionMap->end()) {
    LOG_DEBUG_MSG("Found the work request ID in the RT completion map " << wr_id
        << " Entries " << pd->ReceiveTagCompletionMap->size());
    op_id = (*pd->ReceiveTagCompletionMap)[wr_id];
    (*pd->ReceiveTagCompletionMap).erase(wr_id);
    ret = na_verbs_complete(op_id);
  }
  else {
    LOG_ERROR_MSG("Could not locate work request in Tag completion map " << wr_id);
    ret = NA_PROTOCOL_ERROR;
  }

  FUNC_END_DEBUG_MSG
  return ret;
}
/*---------------------------------------------------------------------------*/
