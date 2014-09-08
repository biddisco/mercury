/*
 * Copyright (C) 2013 Argonne National Laboratory, Department of Energy,
 * All rights reserved.
 *
 * The full copyright notice, including terms governing use, modification,
 * and redistribution, is contained in the COPYING file that can be
 * found at the root of the source code distribution tree.
 */

#ifndef NA_VERBS_H
#define NA_VERBS_H

#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <sys/time.h>
#include <sys/uio.h>
#include <unistd.h>
#include <assert.h>

#include "na.h"
#include "na_private.h"
#include "na_error.h"

#include "mercury_hash_table.h"
#include "mercury_list.h"
#include "mercury_queue.h"
#include "mercury_thread.h"
#include "mercury_thread_mutex.h"
#include "mercury_thread_condition.h"
#include "mercury_time.h"
#include "mercury_atomic.h"
#include "na.h"

#include <ib_user_verbs.h>

#include "cscs_messages.h"

#if (__GNUC__)
#define __likely(x)   __builtin_expect(!!(x), 1)
#define __unlikely(x) __builtin_expect(!!(x), 0)
#else
#define __likely(x)     (x)
#define __unlikely(x)   (x)
#endif

#define NA_VERBS_UNEXPECTED_SIZE CSCS_UserMessageDataSize
#define NA_VERBS_EXPECTED_SIZE   CSCS_UserMessageDataSize

/* Max tag */
#define NA_VERBS_MAX_TAG (NA_TAG_UB >> 2)

/* Default tag used for one-sided over two-sided */
#define NA_VERBS_RMA_REQUEST_TAG (NA_VERBS_MAX_TAG + 1)
#define NA_VERBS_RMA_TAG (NA_VERBS_RMA_REQUEST_TAG + 1)
#define NA_VERBS_MAX_RMA_TAG (NA_TAG_UB >> 1)

#define NA_VERBS_PRIVATE_DATA(na_class) \
    ((struct na_verbs_private_data *)(na_class->private_data))

// simple function which the server uses to
// write out the correct RDMA capable IP address
// it may not be the same as the standard IP address
void NA_VERBS_Get_rdma_device_address(const char *devicename, const char *iface, char *hostname);

struct na_verbs_addr;

typedef void *na_verbs_mem_handle;

struct na_verbs_memhandle {
    uint64_t      memkey;    //!< CNK kernel key for RDMA
    void         *address;   //!< CNK memory address of a memory region within user CNK process space
    uint64_t      bytes;     //!< Number of bytes to CNK memory region in CNK user process space to apply operations
    void         *memregion; // RdmaMemoryRegionPtr class - we do not serialize this
};

#define NA_VERBS_MEM_PTR(var) \
    ((na_verbs_memhandle *)(var))

struct verbs_mr {
  int memId;
};

struct verbs_get {
  struct verbs_mr memregion;
};

struct verbs_msg_recv_expected {
  struct verbs_mr memregion;
  void           *input_buffer;
  na_size_t       input_buffer_size;
};

typedef enum na_verbs_rma_op {
  NA_VERBS_RMA_PUT, /* Request a put operation */
  NA_VERBS_RMA_GET /* Request a get operation */
} na_verbs_rma_op_t;

struct na_verbs_rma_info {
  na_verbs_rma_op_t op; /* Operation requested */
  na_ptr_t base; /* Initial address of memory */
  na_size_t disp; /* Offset from initial address */
  na_size_t count; /* Number of entries */
  uint32_t transfer_tag; /* Tag used for the data transfer */
  uint32_t completion_tag; /* Tag used for completion ack */
};

struct na_verbs_info_send {
  uint64_t         wr_id;            // verbs operation ID
  void            *rdmaMemRegionPtr; // memory region sent
};

struct na_verbs_info_recv {
    void                 *buf;
    na_size_t             buf_size;
    na_tag_t              tag;
    void                 *rdmaMemRegionPtr; // memory region sent
};


struct na_verbs_info_put {
  uint64_t request_op_id;
  uint64_t transfer_op_id;
  na_size_t transfer_actual_size;
  uint64_t completion_op_id;
  na_size_t completion_actual_size;
  na_bool_t internal_progress;
//  verbs_addr_t remote_addr;
  struct na_verbs_rma_info *rma_info;
};

struct na_verbs_info_get {
  uint64_t request_op_id;
  uint64_t transfer_op_id;
  na_size_t transfer_actual_size;
  na_bool_t internal_progress;
//  verbs_addr_t remote_addr;
  struct na_verbs_rma_info *rma_info;
};

/* na_verbs_op_id  TODO uint64_t cookie for cancel ?*/
struct na_verbs_op_id {
  na_context_t *context;
  na_cb_type_t  type;
  na_cb_t       callback;
  void         *arg;
  na_bool_t     completed;
  uint64_t      wr_id;
  struct na_verbs_addr *verbs_addr;
  union {
    struct na_verbs_info_send send;
    struct na_verbs_info_recv recv;
    struct na_verbs_info_put  put;
    struct na_verbs_info_get  get;
  } info;
};

/* ************************************************* */
/* accept blocks until a connnection is made         */
/* ************************************************* */
#ifdef __cplusplus
extern "C" {
#endif
na_return_t na_verbs_accept(void *args);
unsigned int na_verbs_get_port(na_class_t *na_class);
#ifdef __cplusplus
}
#endif

#endif /* NA_VERBS_H */
