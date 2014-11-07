/*
 * Copyright (C) 2013 Argonne National Laboratory, Department of Energy,
 *                    UChicago Argonne, LLC and The HDF Group.
 * All rights reserved.
 *
 * The full copyright notice, including terms governing use, modification,
 * and redistribution, is contained in the COPYING file that can be
 * found at the root of the source code distribution tree.
 */

#include "mercury_test.h"

#include "mercury_time.h"
#ifdef MERCURY_TESTING_HAS_THREAD_POOL
#include "mercury_thread_pool.h"
#endif

/****************/
/* Local Macros */
/****************/
#define PIPELINE_SIZE 4
#define MIN_BUFFER_SIZE (2 << 8) /* 11 Stop at 4KB buffer size */

#ifdef MERCURY_TESTING_HAS_VERIFY_DATA
#define HG_TEST_ALLOC(size) calloc(size, sizeof(char))
#else
#define HG_TEST_ALLOC(size) malloc(size)
#endif

#ifdef MERCURY_TESTING_HAS_THREAD_POOL
#define HG_TEST_RPC_CB(func_name, handle) \
    static hg_return_t \
    func_name ## _thread_cb(hg_handle_t handle)

/* Assuming func_name_cb is defined, calling HG_TEST_THREAD_CB(func_name)
 * will define func_name_thread and func_name_thread_cb that can be used
 * to execute RPC callback from a thread
 */
#define HG_TEST_THREAD_CB(func_name) \
        static HG_THREAD_RETURN_TYPE \
        func_name ## _thread \
        (void *arg) \
        { \
            hg_handle_t handle = (hg_handle_t) arg; \
            hg_thread_ret_t thread_ret = (hg_thread_ret_t) 0; \
            \
            func_name ## _thread_cb(handle); \
            \
            return thread_ret; \
        } \
        hg_return_t \
        func_name ## _cb(hg_handle_t handle) \
        { \
            hg_return_t ret = HG_SUCCESS; \
            \
            hg_thread_pool_post(hg_test_thread_pool_g, func_name ## _thread, \
                    handle); \
            \
            return ret; \
        }
#else
#define HG_TEST_RPC_CB(func_name, handle) \
    hg_return_t \
    func_name ## _cb(hg_handle_t handle)
#define HG_TEST_THREAD_CB(func_name)
#endif

/*******************/
/* Local Variables */
/*******************/
#ifdef MERCURY_TESTING_HAS_THREAD_POOL
extern hg_thread_pool_t *hg_test_thread_pool_g;
#endif

/*---------------------------------------------------------------------------*/
/* Actual definition of the functions that need to be executed */
/*---------------------------------------------------------------------------*/
static HG_INLINE int
rpc_open(const char *path, rpc_handle_t handle, int *event_id)
{
    printf("Called rpc_open of %s with cookie %lu\n", path, handle.cookie);
    *event_id = 232;
    return HG_SUCCESS;
}

/*---------------------------------------------------------------------------*/
static HG_INLINE size_t
bulk_write(int fildes, const void *buf, size_t offset, size_t nbyte, int verbose)
{
#ifdef MERCURY_TESTING_HAS_VERIFY_DATA
    size_t i;
    int error = 0;
    const int *bulk_buf = (const int*) buf;

    if (verbose)
        printf("Executing bulk_write with fildes %d...\n", fildes);

    if (nbyte == 0) {
        HG_ERROR_DEFAULT("Error detected in bulk transfer, nbyte is zero!\n");
        error = 1;
    }

    if (verbose)
        printf("Checking data...\n");

    /* Check bulk buf */
    for (i = 0; i < (nbyte / sizeof(int)); i++) {
        if (bulk_buf[i] != (int) (i + offset)) {
            printf("Error detected in bulk transfer, bulk_buf[%lu] = %d, "
                    "was expecting %d!\n", i, bulk_buf[i], (int) (i + offset));
            error = 1;
            break;
        }
    }
    if (!error && verbose) printf("Successfully transfered %lu bytes!\n", nbyte);
#else
    (void) fildes;
    (void) buf;
    (void) offset;
    (void) verbose;
#endif

    return nbyte;
}

/*---------------------------------------------------------------------------*/
static void
pipeline_bulk_write(double sleep_time, hg_bulk_request_t bulk_request,
        hg_status_t *status)
{
    int ret;
    hg_time_t t1, t2;
    double time_remaining;

    time_remaining = sleep_time;

    /* Force MPI progress for time_remaining ms */
    if (bulk_request != HG_BULK_REQUEST_NULL) {
        hg_time_get_current(&t1);

        ret = HG_Bulk_wait(bulk_request, (unsigned int) time_remaining, status);
        if (ret != HG_SUCCESS) {
            fprintf(stderr, "Error while waiting\n");
        }

        hg_time_get_current(&t2);
        time_remaining -= hg_time_to_double(hg_time_subtract(t2, t1));
    }

    if (time_remaining > 0) {
        /* Should use nanosleep or equivalent */
        hg_time_sleep(hg_time_from_double(time_remaining), NULL);
    }
}

/*---------------------------------------------------------------------------*/
/* RPC callbacks */
/*---------------------------------------------------------------------------*/
HG_TEST_RPC_CB(hg_test_rpc_open, handle)
{
    hg_return_t ret = HG_SUCCESS;

    rpc_open_in_t  rpc_open_in_struct;
    rpc_open_out_t rpc_open_out_struct;

    hg_const_string_t rpc_open_path;
    rpc_handle_t rpc_open_handle;
    int rpc_open_event_id;
    int rpc_open_ret;

    /* Get input buffer */
    ret = HG_Handler_get_input(handle, &rpc_open_in_struct);
    if (ret != HG_SUCCESS) {
        fprintf(stderr, "Could not get input\n");
        return ret;
    }

    /* Get parameters */
    rpc_open_path = rpc_open_in_struct.path;
    rpc_open_handle = rpc_open_in_struct.handle;

    /* Call rpc_open */
    rpc_open_ret = rpc_open(rpc_open_path, rpc_open_handle, &rpc_open_event_id);

    /* Fill output structure */
    rpc_open_out_struct.event_id = rpc_open_event_id;
    rpc_open_out_struct.ret = rpc_open_ret;

    /* Free handle and send response back */
    ret = HG_Handler_start_output(handle, &rpc_open_out_struct);
    if (ret != HG_SUCCESS) {
        fprintf(stderr, "Could not respond\n");
        return ret;
    }

    HG_Handler_free_input(handle, &rpc_open_in_struct);
    HG_Handler_free(handle);

    return ret;
}

/*---------------------------------------------------------------------------*/
HG_TEST_RPC_CB(hg_test_bulk_write, handle)
{
    hg_return_t ret = HG_SUCCESS;

    bulk_write_in_t  bulk_write_in_struct;
    bulk_write_out_t bulk_write_out_struct;

    na_addr_t source = HG_Handler_get_addr(handle);
    hg_bulk_t bulk_write_bulk_handle = HG_BULK_NULL;
    hg_bulk_t bulk_write_bulk_block_handle = HG_BULK_NULL;
    hg_bulk_request_t bulk_write_bulk_request;

    int bulk_write_fildes;
    void *bulk_write_buf;
    size_t bulk_write_nbytes;
    size_t bulk_write_ret;

    /* Get input parameters and data */
    ret = HG_Handler_get_input(handle, &bulk_write_in_struct);
    if (ret != HG_SUCCESS) {
        fprintf(stderr, "Could not get input\n");
        return ret;
    }

    /* Get parameters */
    bulk_write_fildes = bulk_write_in_struct.fildes;
    bulk_write_bulk_handle = bulk_write_in_struct.bulk_handle;

    bulk_write_nbytes = HG_Bulk_handle_get_size(bulk_write_bulk_handle);

    /* Create a new block handle to read the data */
    HG_Bulk_handle_create(1, NULL, &bulk_write_nbytes,
    HG_BULK_READWRITE, &bulk_write_bulk_block_handle);

    /* Read bulk data here and wait for the data to be here  */
    ret = HG_Bulk_transfer(HG_BULK_PULL, source, bulk_write_bulk_handle, 0,
            bulk_write_bulk_block_handle, 0, bulk_write_nbytes,
            &bulk_write_bulk_request);
    if (ret != HG_SUCCESS) {
        fprintf(stderr, "Could not read bulk data\n");
        return ret;
    }

    ret = HG_Bulk_wait(bulk_write_bulk_request, HG_MAX_IDLE_TIME, HG_STATUS_IGNORE);
    if (ret != HG_SUCCESS) {
        fprintf(stderr, "Could not complete bulk data read\n");
        return ret;
    }

    /* Call bulk_write */
    HG_Bulk_handle_access(bulk_write_bulk_block_handle, 0, bulk_write_nbytes,
            HG_BULK_READWRITE, 1, &bulk_write_buf, NULL, NULL);

    bulk_write_ret = bulk_write(bulk_write_fildes, bulk_write_buf, 0,
            bulk_write_nbytes, 1);

    /* Fill output structure */
    bulk_write_out_struct.ret = bulk_write_ret;

    /* Free block handle */
    ret = HG_Bulk_handle_free(bulk_write_bulk_block_handle);
    if (ret != HG_SUCCESS) {
        fprintf(stderr, "Could not free block call\n");
        return ret;
    }

    /* Free handle and send response back */
    ret = HG_Handler_start_output(handle, &bulk_write_out_struct);
    if (ret != HG_SUCCESS) {
        fprintf(stderr, "Could not respond\n");
        return ret;
    }

    HG_Handler_free_input(handle, &bulk_write_in_struct);
    HG_Handler_free(handle);

    return ret;
}

/*---------------------------------------------------------------------------*/
HG_TEST_RPC_CB(hg_test_bulk_seg_write, handle)
{
    hg_return_t ret = HG_SUCCESS;

    bulk_write_in_t  bulk_write_in_struct;
    bulk_write_out_t bulk_write_out_struct;

    na_addr_t source = HG_Handler_get_addr(handle);
    hg_bulk_t bulk_write_bulk_handle = HG_BULK_NULL;
    hg_bulk_t bulk_write_bulk_block_handle1 = HG_BULK_NULL;
    hg_bulk_t bulk_write_bulk_block_handle2 = HG_BULK_NULL;
    size_t bulk_write_nbytes_read;
    ptrdiff_t bulk_write_offset;
    hg_bulk_request_t bulk_write_bulk_request1;
    hg_bulk_request_t bulk_write_bulk_request2;

    int bulk_write_fildes;
    void *bulk_write_buf;
    size_t bulk_write_nbytes;
    int bulk_write_ret = 0;

    /* Get input parameters and data */
    ret = HG_Handler_get_input(handle, &bulk_write_in_struct);
    if (ret != HG_SUCCESS) {
        fprintf(stderr, "Could not get input\n");
        return ret;
    }

    /* Get parameters */
    bulk_write_fildes = bulk_write_in_struct.fildes;
    bulk_write_bulk_handle = bulk_write_in_struct.bulk_handle;

    /* Create a new block handle to read the data */
    bulk_write_nbytes = HG_Bulk_handle_get_size(bulk_write_bulk_handle);

    /* For testing purposes try to read the data in two blocks of different sizes */
    bulk_write_nbytes_read = bulk_write_nbytes / 2 + 16;

    printf("Start reading first chunk of %lu bytes...\n", bulk_write_nbytes_read);

    bulk_write_buf = HG_TEST_ALLOC(bulk_write_nbytes);

    HG_Bulk_handle_create(1, &bulk_write_buf, &bulk_write_nbytes,
            HG_BULK_READWRITE, &bulk_write_bulk_block_handle1);

    ret = HG_Bulk_transfer(HG_BULK_PULL, source, bulk_write_bulk_handle, 0,
            bulk_write_bulk_block_handle1, 0, bulk_write_nbytes_read,
            &bulk_write_bulk_request1);
    if (ret != HG_SUCCESS) {
        fprintf(stderr, "Could not read bulk data\n");
        return ret;
    }

    bulk_write_offset = bulk_write_nbytes_read;
    bulk_write_nbytes_read = bulk_write_nbytes - bulk_write_nbytes_read;

    printf("Start reading second chunk of %lu bytes...\n", bulk_write_nbytes_read);

    ret = HG_Bulk_transfer(HG_BULK_PULL, source, bulk_write_bulk_handle,
            bulk_write_offset, bulk_write_bulk_block_handle1, bulk_write_offset,
            bulk_write_nbytes_read, &bulk_write_bulk_request2);
    if (ret != HG_SUCCESS) {
        fprintf(stderr, "Could not read bulk data\n");
        return ret;
    }

    printf("Waiting for first chunk...\n");
    ret = HG_Bulk_wait(bulk_write_bulk_request1, HG_MAX_IDLE_TIME, HG_STATUS_IGNORE);
    if (ret != HG_SUCCESS) {
        fprintf(stderr, "Could not complete bulk data read\n");
        return ret;
    }

    printf("Waiting for second chunk...\n");
    ret = HG_Bulk_wait(bulk_write_bulk_request2, HG_MAX_IDLE_TIME, HG_STATUS_IGNORE);
    if (ret != HG_SUCCESS) {
        fprintf(stderr, "Could not complete bulk data read\n");
        return ret;
    }

    /* Call bulk_write */
    bulk_write_ret = bulk_write(bulk_write_fildes, bulk_write_buf, 0,
            bulk_write_nbytes, 0);

    /* Fill output structure */
    bulk_write_out_struct.ret = bulk_write_ret;

    /* Free handle and send response back */
    ret = HG_Handler_start_output(handle, &bulk_write_out_struct);
    if (ret != HG_SUCCESS) {
        fprintf(stderr, "Could not respond\n");
        return ret;
    }

    /* Free block handle */
    ret = HG_Bulk_handle_free(bulk_write_bulk_block_handle1);
    if (ret != HG_SUCCESS) {
        fprintf(stderr, "Could not free block call\n");
        return ret;
    }
    ret = HG_Bulk_handle_free(bulk_write_bulk_block_handle2);
    if (ret != HG_SUCCESS) {
        fprintf(stderr, "Could not free block call\n");
        return ret;
    }

    free(bulk_write_buf);

    printf("\n");

    HG_Handler_free_input(handle, &bulk_write_in_struct);
    HG_Handler_free(handle);
    return ret;
}

/*---------------------------------------------------------------------------*/
HG_TEST_RPC_CB(hg_test_pipeline_write, handle)
{
    hg_return_t ret = HG_SUCCESS;

    bulk_write_in_t  bulk_write_in_struct;
    bulk_write_out_t bulk_write_out_struct;

    na_addr_t source = HG_Handler_get_addr(handle);
    hg_bulk_t bulk_write_bulk_handle = HG_BULK_NULL;
    hg_bulk_t bulk_write_bulk_block_handle = HG_BULK_NULL;
    hg_bulk_request_t bulk_write_bulk_request[PIPELINE_SIZE];
    int pipeline_iter;
    size_t pipeline_buffer_size;

    void *bulk_write_buf;
    size_t bulk_write_nbytes;
    size_t bulk_write_ret = 0;

    /* For timing */
    static int first_call = 1; /* Only used for dummy printf */
    double nmbytes;
    int avg_iter;
    double proc_time_read = 0;
    double raw_read_bandwidth, proc_read_bandwidth;
    static double raw_time_read = 0;

    if (first_call) printf("# Received new request\n");

    /* Get input parameters and data */
    ret = HG_Handler_get_input(handle, &bulk_write_in_struct);
    if (ret != HG_SUCCESS) {
        fprintf(stderr, "Could not get input\n");
        return ret;
    }

    /* Get parameters */
    /* unused bulk_write_fildes = bulk_write_in_struct.fildes; */
    bulk_write_bulk_handle = bulk_write_in_struct.bulk_handle;

    /* Create a new block handle to read the data */
    bulk_write_nbytes = HG_Bulk_handle_get_size(bulk_write_bulk_handle);
    bulk_write_buf = HG_TEST_ALLOC(bulk_write_nbytes);

    HG_Bulk_handle_create(1, &bulk_write_buf, &bulk_write_nbytes,
            HG_BULK_READWRITE, &bulk_write_bulk_block_handle);

    /* Timing info */
    nmbytes = (double) bulk_write_nbytes / (1024 * 1024);
    if (first_call) printf("# Reading Bulk Data (%f MB)\n", nmbytes);

    /* Work out BW without pipeline and without processing data */
    for (avg_iter = 0; avg_iter < MERCURY_TESTING_MAX_LOOP; avg_iter++) {
        hg_time_t t1, t2;

        hg_time_get_current(&t1);

        ret = HG_Bulk_transfer(HG_BULK_PULL, source, bulk_write_bulk_handle, 0,
                bulk_write_bulk_block_handle, 0, bulk_write_nbytes,
                &bulk_write_bulk_request[0]);
        if (ret != HG_SUCCESS) {
            fprintf(stderr, "Could not read bulk data\n");
            return ret;
        }

        ret = HG_Bulk_wait(bulk_write_bulk_request[0],
                HG_MAX_IDLE_TIME, HG_STATUS_IGNORE);
        if (ret != HG_SUCCESS) {
            fprintf(stderr, "Could not complete bulk data read\n");
            return ret;
        }

        hg_time_get_current(&t2);

        raw_time_read += hg_time_to_double(hg_time_subtract(t2, t1));
    }

    raw_time_read = raw_time_read / MERCURY_TESTING_MAX_LOOP;
    raw_read_bandwidth = nmbytes / raw_time_read;
    if (first_call) printf("# Raw read time: %f s (%.*f MB/s)\n", raw_time_read, 2, raw_read_bandwidth);

    /* Work out BW without pipeline and with processing data */
    for (avg_iter = 0; avg_iter < MERCURY_TESTING_MAX_LOOP; avg_iter++) {
        hg_time_t t1, t2;

        hg_time_get_current(&t1);

        ret = HG_Bulk_transfer(HG_BULK_PULL, source, bulk_write_bulk_handle, 0,
                bulk_write_bulk_block_handle, 0, bulk_write_nbytes,
                &bulk_write_bulk_request[0]);
        if (ret != HG_SUCCESS) {
            fprintf(stderr, "Could not read bulk data\n");
            return ret;
        }

        ret = HG_Bulk_wait(bulk_write_bulk_request[0],
                HG_MAX_IDLE_TIME, HG_STATUS_IGNORE);
        if (ret != HG_SUCCESS) {
            fprintf(stderr, "Could not complete bulk data read\n");
            return ret;
        }

        /* Call bulk_write */
        pipeline_bulk_write(0, HG_BULK_REQUEST_NULL, HG_STATUS_IGNORE);

        hg_time_get_current(&t2);

        proc_time_read += hg_time_to_double(hg_time_subtract(t2, t1));
    }

    proc_time_read = proc_time_read / MERCURY_TESTING_MAX_LOOP;
    proc_read_bandwidth = nmbytes / proc_time_read;
    if (first_call) printf("# Proc read time: %f s (%.*f MB/s)\n", proc_time_read, 2, proc_read_bandwidth);

    if (first_call) printf("%-*s%*s%*s%*s%*s%*s%*s", 12, "# Size (kB) ",
            10, "Time (s)", 10, "Min (s)", 10, "Max (s)",
            12, "BW (MB/s)", 12, "Min (MB/s)", 12, "Max (MB/s)");
    if (first_call) printf("\n");

    if (!PIPELINE_SIZE) fprintf(stderr, "PIPELINE_SIZE must be > 0!\n");

    for (pipeline_buffer_size = bulk_write_nbytes / PIPELINE_SIZE;
            pipeline_buffer_size > MIN_BUFFER_SIZE;
            pipeline_buffer_size /= 2) {
        double time_read = 0, min_time_read = 0, max_time_read = 0;
        double read_bandwidth, min_read_bandwidth, max_read_bandwidth;

        for (avg_iter = 0; avg_iter < MERCURY_TESTING_MAX_LOOP; avg_iter++) {
            size_t start_offset = 0;
            size_t total_bytes_read = 0;
            size_t chunk_size;

            hg_time_t t1, t2;
            double td;
            double sleep_time;

            chunk_size = (PIPELINE_SIZE == 1) ? bulk_write_nbytes : pipeline_buffer_size;
            sleep_time = chunk_size * raw_time_read / bulk_write_nbytes;

            hg_time_get_current(&t1);

            /* Initialize pipeline */
            for (pipeline_iter = 0; pipeline_iter < PIPELINE_SIZE; pipeline_iter++) {
                size_t write_offset = start_offset + pipeline_iter * chunk_size;

                ret = HG_Bulk_transfer(HG_BULK_PULL, source,
                        bulk_write_bulk_handle, write_offset,
                        bulk_write_bulk_block_handle, write_offset, chunk_size,
                        &bulk_write_bulk_request[pipeline_iter]);
                if (ret != HG_SUCCESS) {
                    fprintf(stderr, "Could not read bulk data\n");
                    return ret;
                }
            }

            while (total_bytes_read != bulk_write_nbytes) {
                /* Alternate wait and read to receives pieces */
                for (pipeline_iter = 0; pipeline_iter < PIPELINE_SIZE; pipeline_iter++) {
                    size_t write_offset = start_offset + pipeline_iter * chunk_size;
                    hg_status_t status;
                    int pipeline_next;

                    if (bulk_write_bulk_request[pipeline_iter] != HG_BULK_REQUEST_NULL) {
                        ret = HG_Bulk_wait(bulk_write_bulk_request[pipeline_iter],
                                HG_MAX_IDLE_TIME, HG_STATUS_IGNORE);
                        if (ret != HG_SUCCESS) {
                            fprintf(stderr, "Could not complete bulk data read\n");
                            return ret;
                        }
                        bulk_write_bulk_request[pipeline_iter] = HG_BULK_REQUEST_NULL;
                    }
                    total_bytes_read += chunk_size;
                    /* printf("total_bytes_read: %lu\n", total_bytes_read); */

                    /* Call bulk_write */
                    pipeline_next = (pipeline_iter < PIPELINE_SIZE - 1) ?
                            pipeline_iter + 1 : 0;

                    pipeline_bulk_write(sleep_time,
                            bulk_write_bulk_request[pipeline_next], &status);

                    if (status) bulk_write_bulk_request[pipeline_next] = HG_BULK_REQUEST_NULL;

                    /* Start another read (which is PIPELINE_SIZE far) */
                    write_offset += chunk_size * PIPELINE_SIZE;
                    if (write_offset < bulk_write_nbytes) {
                        ret = HG_Bulk_transfer(HG_BULK_PULL, source,
                                bulk_write_bulk_handle, write_offset,
                                bulk_write_bulk_block_handle, write_offset,
                                chunk_size,
                                &bulk_write_bulk_request[pipeline_iter]);
                        if (ret != HG_SUCCESS) {
                            fprintf(stderr, "Could not read bulk data\n");
                            return ret;
                        }
                    }
                    /* TODO should also check remaining data */
                }
                start_offset += chunk_size * PIPELINE_SIZE;
            }

            hg_time_get_current(&t2);

            td = hg_time_to_double(hg_time_subtract(t2, t1));

            time_read += td;
            if (!min_time_read) min_time_read = time_read;
            min_time_read = (td < min_time_read) ? td : min_time_read;
            max_time_read = (td > max_time_read) ? td : max_time_read;
        }

        time_read = time_read / MERCURY_TESTING_MAX_LOOP;
        read_bandwidth = nmbytes / time_read;
        min_read_bandwidth = nmbytes / max_time_read;
        max_read_bandwidth = nmbytes / min_time_read;

        /* At this point we have received everything so work out the bandwidth */
        printf("%-*d%*f%*f%*f%*.*f%*.*f%*.*f\n", 12, (int) pipeline_buffer_size / 1024,
                10, time_read, 10, min_time_read, 10, max_time_read,
                12, 2, read_bandwidth, 12, 2, min_read_bandwidth, 12, 2, max_read_bandwidth);

        /* Check data */
        bulk_write_ret = bulk_write(1, bulk_write_buf, 0, bulk_write_nbytes, 0);
    }

    /* Fill output structure */
    bulk_write_out_struct.ret = bulk_write_ret;

    /* Free handle and send response back */
    ret = HG_Handler_start_output(handle, &bulk_write_out_struct);
    if (ret != HG_SUCCESS) {
        fprintf(stderr, "Could not respond\n");
        return ret;
    }

    /* Free block handle */
    ret = HG_Bulk_handle_free(bulk_write_bulk_block_handle);
    if (ret != HG_SUCCESS) {
        fprintf(stderr, "Could not free block call\n");
        return ret;
    }

    free(bulk_write_buf);

    first_call = 0;

    HG_Handler_free_input(handle, &bulk_write_in_struct);
    HG_Handler_free(handle);

    return ret;
}

/*---------------------------------------------------------------------------*/
HG_TEST_RPC_CB(hg_test_posix_open, handle)
{
    hg_return_t hg_ret = HG_SUCCESS;

    open_in_t      open_in_struct;
    open_out_t     open_out_struct;

    const char *path;
    int flags;
    mode_t mode;
    int ret;

    /* Get input struct */
    hg_ret = HG_Handler_get_input(handle, &open_in_struct);
    if (hg_ret != HG_SUCCESS) {
        fprintf(stderr, "Could not get input struct\n");
        return hg_ret;
    }

    path = open_in_struct.path;
    flags = open_in_struct.flags;
    mode = open_in_struct.mode;

    /* Call open */
    printf("Calling open with path: %s\n", path);
    ret = open(path, flags, mode);

    /* Fill output structure */
    open_out_struct.ret = ret;

    /* Free handle and send response back */
    hg_ret = HG_Handler_start_output(handle, &open_out_struct);
    if (hg_ret != HG_SUCCESS) {
        fprintf(stderr, "Could not respond\n");
        return hg_ret;
    }

    HG_Handler_free_input(handle, &open_in_struct);
    HG_Handler_free(handle);

    return hg_ret;
}

/*---------------------------------------------------------------------------*/
HG_TEST_RPC_CB(hg_test_posix_close, handle)
{
    hg_return_t hg_ret = HG_SUCCESS;

    close_in_t     close_in_struct;
    close_out_t    close_out_struct;

    int fd;
    int ret;

    /* Get input struct */
    hg_ret = HG_Handler_get_input(handle, &close_in_struct);
    if (hg_ret != HG_SUCCESS) {
        fprintf(stderr, "Could not get input struct\n");
        return hg_ret;
    }

    fd = close_in_struct.fd;

    /* Call close */
    printf("Calling close with fd: %d\n", fd);
    ret = close(fd);

    /* Fill output structure */
    close_out_struct.ret = ret;

    /* Free handle and send response back */
    hg_ret = HG_Handler_start_output(handle, &close_out_struct);
    if (hg_ret != HG_SUCCESS) {
        fprintf(stderr, "Could not respond\n");
        return hg_ret;
    }

    HG_Handler_free_input(handle, &close_in_struct);
    HG_Handler_free(handle);

    return hg_ret;
}

/*---------------------------------------------------------------------------*/
HG_TEST_RPC_CB(hg_test_posix_write, handle)
{
    hg_return_t hg_ret = HG_SUCCESS;

    write_in_t     write_in_struct;
    write_out_t    write_out_struct;

    na_addr_t source = HG_Handler_get_addr(handle);
    hg_bulk_t bulk_handle = HG_BULK_NULL;
    hg_bulk_t bulk_block_handle = HG_BULK_NULL;
    hg_bulk_request_t bulk_request;

    int fd;
    void *buf;
    size_t count;
    ssize_t ret;

    /* for debug */
    int i;
    const int *buf_ptr;

    /* Get input struct */
    hg_ret = HG_Handler_get_input(handle, &write_in_struct);
    if (hg_ret != HG_SUCCESS) {
        fprintf(stderr, "Could not get input struct\n");
        return hg_ret;
    }

    bulk_handle = write_in_struct.bulk_handle;
    fd = write_in_struct.fd;

    /* Read bulk data here and wait for the data to be here */
    count = HG_Bulk_handle_get_size(bulk_handle);

    buf = HG_TEST_ALLOC(count);

    HG_Bulk_handle_create(1, &buf, &count, HG_BULK_READWRITE,
            &bulk_block_handle);

    hg_ret = HG_Bulk_transfer(HG_BULK_PULL, source, bulk_handle, 0,
            bulk_block_handle, 0, count, &bulk_request);
    if (hg_ret != HG_SUCCESS) {
        fprintf(stderr, "Could not read bulk data\n");
        return hg_ret;
    }

    hg_ret = HG_Bulk_wait(bulk_request, HG_MAX_IDLE_TIME, HG_STATUS_IGNORE);
    if (hg_ret != HG_SUCCESS) {
        fprintf(stderr, "Could not complete bulk data read\n");
        return hg_ret;
    }

    /* Check bulk buf */
    buf_ptr = (const int*) buf;
    for (i = 0; i < (int)(count / sizeof(int)); i++) {
        if (buf_ptr[i] != i) {
            printf("Error detected in bulk transfer, buf[%d] = %d, was expecting %d!\n", i, buf_ptr[i], i);
            break;
        }
    }

    printf("Calling write with fd: %d\n", fd);
    ret = write(fd, buf, count);

    /* Fill output structure */
    write_out_struct.ret = ret;

    /* Free handle and send response back */
    hg_ret = HG_Handler_start_output(handle, &write_out_struct);
    if (hg_ret != HG_SUCCESS) {
        fprintf(stderr, "Could not respond\n");
        return hg_ret;
    }

    /* Free block handle */
    hg_ret = HG_Bulk_handle_free(bulk_block_handle);
    if (hg_ret != HG_SUCCESS) {
        fprintf(stderr, "Could not free block call\n");
        return hg_ret;
    }

    free(buf);

    HG_Handler_free_input(handle, &write_in_struct);
    HG_Handler_free(handle);

    return hg_ret;
}

/*---------------------------------------------------------------------------*/
HG_TEST_RPC_CB(hg_test_posix_read, handle)
{
    hg_return_t hg_ret = HG_SUCCESS;

    read_in_t     read_in_struct;
    read_out_t    read_out_struct;

    na_addr_t dest = HG_Handler_get_addr(handle);
    hg_bulk_t bulk_handle = HG_BULK_NULL;
    hg_bulk_t bulk_block_handle = HG_BULK_NULL;
    hg_bulk_request_t bulk_request;

    int fd;
    void *buf;
    size_t count;
    ssize_t ret;

    /* for debug */
    int i;
    const int *buf_ptr;

    /* Get input struct */
    hg_ret = HG_Handler_get_input(handle, &read_in_struct);
    if (hg_ret != HG_SUCCESS) {
        fprintf(stderr, "Could not get input struct\n");
        return hg_ret;
    }

    bulk_handle = read_in_struct.bulk_handle;
    fd = read_in_struct.fd;

    /* Call read */
    count = HG_Bulk_handle_get_size(bulk_handle);

    buf = HG_TEST_ALLOC(count);

    printf("Calling read with fd: %d\n", fd);
    ret = read(fd, buf, count);

    /* Check bulk buf */
    buf_ptr = (const int*) buf;
    for (i = 0; i < (int)(count / sizeof(int)); i++) {
        if (buf_ptr[i] != i) {
            printf("Error detected after read, buf[%d] = %d, was expecting %d!\n", i, buf_ptr[i], i);
            break;
        }
    }

    /* Create a new block handle to write the data */
    HG_Bulk_handle_create(1, &buf, (size_t *) &ret, HG_BULK_READ_ONLY,
            &bulk_block_handle);

    /* Write bulk data here and wait for the data to be there */
    hg_ret = HG_Bulk_transfer(HG_BULK_PUSH, dest, bulk_handle, 0,
            bulk_block_handle, 0, ret, &bulk_request);
    if (hg_ret != HG_SUCCESS) {
        fprintf(stderr, "Could not write bulk data\n");
        return hg_ret;
    }

    hg_ret = HG_Bulk_wait(bulk_request, HG_MAX_IDLE_TIME, HG_STATUS_IGNORE);
    if (hg_ret != HG_SUCCESS) {
        fprintf(stderr, "Could not complete bulk data write\n");
        return hg_ret;
    }

    /* Fill output structure */
    read_out_struct.ret = ret;

    /* Free handle and send response back */
    hg_ret = HG_Handler_start_output(handle, &read_out_struct);
    if (hg_ret != HG_SUCCESS) {
        fprintf(stderr, "Could not respond\n");
        return hg_ret;
    }

    /* Free block handle */
    hg_ret = HG_Bulk_handle_free(bulk_block_handle);
    if (hg_ret != HG_SUCCESS) {
        fprintf(stderr, "Could not free block call\n");
        return hg_ret;
    }

    free(buf);

    HG_Handler_free_input(handle, &read_in_struct);
    HG_Handler_free(handle);

    return hg_ret;
}

/*---------------------------------------------------------------------------*/
HG_TEST_RPC_CB(hg_test_scale_open, handle)
{
    hg_return_t ret = HG_SUCCESS;

    rpc_open_in_t  rpc_open_in_struct;
    rpc_open_out_t rpc_open_out_struct;

    /* Get input parameters and data */
    ret = HG_Handler_get_input(handle, &rpc_open_in_struct);
    if (ret != HG_SUCCESS) {
        fprintf(stderr, "Could not get input\n");
        return ret;
    }

    /* Free handle and send response back */
    ret = HG_Handler_start_output(handle, &rpc_open_out_struct);
    if (ret != HG_SUCCESS) {
        fprintf(stderr, "Could not start response\n");
        return ret;
    }

    HG_Handler_free_input(handle, &rpc_open_in_struct);
    HG_Handler_free(handle);

    return ret;
}

/*---------------------------------------------------------------------------*/
HG_TEST_RPC_CB(hg_test_scale_write, handle)
{
    hg_return_t ret = HG_SUCCESS;

    bulk_write_in_t  in_struct;
    bulk_write_out_t out_struct;

    na_addr_t source = HG_Handler_get_addr(handle);
    hg_bulk_t bulk_handle = HG_BULK_NULL;
    hg_bulk_t bulk_block_handle;
    hg_bulk_request_t bulk_request;
    size_t nbytes;

    void *buf;
    size_t bulk_write_ret = 0;

    /* Get input parameters and data */
    ret = HG_Handler_get_input(handle, &in_struct);
    if (ret != HG_SUCCESS) {
        fprintf(stderr, "Could not get input\n");
        return ret;
    }

    /* Get parameters */
    bulk_handle = in_struct.bulk_handle;

    /* Create a new block handle to read the data */
    nbytes = HG_Bulk_handle_get_size(bulk_handle);

    buf = HG_TEST_ALLOC(nbytes);

    HG_Bulk_handle_create(1, &buf, &nbytes, HG_BULK_READWRITE,
            &bulk_block_handle);

    ret = HG_Bulk_transfer(HG_BULK_PULL, source, bulk_handle, 0,
            bulk_block_handle, 0, nbytes, &bulk_request);
    if (ret != HG_SUCCESS) {
        fprintf(stderr, "Could not read bulk data\n");
        return ret;
    }

    ret = HG_Bulk_wait(bulk_request, HG_MAX_IDLE_TIME, HG_STATUS_IGNORE);
    if (ret != HG_SUCCESS) {
        fprintf(stderr, "Could not complete bulk data read\n");
        return ret;
    }

    bulk_write(1, buf, 0, nbytes, 0);

    /* Free block handles */
    ret = HG_Bulk_handle_free(bulk_block_handle);
    if (ret != HG_SUCCESS) {
        fprintf(stderr, "Could not free block call\n");
        return ret;
    }

    free(buf);

    bulk_write_ret = nbytes;

    /* Fill output structure */
    out_struct.ret = bulk_write_ret;

    /* Free handle and send response back */
    ret = HG_Handler_start_output(handle, &out_struct);
    if (ret != HG_SUCCESS) {
        fprintf(stderr, "Could not start response\n");
        return ret;
    }

    HG_Handler_free_input(handle, &in_struct);
    HG_Handler_free(handle);

    return ret;
}

/*---------------------------------------------------------------------------*/
HG_TEST_THREAD_CB(hg_test_rpc_open)
HG_TEST_THREAD_CB(hg_test_bulk_write)
HG_TEST_THREAD_CB(hg_test_bulk_seg_write)
HG_TEST_THREAD_CB(hg_test_pipeline_write)
HG_TEST_THREAD_CB(hg_test_posix_open)
HG_TEST_THREAD_CB(hg_test_posix_close)
HG_TEST_THREAD_CB(hg_test_posix_write)
HG_TEST_THREAD_CB(hg_test_posix_read)
HG_TEST_THREAD_CB(hg_test_scale_open)
HG_TEST_THREAD_CB(hg_test_scale_write)

/*---------------------------------------------------------------------------*/
