#include "mercury_thread.h"
#include "mercury_time.h"

#include "mercury_test_config.h"

#include <stdio.h>
#include <stdlib.h>

static HG_THREAD_RETURN_TYPE
thread_cb_incr(void *arg)
{
    hg_thread_ret_t thread_ret = (hg_thread_ret_t) 0;
    int *incr = (int *) arg;

    *incr = 1;

    hg_thread_exit(thread_ret);
    return thread_ret;
}

static HG_THREAD_RETURN_TYPE
thread_cb_sleep(void *arg)
{
    hg_thread_ret_t thread_ret = (hg_thread_ret_t) 0;
    hg_time_t sleep_time = {5, 0};

    (void) arg;
    hg_time_sleep(sleep_time, NULL);
    fprintf(stderr, "Error: did not cancel\n");

    hg_thread_exit(thread_ret);
    return thread_ret;
}

static HG_THREAD_RETURN_TYPE
thread_cb_key(void *arg)
{
    hg_thread_ret_t thread_ret = (hg_thread_ret_t) 0;
    hg_thread_key_t *thread_key = (hg_thread_key_t *) arg;
    int *value_ptr;
    int value = 1;

    hg_thread_setspecific(*thread_key, &value);

    value_ptr = (int *) hg_thread_getspecific(*thread_key);
    if (!value_ptr) {
        fprintf(stderr, "Error: No value associated to key\n");
    }
    if (*value_ptr != 0) {
        fprintf(stderr, "Error: Value is %d\n", *value_ptr);
    }

    hg_thread_exit(thread_ret);
    return thread_ret;
}

int
main(int argc, char *argv[])
{
    hg_thread_t thread;
    hg_thread_key_t thread_key;
    int incr;
    int ret = EXIT_SUCCESS;

    (void) argc;
    (void) argv;

    hg_thread_init(&thread);
    hg_thread_create(&thread, thread_cb_incr, &incr);
    hg_thread_join(thread);

    if (!incr) {
        fprintf(stderr, "Error: Incr is %d\n", incr);
        ret = EXIT_FAILURE;
        goto done;
    }

    hg_thread_create(&thread, thread_cb_sleep, NULL);
    hg_thread_cancel(thread);

    hg_thread_key_create(&thread_key);

    hg_thread_create(&thread, thread_cb_key, &thread_key);
    hg_thread_join(thread);
    hg_thread_key_delete(thread_key);

done:
    return ret;
}
