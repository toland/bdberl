/* -------------------------------------------------------------------
 *
 * bdberl: Berkeley DB Driver for Erlang
 * Copyright (c) 2008 The Hive.  All rights reserved.
 *
 * ------------------------------------------------------------------- */

#include <assert.h>
#include <stdio.h>
#include <stdarg.h>
#include <string.h>
#include <stdint.h>
#include <stdlib.h>
#include <limits.h>
#include <time.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <sys/time.h>
#include <sys/select.h>
#include <sys/statvfs.h>

#include "hive_hash.h"
#include "bdberl_drv.h"
#include "bin_helper.h"

/**
 * Function prototypes
 */
static int open_database(const char* name, DBTYPE type, unsigned int flags, PortData* data, int* dbref_res);
static int close_database(int dbref, unsigned flags, PortData* data);
static int delete_database(const char* name);

static void get_info(int target, void* values, BinHelper* bh);

static void do_async_put(void* arg);
static void do_async_get(void* arg);
static void do_async_txnop(void* arg);
static void do_async_cursor_get(void* arg);
static void do_async_truncate(void* arg);
static void do_async_stat(void* arg);
static void do_async_lock_stat(void* arg);
static void do_async_log_stat(void* arg);
static void do_async_memp_stat(void* arg);
static void do_async_mutex_stat(void* arg);
static void do_async_txn_stat(void* arg);
static void do_sync_data_dirs_info(PortData *p);

static int send_dir_info(ErlDrvPort port, ErlDrvTermData pid, const char *path);
static void send_rc(ErlDrvPort port, ErlDrvTermData pid, int rc);

static int add_dbref(PortData* data, int dbref);
static int del_dbref(PortData* data, int dbref);
static int has_dbref(PortData* data, int dbref);

static int add_portref(int dbref, ErlDrvPort port);
static int del_portref(int dbref, ErlDrvPort port);

static int alloc_dbref();
static char *rc_to_atom_str(int rc);
static void abort_txn(PortData* d);

static void* zalloc(unsigned int size);

static void* deadlock_check(void* arg);
static void* checkpointer(void* arg);

static void bdb_errcall(const DB_ENV* dbenv, const char* errpfx, const char* msg);
static void bdb_msgcall(const DB_ENV* dbenv, const char* msg);
static void send_log_message(ErlDrvTermData* msg, int elements);

/**
 * Global instance of DB_ENV; only a single one exists per O/S process.
 */
static DB_ENV* G_DB_ENV = 0;


/**
 * Global variable to track the return code from opening the DB_ENV. We track this
 * value so as to provide a useful error code when the user attempts to open the
 * port and it fails due to an error that occurred when opening the environment.
 */
static int G_DB_ENV_ERROR = 0;


/**
 * G_DATABASES is a global array of Database structs. Used to track currently opened DB*
 * handles and ensure that they get cleaned up when all ports which were using them exit or
 * explicitly close them.
 * 
 * This array is allocated when the driver is first initialized and does not grow/shrink
 * dynamically. G_DATABASES_SIZE contains the size of the array. G_DATABASES_NAMES is a hash of
 * filenames to array index for an opened Database. 
 *
 * All access to G_DATABASES and G_DATABASES_NAMES must be protected by the read/write lock
 * G_DATABASES_RWLOCK.
 */
static Database*     G_DATABASES        = 0;
static int           G_DATABASES_SIZE   = 0;
static ErlDrvRWLock* G_DATABASES_RWLOCK = 0;
static hive_hash*    G_DATABASES_NAMES  = 0;


/**
 * Deadlock detector thread variables. We run a single thread per VM to detect deadlocks within
 * our global environment. G_DEADLOCK_CHECK_INTERVAL is the time between runs in milliseconds.
 */
#define DEFAULT_DEADLOCK_CHECK_INTERVAL  100  /* 100 milliseconds */

static ErlDrvTid    G_DEADLOCK_THREAD         = 0;
static unsigned int G_DEADLOCK_CHECK_ACTIVE   = 1;
static unsigned int G_DEADLOCK_CHECK_INTERVAL = DEFAULT_DEADLOCK_CHECK_INTERVAL; 


/**
 * Trickle writer for dirty pages. We run a single thread per VM to perform background
 * trickling of dirty pages to disk. G_TRICKLE_INTERVAL is the time between runs in seconds.
 */
static unsigned int G_TRICKLE_ACTIVE     = 1;
static unsigned int G_TRICKLE_INTERVAL   = 60 * 5;      /* Seconds between trickle writes */
static unsigned int G_TRICKLE_PERCENTAGE = 50;          /* Desired % of clean pages in cache */


/**
 * Transaction checkpoint monitor. We run a single thread per VM to flush transaction
 * logs into the backing data store. G_CHECKPOINT_INTERVAL is the time between runs in seconds.
 * TODO The interval should be configurable.
 */
static ErlDrvTid    G_CHECKPOINT_THREAD   = 0;
static unsigned int G_CHECKPOINT_ACTIVE   = 1;
static unsigned int G_CHECKPOINT_INTERVAL = 60 * 60; /* Seconds between checkpoints */

/**
 * Pipe to used to wake up the various monitors.  Instead of just sleeping
 * they wait for an exceptional condition on the read fd of the pipe.  When it is time to 
 * shutdown, the driver closes the write fd and waits for the threads to be joined.
 */
static int G_BDBERL_PIPE[2] = {-1, -1};

/**
 * Lock, port and pid reference for relaying BDB output into the SASL logger. READ lock
 * is required to log data. WRITE lock is used when replacing the pid/port reference. If
 * no pid/port is available, no callback is registered with BDB. 
 */
static ErlDrvRWLock*  G_LOG_RWLOCK = 0;
static ErlDrvTermData G_LOG_PID;
static ErlDrvPort     G_LOG_PORT;

/**
 * Default page size to use for newly created databases
 */
static unsigned int G_PAGE_SIZE = 0;

/** Thread pools
 *
 */
#define DEFAULT_NUM_GENERAL_THREADS 10
#define DEFAULT_NUM_TXN_THREADS     10

static int G_NUM_GENERAL_THREADS = DEFAULT_NUM_GENERAL_THREADS;
static int G_NUM_TXN_THREADS = DEFAULT_NUM_TXN_THREADS;
static TPool* G_TPOOL_GENERAL = NULL;
static TPool* G_TPOOL_TXNS    = NULL;


/**
 * Helpful macros
 */
#define READ_LOCK(L) erl_drv_rwlock_rlock(L)
#define READ_UNLOCK(L) erl_drv_rwlock_runlock(L)
#define PROMOTE_READ_LOCK(L) erl_drv_rwlock_runlock(L); erl_drv_rwlock_rwlock(L)
#define WRITE_LOCK(L) erl_drv_rwlock_rwlock(L)
#define WRITE_UNLOCK(L) erl_drv_rwlock_rwunlock(L)

#define UNPACK_BYTE(_buf, _off) (_buf[_off])
#define UNPACK_INT(_buf, _off) *((int*)(_buf+(_off)))
#define UNPACK_STRING(_buf, _off) (char*)(_buf+(_off))
#define UNPACK_BLOB(_buf, _off) (void*)(_buf+(_off))

#define RETURN_BH(bh, outbuf) *outbuf = (char*)bh.bin; return bh.offset;

#define RETURN_INT(val, outbuf) {             \
        BinHelper bh;                         \
        bin_helper_init(&bh);                 \
        bin_helper_push_int32(&bh, val);      \
        RETURN_BH(bh, outbuf); }

#define FAIL_IF_ASYNC_PENDING(d, outbuf) {              \
    erl_drv_mutex_lock(d->port_lock);                   \
    if (d->async_op != CMD_NONE) {                      \
        erl_drv_mutex_unlock(d->port_lock);             \
        RETURN_INT(ERROR_ASYNC_PENDING, outbuf);        \
    } else {                                            \
        erl_drv_mutex_unlock(d->port_lock);             \
    }}

#define FAIL_IF_CURSOR_OPEN(d, outbuf) {                        \
    if (NULL != d->cursor)                                      \
    {                                                           \
        send_rc(d->port, d->port_owner, ERROR_CURSOR_OPEN);     \
        RETURN_INT(0, outbuf);                                  \
    }}
#define FAIL_IF_NO_CURSOR(d, outbuf) {                          \
    if (NULL == d->cursor)                                      \
    {                                                           \
        send_rc(d->port, d->port_owner, ERROR_NO_CURSOR);       \
        RETURN_INT(0, outbuf);                                  \
    }}

#define FAIL_IF_TXN_OPEN(d, outbuf) {                           \
        if (NULL != d->txn)                                     \
    {                                                           \
        send_rc(d->port, d->port_owner, ERROR_TXN_OPEN);        \
        RETURN_INT(0, outbuf);                                  \
    }}
#define FAIL_IF_NO_TXN(d, outbuf) {                             \
        if (NULL == d->txn)                                     \
    {                                                           \
        send_rc(d->port, d->port_owner, ERROR_NO_TXN);          \
        RETURN_INT(0, outbuf);                                  \
    }}

#ifdef DEBUG
#  define DBG(...) fprintf(stderr, __VA_ARGS__)
static void DBGCMD(PortData *d, const char *fmt, ...)
{
    char buf[1024];

    va_list ap;
    va_start(ap, fmt);
    vsnprintf(buf, sizeof(buf), fmt, ap);
    va_end(ap);

    (void)fprintf(stderr, "threadid %p port %p: %s\r\n", erl_drv_thread_self(), d->port, buf);
}
static void DBGCMDRC(PortData *d, int rc)
{
    (void)fprintf(stderr, "threadid %p port %p: rc = %s (%d)\r\n",
                  erl_drv_thread_self(), d->port, rc == 0 ? "ok" : rc_to_atom_str(rc), rc);
}

#else
#  define DBG(arg1,...)
#  define DBGCMD(d, fmt, ...)
#  define DBGCMDRC(d, rc) { while (0) { rc++; } }  // otherwise get unused variable error
#endif


DRIVER_INIT(bdberl_drv) 
{
    DBG("DRIVER INIT\r\n");
    // Setup flags we'll use to init the environment
    int flags = 
        DB_INIT_LOCK |          /* Enable support for locking */
        DB_INIT_TXN |           /* Enable support for transactions */
        DB_INIT_MPOOL |         /* Enable support for memory pools */
        DB_RECOVER |            /* Enable support for recovering from failures */
        DB_CREATE |             /* Create files as necessary */
        DB_REGISTER |           /* Run recovery if needed */
        DB_USE_ENVIRON |        /* Use DB_HOME environment variable */
        DB_THREAD;              /* Make the environment free-threaded */

    // Check for environment flag which indicates we want to use DB_SYSTEM_MEM
    char value[1];
    size_t value_size = sizeof(value);
    if (erl_drv_getenv("BDBERL_SYSTEM_MEM", value, &value_size) >= 0)
    {
        flags |= DB_SYSTEM_MEM;
    }

    // Initialize global environment -- use environment variable DB_HOME to 
    // specify where the working directory is
    DBG("db_env_create(%p, 0)", &G_DB_ENV);
    G_DB_ENV_ERROR = db_env_create(&G_DB_ENV, 0); 
    DBG(" = %d\r\n", G_DB_ENV_ERROR);
    if (G_DB_ENV_ERROR != 0)
    {
        G_DB_ENV = 0;
    }
    else
    {
        DBG("G_DB_ENV->open(%p, 0, %08X, 0);", &G_DB_ENV, flags);
        G_DB_ENV_ERROR = G_DB_ENV->open(G_DB_ENV, 0, flags, 0);
        DBG(" = %d\r\n", G_DB_ENV_ERROR);
        if (G_DB_ENV_ERROR != 0)
        {
            // Something bad happened while initializing BDB; in this situation we 
            // cleanup and set the environment to zero. Attempts to open ports will
            // fail and the user will have to sort out how to resolve the issue.
            DBG("G_DB_ENV->close(%p, 0);\r\n", &G_DB_ENV);
            G_DB_ENV->close(G_DB_ENV, 0);
            G_DB_ENV = 0;
        }
    }

    if (G_DB_ENV_ERROR == 0)
    {
        // Pipe for signalling the utility threads all is over.
        assert(0 == pipe(G_BDBERL_PIPE));

        // Use the BDBERL_MAX_DBS environment value to determine the max # of
        // databases to permit the VM to open at once. Defaults to 1024.
        G_DATABASES_SIZE = 1024;
        char max_dbs_str[64];
        value_size = sizeof(max_dbs_str);
        if (erl_drv_getenv("BDBERL_MAX_DBS", max_dbs_str, &value_size) >= 0)
        {
            assert(value_size < sizeof(max_dbs_str));
            G_DATABASES_SIZE = atoi(max_dbs_str);
            if (G_DATABASES_SIZE <= 0)
            {
                G_DATABASES_SIZE = 1024;
            }
        }

        // Use the BDBERL_TRICKLE_TIME and BDBERL_TRICKLE_PERCENTAGE to control how often
        // the trickle writer runs and what percentage of pages should be flushed.
        char trickle_time_str[64];
        value_size = sizeof(trickle_time_str);
        if (erl_drv_getenv("BDBERL_TRICKLE_TIME", trickle_time_str, &value_size) >= 0)
        {
            assert(value_size < sizeof(trickle_time_str));
            G_TRICKLE_INTERVAL = atoi(trickle_time_str);
            if (G_TRICKLE_INTERVAL <= 0)
            {
                G_TRICKLE_INTERVAL = 60 * 5;
            }
        }

        char trickle_percentage_str[64];
        value_size = sizeof(trickle_percentage_str);
        if (erl_drv_getenv("BDBERL_TRICKLE_PERCENTAGE", trickle_percentage_str, &value_size) >= 0)
        {
            assert(value_size < sizeof(trickle_percentage_str));

            G_TRICKLE_PERCENTAGE = atoi(trickle_percentage_str);
            if (G_TRICKLE_PERCENTAGE <= 0)
            {
                G_TRICKLE_PERCENTAGE = 50;
            }
        }

        // Set the deadlock interval
        char deadlock_check_interval_str[64];
        value_size = sizeof(deadlock_check_interval_str);
        if (erl_drv_getenv("BDBERL_DEADLOCK_CHECK_INTERVAL", 
                           deadlock_check_interval_str, &value_size) >= 0)
        {
            assert(value_size < sizeof(deadlock_check_interval_str));

            G_DEADLOCK_CHECK_INTERVAL = atoi(deadlock_check_interval_str);
            if (G_DEADLOCK_CHECK_INTERVAL < 0)
            {
                G_DEADLOCK_CHECK_INTERVAL = DEFAULT_DEADLOCK_CHECK_INTERVAL;
            }

            fprintf(stderr, "Deadlock check interval set to %d\r\n", G_DEADLOCK_CHECK_INTERVAL);
        }


        // Initialize default page size
        char page_size_str[64];
        value_size = sizeof(page_size_str);
        if (erl_drv_getenv("BDBERL_PAGE_SIZE", page_size_str, &value_size) >= 0)
        {
            assert(value_size < sizeof(page_size_str));

            // Convert to integer and only set it if it is a power of 2.
            unsigned int page_size = atoi(page_size_str);
            if (page_size != 0 && ((page_size & (~page_size +1)) == page_size))
            {
                G_PAGE_SIZE = page_size;
            }
        }

        // Make sure we can distiguish between lock timeouts and deadlocks
        G_DB_ENV->set_flags(G_DB_ENV, DB_TIME_NOTGRANTED, 1);

        // Initialization transaction timeout so that deadlock checking works properly
        db_timeout_t to = 500 * 1000; // 500 ms
        G_DB_ENV->set_timeout(G_DB_ENV, to, DB_SET_TXN_TIMEOUT);

        // BDB is setup -- allocate structures for tracking databases
        G_DATABASES = (Database*) driver_alloc(sizeof(Database) * G_DATABASES_SIZE);
        memset(G_DATABASES, '\0', sizeof(Database) * G_DATABASES_SIZE);
        G_DATABASES_RWLOCK = erl_drv_rwlock_create("bdberl_drv: G_DATABASES_RWLOCK");
        G_DATABASES_NAMES = hive_hash_new(G_DATABASES_SIZE);            

        // Startup deadlock check thread
        erl_drv_thread_create("bdberl_drv_deadlock_checker", &G_DEADLOCK_THREAD, 
                              &deadlock_check, 0, 0);

        // Use the BDBERL_CHECKPOINT_TIME environment value to determine the
        // interval between transaction checkpoints. Defaults to 1 hour.
        char cp_int_str[64];
        value_size = sizeof(cp_int_str);
        if (erl_drv_getenv("BDBERL_CHECKPOINT_TIME", cp_int_str, &value_size) >= 0)
        {
            assert(value_size < sizeof(cp_int_str));

            G_CHECKPOINT_INTERVAL = atoi(cp_int_str);
            if (G_CHECKPOINT_INTERVAL <= 0)
            {
                G_CHECKPOINT_INTERVAL = 60 * 60;
            }
        }

        // Startup checkpoint thread
        erl_drv_thread_create("bdberl_drv_checkpointer", &G_CHECKPOINT_THREAD,
                              &checkpointer, 0, 0);

        // Startup our thread pools
        char num_general_threads_str[64];
        value_size = sizeof(num_general_threads_str);
        if (erl_drv_getenv("BDBERL_NUM_GENERAL_THREADS", num_general_threads_str, &value_size) >= 0)
        {
            assert(value_size < sizeof(num_general_threads_str));

            G_NUM_GENERAL_THREADS = atoi(num_general_threads_str);
            if (G_NUM_GENERAL_THREADS <= 0)
            {
                G_NUM_GENERAL_THREADS = DEFAULT_NUM_GENERAL_THREADS;
            }
        }
        G_TPOOL_GENERAL = bdberl_tpool_start(G_NUM_GENERAL_THREADS);

        char num_txn_threads_str[64];
        value_size = sizeof(num_txn_threads_str);
        if (erl_drv_getenv("BDBERL_NUM_TXN_THREADS", num_txn_threads_str, &value_size) >= 0)
        {
            assert(value_size < sizeof(num_txn_threads_str));

            G_NUM_TXN_THREADS = atoi(num_txn_threads_str);
            if (G_NUM_TXN_THREADS <= 0)
            {
                G_NUM_TXN_THREADS = DEFAULT_NUM_TXN_THREADS;
            }
        }
        G_TPOOL_TXNS    = bdberl_tpool_start(G_NUM_TXN_THREADS);

        // Initialize logging lock and refs
        G_LOG_RWLOCK = erl_drv_rwlock_create("bdberl_drv: G_LOG_RWLOCK");
        G_LOG_PORT   = 0;
        G_LOG_PID    = 0;
    }
    else
    {
        DBG("DRIVER INIT FAILED - %s\r\n", db_strerror(G_DB_ENV_ERROR));
    }

    return &bdberl_drv_entry;
}

static ErlDrvData bdberl_drv_start(ErlDrvPort port, char* buffer)
{
    DBG("threadid %p port %p: BDB DRIVER STARTING\r\n", 
        erl_drv_thread_self(), port);

    // Make sure we have a functional environment -- if we don't,
    // bail...
    if (!G_DB_ENV)
    {
        return ERL_DRV_ERROR_BADARG;
    }
    
    PortData* d = (PortData*)driver_alloc(sizeof(PortData));
    memset(d, '\0', sizeof(PortData));

    // Save handle to the port
    d->port = port;

    // Allocate a mutex for the port
    d->port_lock = erl_drv_mutex_create("bdberl_port_lock");

    // Save the caller/owner PID
    d->port_owner = driver_connected(port);

    // Allocate an initial buffer for work purposes
    d->work_buffer = driver_alloc(4096);
    d->work_buffer_sz = 4096;

    // Make sure port is running in binary mode
    set_port_control_flags(port, PORT_CONTROL_FLAG_BINARY);

    DBGCMD(d, "BDB DRIVER STARTED");

    return (ErlDrvData)d;
}

static void bdberl_drv_stop(ErlDrvData handle)
{
    PortData* d = (PortData*)handle;

    DBG("Stopping port %p\r\n", d->port);

    // Grab the port lock, in case we have an async job running
    erl_drv_mutex_lock(d->port_lock);

    // If there is an async job pending, we need to cancel it. The cancel operation will
    // block until the job has either been removed or has run
    if (d->async_job)
    {
        // Drop the lock prior to starting the wait for the async process
        erl_drv_mutex_unlock(d->port_lock);

        DBG("Cancelling async job for port: %p\r\n", d->port);
        bdberl_tpool_cancel(d->async_pool, d->async_job);
        DBG("Canceled async job for port: %p\r\n", d->port);
    }
    else
    {
        // If there was no async job, drop the lock -- not needed
        erl_drv_mutex_unlock(d->port_lock);
    }
    
    // Cleanup the port lock
    erl_drv_mutex_destroy(d->port_lock);

    // If a cursor is open, close it
    if (d->cursor)
    {
        d->cursor->close(d->cursor);
    }

    // If a txn is currently active, terminate it. 
    abort_txn(d);

    // Close all the databases we previously opened
    while (d->dbrefs)
    {
        close_database(d->dbrefs->dbref, 0, d);
    }

    // If this port was registered as the endpoint for logging, go ahead and 
    // remove it. Note that we don't need to lock to check this since we only
    // unregister if it's already initialized to this port.
    if (G_LOG_PORT == d->port)
    {
        WRITE_LOCK(G_LOG_RWLOCK);

        // Remove the references
        G_LOG_PORT = 0;
        G_LOG_PID  = 0;
        
        // Unregister with BDB -- MUST DO THIS WITH WRITE LOCK HELD!
        G_DB_ENV->set_msgcall(G_DB_ENV, 0);
        G_DB_ENV->set_errcall(G_DB_ENV, 0);

        WRITE_UNLOCK(G_LOG_RWLOCK);
    }

    DBG("Stopped port: %p\r\n", d->port);
    
    // Release the port instance data
    driver_free(d->work_buffer);
    driver_free(handle);
}

static void bdberl_drv_finish()
{
    DBG("BDB DRIVER FINISHING\r\n");
    // Stop the thread pools
    if (NULL != G_TPOOL_GENERAL)
    {
        bdberl_tpool_stop(G_TPOOL_GENERAL);
        G_TPOOL_GENERAL = NULL;
    }

    if (NULL != G_TPOOL_TXNS)
    {
        bdberl_tpool_stop(G_TPOOL_TXNS);
        G_TPOOL_TXNS = NULL;
    }

    // Signal the utility threads time is up
    G_TRICKLE_ACTIVE = 0;
    G_DEADLOCK_CHECK_ACTIVE = 0;
    G_CHECKPOINT_ACTIVE = 0;

    // Close the writer fd on the pipe to signal finish to the utility threads
    if (-1 != G_BDBERL_PIPE[1])
    {
        close(G_BDBERL_PIPE[1]);
        G_BDBERL_PIPE[1] = -1;
    }

    // Wait for the deadlock checker to shutdown -- then wait for it
    if (0 != G_DEADLOCK_THREAD)
    {
        erl_drv_thread_join(G_DEADLOCK_THREAD, 0);
        G_DEADLOCK_THREAD = 0;
    }

    // Wait for the checkpointer to shutdown -- then wait for it
    if (0 != G_CHECKPOINT_THREAD)
    {
        erl_drv_thread_join(G_CHECKPOINT_THREAD, 0);
        G_CHECKPOINT_THREAD = 0;
    }

    // Close the reader fd on the pipe now utility threads are closed
    if (-1 != G_BDBERL_PIPE[0])
    {
        close(G_BDBERL_PIPE[0]);
    }
    G_BDBERL_PIPE[0] = -1;

    // Cleanup and shut down the BDB environment. Note that we assume
    // all ports have been released and thuse all databases/txns/etc are also gone.
    if (NULL != G_DB_ENV)
    {
        G_DB_ENV->close(G_DB_ENV, 0);
        G_DB_ENV = NULL;
    }
    if (NULL != G_DATABASES)
    {
        driver_free(G_DATABASES);
        G_DATABASES = NULL;
    }
    if (NULL != G_DATABASES_RWLOCK)
    {
        erl_drv_rwlock_destroy(G_DATABASES_RWLOCK);
        G_DATABASES_RWLOCK = NULL;
    }

    if (NULL != G_DATABASES_NAMES)
    {
        hive_hash_destroy(G_DATABASES_NAMES);
        G_DATABASES_NAMES = NULL;
    }
    
    // Release the logging rwlock
    if (NULL != G_LOG_RWLOCK)
    {
        erl_drv_rwlock_destroy(G_LOG_RWLOCK);
        G_LOG_RWLOCK = NULL;
    }

    DBG("BDB DRIVER FINISHED\r\n");
}

static int bdberl_drv_control(ErlDrvData handle, unsigned int cmd, 
                              char* inbuf, int inbuf_sz, 
                              char** outbuf, int outbuf_sz)
{
    PortData* d = (PortData*)handle;
    switch(cmd)        
    {
    case CMD_OPEN_DB:
    {
        // Extract the type code and filename from the inbuf
        // Inbuf is: <<Flags:32/unsigned, Type:8, Name/bytes, 0:8>>
        unsigned flags = UNPACK_INT(inbuf, 0);
        DBTYPE type = (DBTYPE) UNPACK_BYTE(inbuf, 4);
        char* name = UNPACK_STRING(inbuf, 5);
        int dbref;
        int rc = open_database(name, type, flags, d, &dbref);

        // Queue up a message for bdberl:open to process
        if (rc == 0) // success: send {ok, DbRef}
        {
            ErlDrvTermData response[] = { ERL_DRV_ATOM, driver_mk_atom("ok"),
                                          ERL_DRV_INT,  dbref,
                                          ERL_DRV_TUPLE, 2};
            driver_send_term(d->port, d->port_owner,
                             response, sizeof(response) / sizeof(response[0]));
        }
        else // failure: send {error, atom() | {error, {unknown, Rc}}
        {
            send_rc(d->port, d->port_owner, rc);
        }
        // Outbuf is: <<Rc:32>> - always send 0 and the driver will receive the real value
        RETURN_INT(0, outbuf);
    }
    case CMD_CLOSE_DB:
    {
        FAIL_IF_ASYNC_PENDING(d, outbuf);
        FAIL_IF_CURSOR_OPEN(d, outbuf);
        FAIL_IF_TXN_OPEN(d, outbuf);

        // Take the provided dbref and attempt to close it
        // Inbuf is: <<DbRef:32, Flags:32/unsigned>>
        int dbref = UNPACK_INT(inbuf, 0);
        unsigned flags = (unsigned) UNPACK_INT(inbuf, 4);

        int rc = close_database(dbref, flags, d);

        // Queue up a message for bdberl:close to process
        send_rc(d->port, d->port_owner, rc);
        // Outbuf is: <<Rc:32>> - always send 0 and the driver will receive the real value
        RETURN_INT(0, outbuf);
    }
    case CMD_TXN_BEGIN:
    {
        FAIL_IF_ASYNC_PENDING(d, outbuf);
        FAIL_IF_TXN_OPEN(d, outbuf);

        // Setup async command and schedule it on the txns threadpool
        d->async_op = cmd;
        d->async_flags = UNPACK_INT(inbuf, 0);
        d->async_pool = G_TPOOL_TXNS;
        bdberl_tpool_run(d->async_pool, &do_async_txnop, d, 0, &d->async_job);

        // Outbuf is <<Rc:32>>
        RETURN_INT(0, outbuf);
    }
    case CMD_TXN_COMMIT:
    case CMD_TXN_ABORT:
    {
        FAIL_IF_ASYNC_PENDING(d, outbuf);
        FAIL_IF_NO_TXN(d, outbuf);

        // Setup async command and schedule it on the txns threadpool
        d->async_op = cmd;
        if (cmd == CMD_TXN_COMMIT)
        {
            d->async_flags = UNPACK_INT(inbuf, 0);
        }
        d->async_pool = G_TPOOL_TXNS;
        bdberl_tpool_run(d->async_pool, &do_async_txnop, d, 0, &d->async_job);

        // Outbuf is <<Rc:32>>
        RETURN_INT(0, outbuf);
    }
    case CMD_PUT:
    case CMD_GET:
    case CMD_PUT_COMMIT:
    {
        FAIL_IF_ASYNC_PENDING(d, outbuf);

        // Put/commit requires a transaction to be active
        if (cmd == CMD_PUT_COMMIT && (!d->txn))
        {
            send_rc(d->port, d->port_owner, ERROR_NO_TXN);
            RETURN_INT(0, outbuf);
        }

        // Inbuf is: << DbRef:32, Rest/binary>>
        int dbref = UNPACK_INT(inbuf, 0);

        // Make sure this port currently has dbref open -- if it doesn't, error out. Of note,
        // if it's in our list, we don't need to grab the RWLOCK, as we don't have to worry about
        // the underlying handle disappearing since we have a reference.
        if (has_dbref(d, dbref))
        {
            // If the working buffer is large enough, copy the data to put/get into it. Otherwise, realloc
            // until it is large enough
            if (d->work_buffer_sz < inbuf_sz)
            {
                d->work_buffer = driver_realloc(d->work_buffer, inbuf_sz);
                d->work_buffer_sz = inbuf_sz;
            }

            // Copy the payload into place
            memcpy(d->work_buffer, inbuf, inbuf_sz);
            d->work_buffer_offset = inbuf_sz;

            // Mark the port as busy and then schedule the appropriate async operation
            d->async_op = cmd;
            TPoolJobFunc fn;
            if (cmd == CMD_PUT || cmd == CMD_PUT_COMMIT)
            {
                fn = &do_async_put;
            }
            else 
            {
                assert(cmd == CMD_GET);
                fn = &do_async_get;
            }
            d->async_pool = G_TPOOL_GENERAL;
            bdberl_tpool_run(d->async_pool, fn, d, 0, &d->async_job);
        
            // Let caller know that the operation is in progress
            // Outbuf is: <<0:32>>
            RETURN_INT(0, outbuf);
        }
        else
        {
            // Invalid dbref
            send_rc(d->port, d->port_owner, ERROR_INVALID_DBREF);
            RETURN_INT(0, outbuf);
        }
    }        
    case CMD_GETINFO:
    {
        // Inbuf is: << Target:32, Values/binary >>
        int target = UNPACK_INT(inbuf, 0);
        char* values = UNPACK_BLOB(inbuf, 4);

        // Execute the tuning -- the result to send back to the caller is wrapped
        // up in the provided binhelper
        BinHelper bh;
        get_info(target, values, &bh);
        RETURN_BH(bh, outbuf);
    }
    case CMD_CURSOR_OPEN:
    {
        FAIL_IF_ASYNC_PENDING(d, outbuf);        
        FAIL_IF_CURSOR_OPEN(d, outbuf);

        // Inbuf is << DbRef:32, Flags:32 >>
        int dbref = UNPACK_INT(inbuf, 0);
        unsigned int flags = UNPACK_INT(inbuf, 4);

        // Make sure we have a reference to the requested database
        if (has_dbref(d, dbref))
        {
            // Grab the database handle and open the cursor
            DB* db = G_DATABASES[dbref].db;
            int rc = db->cursor(db, d->txn, &(d->cursor), flags);
            send_rc(d->port, d->port_owner, rc);
            RETURN_INT(0, outbuf);
        }
        else
        {
            send_rc(d->port, d->port_owner, ERROR_INVALID_DBREF);
            RETURN_INT(0, outbuf);
        }
    }
    case CMD_CURSOR_CURR:
    case CMD_CURSOR_NEXT:
    case CMD_CURSOR_PREV:
    {
        FAIL_IF_ASYNC_PENDING(d, outbuf);
        FAIL_IF_NO_CURSOR(d, outbuf);
        
        // Schedule the operation
        d->async_op = cmd;
        d->async_pool = G_TPOOL_GENERAL;
        bdberl_tpool_run(d->async_pool, &do_async_cursor_get, d, 0, &d->async_job);

        // Let caller know operation is in progress
        RETURN_INT(0, outbuf);
    }
    case CMD_CURSOR_CLOSE:
    {
        FAIL_IF_ASYNC_PENDING(d, outbuf);
        FAIL_IF_NO_CURSOR(d, outbuf);

        // It's possible to get a deadlock when closing a cursor -- in that situation we also
        // need to go ahead and abort the txn
        int rc = d->cursor->close(d->cursor);
        if (d->txn && (rc == DB_LOCK_NOTGRANTED || rc == DB_LOCK_DEADLOCK))
        {
            abort_txn(d);
        }

        // Regardless of what happens, clear out the cursor pointer
        d->cursor = 0;
        
        // Send result code
        send_rc(d->port, d->port_owner, rc);
        RETURN_INT(0, outbuf);
    }
    case CMD_REMOVE_DB:
    {
        FAIL_IF_ASYNC_PENDING(d, outbuf);
        FAIL_IF_TXN_OPEN(d, outbuf);

        // Inbuf is: << dbname/bytes, 0:8 >>
        const char* dbname = UNPACK_STRING(inbuf, 0);
        int rc = delete_database(dbname);
        send_rc(d->port, d->port_owner, rc);
        RETURN_INT(0, outbuf);
    }
    case CMD_TRUNCATE:
    {
        FAIL_IF_ASYNC_PENDING(d, outbuf);
        FAIL_IF_CURSOR_OPEN(d, outbuf);

        // Inbuf is: <<DbRef:32>>
        int dbref = UNPACK_INT(inbuf, 0);

        // Make sure this port currently has dbref open -- if it doesn't, error out. Of note,
        // if it's in our list, we don't need to grab the RWLOCK, as we don't have to worry about
        // the underlying handle disappearing since we have a reference.
        if (dbref == -1 || has_dbref(d, dbref))
        {
            // Mark the port as busy and then schedule the appropriate async operation
            d->async_op = cmd;
            d->async_pool = G_TPOOL_GENERAL;
            d->async_dbref = dbref;
            bdberl_tpool_run(d->async_pool, &do_async_truncate, d, 0, &d->async_job);

            // Let caller know that the operation is in progress
            // Outbuf is: <<0:32>>
            RETURN_INT(0, outbuf);
        }
        else
        {
            // Invalid dbref
            RETURN_INT(ERROR_INVALID_DBREF, outbuf);
        }
    }
    case CMD_REGISTER_LOGGER:
    {
        // If this port is not the current logger, make it so. Only one logger can be registered
        // at a time. 
        if (G_LOG_PORT != d->port)
        {
            // Grab the write lock and update the global vars; also make sure to update BDB callbacks
            // within the write lock to avoid race conditions.
            WRITE_LOCK(G_LOG_RWLOCK);
            
            G_LOG_PORT = d->port;
            G_LOG_PID  = driver_connected(d->port);

            G_DB_ENV->set_msgcall(G_DB_ENV, &bdb_msgcall);
            G_DB_ENV->set_errcall(G_DB_ENV, &bdb_errcall);

            WRITE_UNLOCK(G_LOG_RWLOCK);
        }
        *outbuf = 0;
        return 0;
    }
    case CMD_DB_STAT:
    {
        FAIL_IF_ASYNC_PENDING(d, outbuf);

        // Inbuf is << DbRef:32, Flags:32 >>
        int dbref = UNPACK_INT(inbuf, 0);

        // Make sure this port currently has dbref open -- if it doesn't, error out. Of note,
        // if it's in our list, we don't need to grab the RWLOCK, as we don't have to worry about
        // the underlying handle disappearing since we have a reference.
        if (has_dbref(d, dbref))
        {
            // Mark the port as busy and then schedule the appropriate async operation
            d->async_dbref = dbref;
            d->async_op = cmd;
            d->async_pool = G_TPOOL_GENERAL;
            d->async_flags = UNPACK_INT(inbuf, 4);
            bdberl_tpool_run(d->async_pool, &do_async_stat, d, 0, &d->async_job);

            // Let caller know that the operation is in progress
            // Outbuf is: <<0:32>>
            RETURN_INT(0, outbuf);
        }
        else
        {
            // Invalid dbref
            RETURN_INT(ERROR_INVALID_DBREF, outbuf);
        }
    }
    case CMD_DB_STAT_PRINT:
    {
        FAIL_IF_ASYNC_PENDING(d, outbuf);

        // Inbuf is << DbRef:32, Flags:32 >>
        int dbref = UNPACK_INT(inbuf, 0);

        // Make sure this port currently has dbref open -- if it doesn't, error out. Of note,
        // if it's in our list, we don't need to grab the RWLOCK, as we don't have to worry about
        // the underlying handle disappearing since we have a reference.
        if (has_dbref(d, dbref))
        {
            DB* db = G_DATABASES[dbref].db;
            unsigned int flags = UNPACK_INT(inbuf, 4);
            
            // Outbuf is <<Rc:32>>
            // Run the command on the VM thread - this is for debugging only,
            // any real monitoring 
            int rc = db->stat_print(db, flags);
            RETURN_INT(rc, outbuf);
        }
        else
        {
            // Invalid dbref
            RETURN_INT(ERROR_INVALID_DBREF, outbuf);
        }
    }
    case CMD_ENV_STAT_PRINT:
    {
        FAIL_IF_ASYNC_PENDING(d, outbuf);

        // Inbuf is << Flags:32 >>
        unsigned int flags = UNPACK_INT(inbuf, 0);
            
        // Outbuf is <<Rc:32>>
        int rc = G_DB_ENV->stat_print(G_DB_ENV, flags);
        RETURN_INT(rc, outbuf);
    }
    case CMD_LOCK_STAT:
    {
        FAIL_IF_ASYNC_PENDING(d, outbuf);

        // Mark the port as busy and then schedule the appropriate async operation
        d->async_op = cmd;
        d->async_pool = G_TPOOL_GENERAL;
        d->async_flags = UNPACK_INT(inbuf, 0);
        bdberl_tpool_run(d->async_pool, &do_async_lock_stat, d, 0, &d->async_job);
        
        // Let caller know that the operation is in progress
        // Outbuf is: <<0:32>>
        RETURN_INT(0, outbuf);
    }
    case CMD_LOCK_STAT_PRINT:
    {
        FAIL_IF_ASYNC_PENDING(d, outbuf);

        // Inbuf is << Flags:32 >>
        unsigned int flags = UNPACK_INT(inbuf, 0);
            
        // Outbuf is <<Rc:32>>
        // Run the command on the VM thread - this is for debugging only,
        // any real monitoring will use the async lock_stat 
        int rc = G_DB_ENV->lock_stat_print(G_DB_ENV, flags);
        RETURN_INT(rc, outbuf);
    }
    case CMD_LOG_STAT:
    {
        FAIL_IF_ASYNC_PENDING(d, outbuf);

        // Inbuf is <<Flags:32 >>

        // Mark the port as busy and then schedule the appropriate async operation
        d->async_op = cmd;
        d->async_pool = G_TPOOL_GENERAL;
        d->async_flags = UNPACK_INT(inbuf, 0);
        bdberl_tpool_run(d->async_pool, &do_async_log_stat, d, 0, &d->async_job);
        
        // Let caller know that the operation is in progress
        // Outbuf is: <<0:32>>
        RETURN_INT(0, outbuf);
    }
    case CMD_LOG_STAT_PRINT:
    {
        FAIL_IF_ASYNC_PENDING(d, outbuf);

        // Inbuf is << Flags:32 >>
        unsigned int flags = UNPACK_INT(inbuf, 0);
            
        // Outbuf is <<Rc:32>>
        // Run the command on the VM thread - this is for debugging only,
        // any real monitoring will use the async lock_stat 
        int rc = G_DB_ENV->log_stat_print(G_DB_ENV, flags);
        RETURN_INT(rc, outbuf);
    }
    case CMD_MEMP_STAT:
    {
        FAIL_IF_ASYNC_PENDING(d, outbuf);

        // Inbuf is <<Flags:32 >>
      
        // Mark the port as busy and then schedule the appropriate async operation
        d->async_op = cmd;
        d->async_pool = G_TPOOL_GENERAL;
        d->async_flags = UNPACK_INT(inbuf, 0);
        bdberl_tpool_run(d->async_pool, &do_async_memp_stat, d, 0, &d->async_job);
        
        // Let caller know that the operation is in progress
        // Outbuf is: <<0:32>>
        RETURN_INT(0, outbuf);
    }
    case CMD_MEMP_STAT_PRINT:
    {
        FAIL_IF_ASYNC_PENDING(d, outbuf);

        // Inbuf is << Flags:32 >>
        unsigned int flags = UNPACK_INT(inbuf, 0);
            
        // Outbuf is <<Rc:32>>
        // Run the command on the VM thread - this is for debugging only,
        // any real monitoring will use the async lock_stat 
        int rc = G_DB_ENV->memp_stat_print(G_DB_ENV, flags);
        RETURN_INT(rc, outbuf);
    }
    case CMD_MUTEX_STAT:
    {
        FAIL_IF_ASYNC_PENDING(d, outbuf);

        // Inbuf is <<Flags:32 >>

        // Mark the port as busy and then schedule the appropriate async operation
        d->async_op = cmd;
        d->async_pool = G_TPOOL_GENERAL;
        d->async_flags = UNPACK_INT(inbuf, 0);
        bdberl_tpool_run(d->async_pool, &do_async_mutex_stat, d, 0, &d->async_job);
        
        // Let caller know that the operation is in progress
        // Outbuf is: <<0:32>>
        RETURN_INT(0, outbuf);
    }
    case CMD_MUTEX_STAT_PRINT:
    {
        FAIL_IF_ASYNC_PENDING(d, outbuf);

        // Inbuf is << Flags:32 >>
        unsigned int flags = UNPACK_INT(inbuf, 0);
            
        // Outbuf is <<Rc:32>>
        // Run the command on the VM thread - this is for debugging only,
        // any real monitoring will use the async lock_stat 
        int rc = G_DB_ENV->mutex_stat_print(G_DB_ENV, flags);
        RETURN_INT(rc, outbuf);
    }
    case CMD_TXN_STAT:
    {
        FAIL_IF_ASYNC_PENDING(d, outbuf);

        // Inbuf is <<Flags:32 >>
        // Mark the port as busy and then schedule the appropriate async operation
        d->async_op = cmd;
        d->async_pool = G_TPOOL_GENERAL;
        d->async_flags = UNPACK_INT(inbuf, 0);
        bdberl_tpool_run(d->async_pool, &do_async_txn_stat, d, 0, &d->async_job);
        
        // Let caller know that the operation is in progress
        // Outbuf is: <<0:32>>
        RETURN_INT(0, outbuf);
    }
    case CMD_TXN_STAT_PRINT:
    {
        FAIL_IF_ASYNC_PENDING(d, outbuf);

        // Inbuf is << Flags:32 >>
        unsigned int flags = UNPACK_INT(inbuf, 0);
            
        // Outbuf is <<Rc:32>>
        // Run the command on the VM thread - this is for debugging only,
        // any real monitoring will use the async lock_stat 
        int rc = G_DB_ENV->txn_stat_print(G_DB_ENV, flags);
        RETURN_INT(rc, outbuf);
    }
    case CMD_DATA_DIRS_INFO:
    {
        FAIL_IF_ASYNC_PENDING(d, outbuf);

        do_sync_data_dirs_info(d);
       
        // Let caller know that the operation is in progress
        // Outbuf is: <<0:32>>
        RETURN_INT(0, outbuf);
    }

    case CMD_LOG_DIR_INFO:
    {
        FAIL_IF_ASYNC_PENDING(d, outbuf);

        // Find the log dir or use DB_HOME - error if not present
        const char *lg_dir = NULL;
        int rc = G_DB_ENV->get_lg_dir(G_DB_ENV, &lg_dir);
        if (0 == rc && NULL == lg_dir)
        {
            rc = G_DB_ENV->get_home(G_DB_ENV, &lg_dir);
        }
        // Send info if we can get a dir, otherwise return the error
        if (0 == rc)
        {
            // send a dirinfo message - will send an error message on a NULL lg_dir
            send_dir_info(d->port, d->port_owner, lg_dir);
        }
        else
        {
            send_rc(d->port, d->port_owner, rc);
        }

        // Let caller know that the operation is in progress
        // Outbuf is: <<0:32>>
        RETURN_INT(0, outbuf);
    }


    }
    *outbuf = 0;
    return 0;
}


static int open_database(const char* name, DBTYPE type, unsigned int flags, PortData* data, int* dbref_res)
{
    *dbref_res = -1;

    READ_LOCK(G_DATABASES_RWLOCK);

    // Look up the database by name in our hash table
    Database* database = (Database*)hive_hash_get(G_DATABASES_NAMES, name);
    if (database)
    {
        // Convert the database pointer into a dbref
        int dbref = database - G_DATABASES;

        // Great, the database was previously opened by someone else. Add it to our
        // list of refs, and if it's a new addition also register this port with the
        // Database structure in G_DATABASES
        if (add_dbref(data, dbref))
        {
            // Need to update G_DATABASES -- grab the write lock
            PROMOTE_READ_LOCK(G_DATABASES_RWLOCK);

            // Add a reference to this port
            add_portref(dbref, data->port);

            // Release RW lock and return the ref
            WRITE_UNLOCK(G_DATABASES_RWLOCK);
            *dbref_res = dbref;
            return 0;
        }
        else
        {
            // Already in our list of opened databases -- unlock and return the reference
            READ_UNLOCK(G_DATABASES_RWLOCK);
            *dbref_res = dbref;
            return 0;
        }
    }
    else
    {
        // This database hasn't been opened yet -- grab a write lock
        PROMOTE_READ_LOCK(G_DATABASES_RWLOCK);

        // While waiting on the write lock, another thread could have slipped in and 
        // opened the database, so do one more check to see if the database is already
        // open
        database = (Database*)hive_hash_get(G_DATABASES_NAMES, name);
        if (database)
        {
            // Database got created while we were waiting on the write lock, add a reference
            // to our port and drop the lock ASAP
            int dbref = database - G_DATABASES;
            add_portref(dbref, data->port);
            WRITE_UNLOCK(G_DATABASES_RWLOCK);

            add_dbref(data, dbref);
            *dbref_res = dbref;
            return 0;
        }

        // Database hasn't been created while we were waiting on write lock, so
        // create/open it

        // Find the first available slot in G_DATABASES; the index will be our
        // reference for database operations
        int dbref = alloc_dbref();
        if (dbref < 0)
        {
            // No more slots available 
            WRITE_UNLOCK(G_DATABASES_RWLOCK);
            return ERROR_MAX_DBS;
        }

        // Create the DB handle
        DB* db = NULL;
        DBGCMD(data, "db_create(&db, %p, 0);", G_DB_ENV);
        int rc = db_create(&db, G_DB_ENV, 0);
        DBGCMD(data, "rc = %s (%d) db = %p", rc == 0 ? "ok" : rc_to_atom_str(rc), rc, db);
        if (rc != 0)
        {
            // Failure while creating the database handle -- drop our lock and return 
            // the code
            WRITE_UNLOCK(G_DATABASES_RWLOCK);
            return rc;
        }

        // If a custom page size has been specified, try to use it
        if (G_PAGE_SIZE > 0)
        {
            if (db->set_pagesize(db, G_PAGE_SIZE) != 0)
            {
                bdb_errcall(G_DB_ENV, "", "Failed to set page size.");
            }
        }

        // Attempt to open our database
        DBGCMD(data, "db->open(%p, 0, '%s', 0, %x, %08x, 0);", db, name, type, flags);
        rc = db->open(db, 0, name, 0, type, flags, 0);
        DBGCMDRC(data, rc);
        if (rc != 0)
        {
            // Failure while opening the database -- cleanup the handle, drop the lock
            // and return
            db->close(db, 0);
            WRITE_UNLOCK(G_DATABASES_RWLOCK);
            return rc;
        }

        // Database is open. Store all the data into the allocated ref
        G_DATABASES[dbref].db = db;
        G_DATABASES[dbref].name = strdup(name);
        G_DATABASES[dbref].ports = zalloc(sizeof(PortList));
        G_DATABASES[dbref].ports->port = data->port;

        // Make entry in hash table of names
        hive_hash_add(G_DATABASES_NAMES, G_DATABASES[dbref].name, &(G_DATABASES[dbref]));

        // Drop the write lock
        WRITE_UNLOCK(G_DATABASES_RWLOCK);

        // Add the dbref to the port list 
        add_dbref(data, dbref);
        *dbref_res = dbref;
        return 0;
    }
}

static int close_database(int dbref, unsigned flags, PortData* data)
{
    // Remove this database from our list 
    if (del_dbref(data, dbref))
    {
        // Something was actually deleted from our list -- now we need to disassociate the
        // calling port with the global database structure.
        WRITE_LOCK(G_DATABASES_RWLOCK);

        assert(G_DATABASES[dbref].db != 0);
        assert(G_DATABASES[dbref].ports != 0);

        // Now disassociate this port from the database's port list
        del_portref(dbref, data->port);

        // Finally, if there are no other references to the database, close out
        // the database completely
        Database* database = &G_DATABASES[dbref];
        if (database->ports == 0)
        {
            // Close out the BDB handle
            DBGCMD(data, "database->db->close(%p, %08x) (for dbref %d)", database->db, flags, dbref);
            int rc = database->db->close(database->db, flags);
            DBGCMDRC(data, rc);

            // Remove the entry from the names map
            hive_hash_remove(G_DATABASES_NAMES, database->name);
            free((char*)database->name);

            // Zero out the whole record
            memset(database, '\0', sizeof(Database));
        }

        WRITE_UNLOCK(G_DATABASES_RWLOCK);
        return ERROR_NONE;
    }
    else
    {
        return ERROR_INVALID_DBREF;
    }
}

// Abort the transaction and clean up
static void abort_txn(PortData* d)
{
    if (d->txn)
    {
        DBGCMD(d, "d->txn->abort(%p)", d->txn);
        int rc = d->txn->abort(d->txn);
        DBGCMDRC(d, rc);
        d->txn = NULL;
    }
}

static int delete_database(const char* name)
{
    // Go directly to a write lock on the global databases structure
    WRITE_LOCK(G_DATABASES_RWLOCK);

    // Make sure the database is not opened by anyone
    if (hive_hash_get(G_DATABASES_NAMES, name))
    {
        WRITE_UNLOCK(G_DATABASES_RWLOCK);
        return ERROR_DB_ACTIVE;
    }

    // Good, database doesn't seem to be open -- attempt the delete
    DBG("Attempting to delete database: %s\r\n", name);
    int rc = G_DB_ENV->dbremove(G_DB_ENV, 0, name, 0, DB_AUTO_COMMIT);
    WRITE_UNLOCK(G_DATABASES_RWLOCK);

    return rc;
}

/**
 * Given a target system parameter, return the requested value
 */
static void get_info(int target, void* values, BinHelper* bh)
{
    switch(target) 
    {
    case SYSP_CACHESIZE_GET:
    {
        unsigned int gbytes = 0;
        unsigned int bytes = 0;
        int caches = 0;
        int rc = G_DB_ENV->get_cachesize(G_DB_ENV, &gbytes, &bytes, &caches);
        bin_helper_init(bh);
        bin_helper_push_int32(bh, rc);
        bin_helper_push_int32(bh, gbytes);
        bin_helper_push_int32(bh, bytes);
        bin_helper_push_int32(bh, caches);
        break;
    }
    case SYSP_TXN_TIMEOUT_GET:
    {
        unsigned int timeout = 0;
        int rc = G_DB_ENV->get_timeout(G_DB_ENV, &timeout, DB_SET_TXN_TIMEOUT);
        bin_helper_init(bh);
        bin_helper_push_int32(bh, rc);
        bin_helper_push_int32(bh, timeout);
        break;
    }
    case SYSP_DATA_DIR_GET:
    {
        const char** dirs = 0;
        int rc = G_DB_ENV->get_data_dirs(G_DB_ENV, &dirs);
        bin_helper_init(bh);
        bin_helper_push_int32(bh, rc);
        while (dirs && *dirs)
        {
            bin_helper_push_string(bh, *dirs);
            dirs++;
        }
        break;
    }
    case SYSP_LOG_DIR_GET:
    {
        const char* dir = 0;
        // Get the log dir - according to BDB docs, if not set
        // the DB_HOME is used.
        int rc = G_DB_ENV->get_lg_dir(G_DB_ENV, &dir);
        if (NULL == dir)
        {
            if (0 != G_DB_ENV->get_home(G_DB_ENV, &dir))
            {
                dir = NULL;
            }
        }
        bin_helper_init(bh);
        bin_helper_push_int32(bh, rc);
        bin_helper_push_string(bh, dir); // Will convert NULL pointer to "<null>"
        break;
    }
    }
}

static void async_cleanup(PortData* d)
{
    // Release the port for another operation
    d->work_buffer_offset = 0;
    erl_drv_mutex_lock(d->port_lock);
    d->async_dbref = -1;
    d->async_pool = 0;
    d->async_job  = 0;
    d->async_op = CMD_NONE;
    erl_drv_mutex_unlock(d->port_lock);
}


// Convert an rc from BDB into a string suitable for driver_mk_atom
// returns NULL on no match
static char *rc_to_atom_str(int rc)
{
    char *error = erl_errno_id(rc);
    //fprintf(stderr, "erl_errno_id(%d) = %s db_strerror = %s\n", rc, error, db_strerror(rc));
    if (NULL != error && strcmp("unknown", error) != 0)
    {
        return error;
    }
    else
    {
        switch (rc)
        {
            // bdberl driver errors
            case ERROR_MAX_DBS:       return "max_dbs";
            case ERROR_ASYNC_PENDING: return "async_pending";
            case ERROR_INVALID_DBREF: return "invalid_db";
            case ERROR_TXN_OPEN:      return "transaction_open";
            case ERROR_NO_TXN:        return "no_txn";
            case ERROR_CURSOR_OPEN:   return "cursor_open";
            case ERROR_NO_CURSOR:     return "no_cursor";
            case ERROR_DB_ACTIVE:     return "db_active";
            case ERROR_INVALID_CMD:   return "invalid_cmd";
            case ERROR_INVALID_DB_TYPE: return "invalid_db_type";
            case ERROR_INVALID_VALUE: return "invalid_value";
            // bonafide BDB errors
            case DB_BUFFER_SMALL:     return "buffer_small";
            case DB_DONOTINDEX:       return "do_not_index";
            case DB_FOREIGN_CONFLICT: return "foreign_conflict";
            case DB_KEYEMPTY:         return "key_empty";
            case DB_KEYEXIST:         return "key_exist";
            case DB_LOCK_DEADLOCK:    return "deadlock";
            case DB_LOCK_NOTGRANTED:  return "lock_not_granted";
            case DB_LOG_BUFFER_FULL:  return "log_buffer_full";
            case DB_NOTFOUND:         return "not_found";
            case DB_OLD_VERSION:      return "old_version";
            case DB_PAGE_NOTFOUND:    return "page_not_found";
            case DB_RUNRECOVERY:      return "run_recovery";
            case DB_VERIFY_BAD:       return "verify_bad";
            case DB_VERSION_MISMATCH: return "version_mismatch";
            default:                  return NULL;
        }
    }
}


// Send a {dirinfo, Path, FsId, MbyteAvail} message to pid given.
// Send an {errno, Reason} on failure
// returns 0 on success, errno on failure
static int send_dir_info(ErlDrvPort port, ErlDrvTermData pid, const char *path)
{
    struct statvfs svfs;
    int rc;

    if (NULL == path)
    {
        rc = EINVAL;
    }
    else if (0 != statvfs(path, &svfs))
    {
        rc = errno;
    }
    else
    {
        rc = 0;
    }

    if (0 != rc)
    {
        send_rc(port, pid, rc);
    }
    else
    {
        fsblkcnt_t blocks_per_mbyte = 1024 * 1024 / svfs.f_frsize;
        assert(blocks_per_mbyte > 0);
        unsigned int mbyte_avail = (unsigned int) (svfs.f_bavail / blocks_per_mbyte);
        int path_len = strlen(path);

        ErlDrvTermData response[] = { ERL_DRV_ATOM,  driver_mk_atom("dirinfo"),
                                      ERL_DRV_STRING, (ErlDrvTermData) path, path_len,
                                      // send fsid as a binary as will only be used
                                      // to compare which physical filesystem is on
                                      // and the definintion varies between platforms.
                                      ERL_DRV_BUF2BINARY, (ErlDrvTermData) &svfs.f_fsid, 
                                                          sizeof(svfs.f_fsid),
                                      ERL_DRV_UINT, mbyte_avail,
                                      ERL_DRV_TUPLE, 4};
        driver_send_term(port, pid, response, sizeof(response) / sizeof(response[0]));
    }
    return rc;
}


static void send_rc(ErlDrvPort port, ErlDrvTermData pid, int rc)
{
    // TODO: May need to tag the messages a bit more explicitly so that if another async
    // job runs to completion before the message gets delivered we don't mis-interpret this
    // response code.
    if (rc == 0)
    {
        ErlDrvTermData response[] = {ERL_DRV_ATOM, driver_mk_atom("ok")};
        driver_send_term(port, pid, response, sizeof(response) / sizeof(response[0]));
    }
    else
    {
        // See if this is a standard errno that we have an erlang code for
        char *error = rc_to_atom_str(rc);
        if (NULL != error)
        {
            ErlDrvTermData response[] = { ERL_DRV_ATOM,  driver_mk_atom("error"),
                                          ERL_DRV_ATOM,  driver_mk_atom(error),
                                          ERL_DRV_TUPLE, 2};
            driver_send_term(port, pid, response, sizeof(response) / sizeof(response[0]));
        }
        else
        {
            ErlDrvTermData response[] = { ERL_DRV_ATOM, driver_mk_atom("error"),
                                          ERL_DRV_ATOM, driver_mk_atom("unknown"),
                                          ERL_DRV_INT,  rc,
                                          ERL_DRV_TUPLE, 2,
                                          ERL_DRV_TUPLE, 2};
            driver_send_term(port, pid, response, sizeof(response) / sizeof(response[0]));
        }
    }
}

static void async_cleanup_and_send_rc(PortData* d, int rc)
{
    // Save the port and pid references -- we need copies independent from the PortData
    // structure. Once we release the port_lock after clearing the cmd, it's possible that
    // the port could go away without waiting on us to finish. This is acceptable, but we need
    // to be certain that there is no overlap of data between the two threads. driver_send_term
    // is safe to use from a thread, even if the port you're sending from has already expired.
    ErlDrvPort port = d->port;
    ErlDrvTermData pid = d->port_owner;

    async_cleanup(d);
    send_rc(port, pid, rc);
}

static void async_cleanup_and_send_kv(PortData* d, int rc, DBT* key, DBT* value)
{
    // Save the port and pid references -- we need copies independent from the PortData
    // structure. Once we release the port_lock after clearing the cmd, it's possible that
    // the port could go away without waiting on us to finish. This is acceptable, but we need
    // to be certain that there is no overlap of data between the two threads. driver_send_term
    // is safe to use from a thread, even if the port you're sending from has already expired.
    ErlDrvPort port = d->port;
    ErlDrvTermData pid = d->port_owner;

    async_cleanup(d);

    // Notify port of result
    if (rc == 0)
    {
        ErlDrvTermData response[] = { ERL_DRV_ATOM, driver_mk_atom("ok"),
                                      ERL_DRV_BUF2BINARY, (ErlDrvTermData)key->data, (ErlDrvUInt)key->size,
                                      ERL_DRV_BUF2BINARY, (ErlDrvTermData)value->data, (ErlDrvUInt)value->size,
                                      ERL_DRV_TUPLE, 3};
        driver_send_term(port, pid, response, sizeof(response) / sizeof(response[0]));
    }
    else if (rc == DB_NOTFOUND)
    {
        ErlDrvTermData response[] = { ERL_DRV_ATOM, driver_mk_atom("not_found") };
        driver_send_term(port, pid, response, sizeof(response) / sizeof(response[0]));
    }
    else
    {
        // See if this is a standard errno that we have an erlang code for
        char *error = rc_to_atom_str(rc);
        if (NULL != error)
        {
            ErlDrvTermData response[] = { ERL_DRV_ATOM,  driver_mk_atom("error"),
                                          ERL_DRV_ATOM,  driver_mk_atom(error),
                                          ERL_DRV_TUPLE, 2};
            driver_send_term(port, pid, response, sizeof(response) / sizeof(response[0]));
        }
        else
        {
            ErlDrvTermData response[] = { ERL_DRV_ATOM, driver_mk_atom("error"),
                                          ERL_DRV_ATOM, driver_mk_atom("unknown"),
                                          ERL_DRV_INT,  rc,
                                          ERL_DRV_TUPLE, 2,
                                          ERL_DRV_TUPLE, 2};
            driver_send_term(port, pid, response, sizeof(response) / sizeof(response[0]));
        }
    }
}



#define BT_STATS_TUPLE(base, member)                       \
    ERL_DRV_ATOM, driver_mk_atom(#member),              \
        ERL_DRV_UINT, (base)->bt_##member,                   \
        ERL_DRV_TUPLE, 2
static void async_cleanup_and_send_btree_stats(PortData* d, char *type, DB_BTREE_STAT *bsp)
{
    // Save the port and pid references -- we need copies independent from the PortData
    // structure. Once we release the port_lock after clearing the cmd, it's possible that
    // the port could go away without waiting on us to finish. This is acceptable, but we need
    // to be certain that there is no overlap of data between the two threads. driver_send_term
    // is safe to use from a thread, even if the port you're sending from has already expired.
    ErlDrvPort port = d->port;
    ErlDrvTermData pid = d->port_owner;
    async_cleanup(d);

    ErlDrvTermData response[] = {
        ERL_DRV_ATOM, driver_mk_atom("ok"),
        // Start of list
            ERL_DRV_ATOM, driver_mk_atom("type"),
            ERL_DRV_ATOM, driver_mk_atom(type),
            ERL_DRV_TUPLE, 2,
        BT_STATS_TUPLE(bsp, magic),		/* Magic number. */
        BT_STATS_TUPLE(bsp, version),		/* Version number. */
        BT_STATS_TUPLE(bsp, metaflags),		/* Metadata flags. */
        BT_STATS_TUPLE(bsp, nkeys),		/* Number of unique keys. */
        BT_STATS_TUPLE(bsp, ndata),		/* Number of data items. */
        BT_STATS_TUPLE(bsp, pagecnt),		/* Page count. */
        BT_STATS_TUPLE(bsp, pagesize),		/* Page size. */
        BT_STATS_TUPLE(bsp, minkey),		/* Minkey value. */
        BT_STATS_TUPLE(bsp, re_len),		/* Fixed-length record length. */
        BT_STATS_TUPLE(bsp, re_pad),		/* Fixed-length record pad. */
        BT_STATS_TUPLE(bsp, levels),		/* Tree levels. */
        BT_STATS_TUPLE(bsp, int_pg),		/* Internal pages. */
        BT_STATS_TUPLE(bsp, leaf_pg),		/* Leaf pages. */
        BT_STATS_TUPLE(bsp, dup_pg),		/* Duplicate pages. */
        BT_STATS_TUPLE(bsp, over_pg),		/* Overflow pages. */
        BT_STATS_TUPLE(bsp, empty_pg),		/* Empty pages. */
        BT_STATS_TUPLE(bsp, free),		/* Pages on the free list. */
        BT_STATS_TUPLE(bsp, int_pgfree),	/* Bytes free in internal pages. */
        BT_STATS_TUPLE(bsp, leaf_pgfree),	/* Bytes free in leaf pages. */
        BT_STATS_TUPLE(bsp, dup_pgfree),	/* Bytes free in duplicate pages. */
        BT_STATS_TUPLE(bsp, over_pgfree),	/* Bytes free in overflow pages. */
        // End of list
        ERL_DRV_NIL, 
        ERL_DRV_LIST, 21+2,
        ERL_DRV_TUPLE, 2 
    };
    driver_send_term(port, pid, response, sizeof(response) / sizeof(response[0]));
}
#undef BT_STATS_TUPLE


#define HASH_STATS_TUPLE(base, member)                       \
    ERL_DRV_ATOM, driver_mk_atom(#member),              \
        ERL_DRV_UINT, (base)->hash_##member,                   \
        ERL_DRV_TUPLE, 2

static void async_cleanup_and_send_hash_stats(PortData* d, DB_HASH_STAT *hsp)
{
    // Save the port and pid references -- we need copies independent from the PortData
    // structure. Once we release the port_lock after clearing the cmd, it's possible that
    // the port could go away without waiting on us to finish. This is acceptable, but we need
    // to be certain that there is no overlap of data between the two threads. driver_send_term
    // is safe to use from a thread, even if the port you're sending from has already expired.
    ErlDrvPort port = d->port;
    ErlDrvTermData pid = d->port_owner;
    async_cleanup(d);

    ErlDrvTermData response[] = {
        ERL_DRV_ATOM, driver_mk_atom("ok"),
        // Start of list
            ERL_DRV_ATOM, driver_mk_atom("type"),
            ERL_DRV_ATOM, driver_mk_atom("hash"),
            ERL_DRV_TUPLE, 2,
	HASH_STATS_TUPLE(hsp, magic),		/* Magic number. */
	HASH_STATS_TUPLE(hsp, version),		/* Version number. */
	HASH_STATS_TUPLE(hsp, metaflags),	/* Metadata flags. */
	HASH_STATS_TUPLE(hsp, nkeys),		/* Number of unique keys. */
	HASH_STATS_TUPLE(hsp, ndata),		/* Number of data items. */
	HASH_STATS_TUPLE(hsp, pagecnt),		/* Page count. */
	HASH_STATS_TUPLE(hsp, pagesize),	/* Page size. */
	HASH_STATS_TUPLE(hsp, ffactor),		/* Fill factor specified at create. */
	HASH_STATS_TUPLE(hsp, buckets),		/* Number of hash buckets. */
	HASH_STATS_TUPLE(hsp, free),		/* Pages on the free list. */
	HASH_STATS_TUPLE(hsp, bfree),		/* Bytes free on bucket pages. */
	HASH_STATS_TUPLE(hsp, bigpages),	/* Number of big key/data pages. */
	HASH_STATS_TUPLE(hsp, big_bfree),	/* Bytes free on big item pages. */
	HASH_STATS_TUPLE(hsp, overflows),	/* Number of overflow pages. */
	HASH_STATS_TUPLE(hsp, ovfl_free),	/* Bytes free on ovfl pages. */
	HASH_STATS_TUPLE(hsp, dup),		/* Number of dup pages. */
	HASH_STATS_TUPLE(hsp, dup_free),	/* Bytes free on duplicate pages. */
        // End of list
        ERL_DRV_NIL, 
        ERL_DRV_LIST, 17+2,
        ERL_DRV_TUPLE, 2 
    };
    driver_send_term(port, pid, response, sizeof(response) / sizeof(response[0]));
}
#undef HASH_STATS_TUPLE

#ifdef ENABLE_QUEUE // If we ever decide to support Queues 

#define QS_STATS_TUPLE(base, member)                       \
    ERL_DRV_ATOM, driver_mk_atom(#member),              \
        ERL_DRV_UINT, (base)->qs_##member,                   \
        ERL_DRV_TUPLE, 2
static void async_cleanup_and_send_queue_stats(PortData* d, DB_QUEUE_STAT *qsp)
{
    // Save the port and pid references -- we need copies independent from the PortData
    // structure. Once we release the port_lock after clearing the cmd, it's possible that
    // the port could go away without waiting on us to finish. This is acceptable, but we need
    // to be certain that there is no overlap of data between the two threads. driver_send_term
    // is safe to use from a thread, even if the port you're sending from has already expired.
    ErlDrvPort port = d->port;
    ErlDrvTermData pid = d->port_owner;
    async_cleanup(d);

    ErlDrvTermData response[] = {
        ERL_DRV_ATOM, driver_mk_atom("ok"),
        // Start of list
            ERL_DRV_ATOM, driver_mk_atom("type"),
            ERL_DRV_ATOM, driver_mk_atom("queue"),
            ERL_DRV_TUPLE, 2,
	QS_STAT_TUPLE(qsp, qs_magic),		/* Magic number. */
	QS_STAT_TUPLE(qsp, version),		/* Version number. */
	QS_STAT_TUPLE(qsp, metaflags),		/* Metadata flags. */
	QS_STAT_TUPLE(qsp, nkeys),		/* Number of unique keys. */
	QS_STAT_TUPLE(qsp, ndata),		/* Number of data items. */
	QS_STAT_TUPLE(qsp, pagesize),		/* Page size. */
	QS_STAT_TUPLE(qsp, extentsize),	        /* Pages per extent. */
	QS_STAT_TUPLE(qsp, pages),		/* Data pages. */
	QS_STAT_TUPLE(qsp, re_len),		/* Fixed-length record length. */
	QS_STAT_TUPLE(qsp, re_pad),		/* Fixed-length record pad. */
	QS_STAT_TUPLE(qsp, pgfree),		/* Bytes free in data pages. */
	QS_STAT_TUPLE(qsp, first_recno),	/* First not deleted record. */
	QS_STAT_TUPLE(qsp, cur_recno),		/* Next available record number. */
        // End of list
        ERL_DRV_NIL, 
        ERL_DRV_LIST, 13+2,
        ERL_DRV_TUPLE, 2 
    };
    driver_send_term(port, pid, response, sizeof(response) / sizeof(response[0]));
}
#undef QUEUE_STATS_TUPLE
#endif // ENABLE_QUEUE

#define ST_STATS_TUPLE(base, member)                       \
    ERL_DRV_ATOM, driver_mk_atom(#member),              \
        ERL_DRV_UINT, (base)->st_##member,                   \
        ERL_DRV_TUPLE, 2

#define ST_STATS_INT_TUPLE(base, member)                       \
    ERL_DRV_ATOM, driver_mk_atom(#member),              \
        ERL_DRV_INT, (base)->st_##member,                   \
        ERL_DRV_TUPLE, 2

static void async_cleanup_and_send_lock_stats(PortData* d, DB_LOCK_STAT *lsp)
{
    // Save the port and pid references -- we need copies independent from the PortData
    // structure. Once we release the port_lock after clearing the cmd, it's possible that
    // the port could go away without waiting on us to finish. This is acceptable, but we need
    // to be certain that there is no overlap of data between the two threads. driver_send_term
    // is safe to use from a thread, even if the port you're sending from has already expired.
    ErlDrvPort port = d->port;
    ErlDrvTermData pid = d->port_owner;
    async_cleanup(d);

    ErlDrvTermData response[] = {
        ERL_DRV_ATOM, driver_mk_atom("ok"),
        // Start of list
	ST_STATS_TUPLE(lsp, id),		/* Last allocated locker ID. */
	ST_STATS_TUPLE(lsp, cur_maxid),	/* Current maximum unused ID. */
	ST_STATS_TUPLE(lsp, maxlocks),	/* Maximum number of locks in table. */
	ST_STATS_TUPLE(lsp, maxlockers),	/* Maximum num of lockers in table. */
	ST_STATS_TUPLE(lsp, maxobjects),	/* Maximum num of objects in table. */
	ST_STATS_TUPLE(lsp, partitions),	/* number of partitions. */
	ST_STATS_INT_TUPLE(lsp, nmodes),	/* Number of lock modes. */
	ST_STATS_TUPLE(lsp, nlockers),	/* Current number of lockers. */
	ST_STATS_TUPLE(lsp, nlocks),	/* Current number of locks. */
	ST_STATS_TUPLE(lsp, maxnlocks),	/* Maximum number of locks so far. */
	ST_STATS_TUPLE(lsp, maxhlocks),	/* Maximum number of locks in any bucket. */
	ST_STATS_TUPLE(lsp, locksteals),	/* Number of lock steals so far. */
	ST_STATS_TUPLE(lsp, maxlsteals),	/* Maximum number steals in any partition. */
	ST_STATS_TUPLE(lsp, maxnlockers),	/* Maximum number of lockers so far. */
	ST_STATS_TUPLE(lsp, nobjects),	/* Current number of objects. */
	ST_STATS_TUPLE(lsp, maxnobjects),	/* Maximum number of objects so far. */
	ST_STATS_TUPLE(lsp, maxhobjects),	/* Maximum number of objectsin any bucket. */
	ST_STATS_TUPLE(lsp, objectsteals),	/* Number of objects steals so far. */
	ST_STATS_TUPLE(lsp, maxosteals),	/* Maximum number of steals in any partition. */
	ST_STATS_TUPLE(lsp, nrequests),	/* Number of lock gets. */
	ST_STATS_TUPLE(lsp, nreleases),	/* Number of lock puts. */
	ST_STATS_TUPLE(lsp, nupgrade),	/* Number of lock upgrades. */
	ST_STATS_TUPLE(lsp, ndowngrade),	/* Number of lock downgrades. */
	ST_STATS_TUPLE(lsp, lock_wait),	/* Lock conflicts w/ subsequent wait */
	ST_STATS_TUPLE(lsp, lock_nowait),	/* Lock conflicts w/o subsequent wait */
	ST_STATS_TUPLE(lsp, ndeadlocks),	/* Number of lock deadlocks. */
	ST_STATS_TUPLE(lsp, locktimeout),	/* Lock timeout. */
	ST_STATS_TUPLE(lsp, nlocktimeouts),	/* Number of lock timeouts. */
	ST_STATS_TUPLE(lsp, txntimeout),	/* Transaction timeout. */
	ST_STATS_TUPLE(lsp, ntxntimeouts),	/* Number of transaction timeouts. */
	ST_STATS_TUPLE(lsp, part_wait),	/* Partition lock granted after wait. */
	ST_STATS_TUPLE(lsp, part_nowait),	/* Partition lock granted without wait. */
	ST_STATS_TUPLE(lsp, part_max_wait),	/* Max partition lock granted after wait. */
	ST_STATS_TUPLE(lsp, part_max_nowait),	/* Max partition lock granted without wait. */
	ST_STATS_TUPLE(lsp, objs_wait),	/* Object lock granted after wait. */
	ST_STATS_TUPLE(lsp, objs_nowait),	/* Object lock granted without wait. */
	ST_STATS_TUPLE(lsp, lockers_wait),	/* Locker lock granted after wait. */
	ST_STATS_TUPLE(lsp, lockers_nowait),/* Locker lock granted without wait. */
	ST_STATS_TUPLE(lsp, region_wait),	/* Region lock granted after wait. */
	ST_STATS_TUPLE(lsp, region_nowait),	/* Region lock granted without wait. */
	ST_STATS_TUPLE(lsp, hash_len),	/* Max length of bucket. */
	ST_STATS_TUPLE(lsp, regsize),	/* Region size. - will have to cast to uint */

        // End of list
        ERL_DRV_NIL, 
        ERL_DRV_LIST, 42+1,
        ERL_DRV_TUPLE, 2 
    };
    driver_send_term(port, pid, response, sizeof(response) / sizeof(response[0]));
}

static void async_cleanup_and_send_log_stats(PortData* d, DB_LOG_STAT *lsp)
{
    // Save the port and pid references -- we need copies independent from the PortData
    // structure. Once we release the port_lock after clearing the cmd, it's possible that
    // the port could go away without waiting on us to finish. This is acceptable, but we need
    // to be certain that there is no overlap of data between the two threads. driver_send_term
    // is safe to use from a thread, even if the port you're sending from has already expired.
    ErlDrvPort port = d->port;
    ErlDrvTermData pid = d->port_owner;
    async_cleanup(d);

    ErlDrvTermData response[] = {
        ERL_DRV_ATOM, driver_mk_atom("ok"),
        // Start of list
	ST_STATS_TUPLE(lsp, magic),	/* Log file magic number. */
	ST_STATS_TUPLE(lsp, version),	/* Log file version number. */
	ST_STATS_INT_TUPLE(lsp, mode),	/* Log file permissions mode. */
	ST_STATS_TUPLE(lsp, lg_bsize),	/* Log buffer size. */
	ST_STATS_TUPLE(lsp, lg_size),	/* Log file size. */
	ST_STATS_TUPLE(lsp, wc_bytes),	/* Bytes to log since checkpoint. */
	ST_STATS_TUPLE(lsp, wc_mbytes),	/* Megabytes to log since checkpoint. */
	ST_STATS_TUPLE(lsp, record),	/* Records entered into the log. */
	ST_STATS_TUPLE(lsp, w_bytes),	/* Bytes to log. */
	ST_STATS_TUPLE(lsp, w_mbytes),	/* Megabytes to log. */
	ST_STATS_TUPLE(lsp, wcount),	/* Total I/O writes to the log. */
	ST_STATS_TUPLE(lsp, wcount_fill),/* Overflow writes to the log. */
	ST_STATS_TUPLE(lsp, rcount),	/* Total I/O reads from the log. */
	ST_STATS_TUPLE(lsp, scount),	/* Total syncs to the log. */
	ST_STATS_TUPLE(lsp, region_wait),	/* Region lock granted after wait. */
	ST_STATS_TUPLE(lsp, region_nowait),	/* Region lock granted without wait. */
	ST_STATS_TUPLE(lsp, cur_file),	/* Current log file number. */
	ST_STATS_TUPLE(lsp, cur_offset),/* Current log file offset. */
	ST_STATS_TUPLE(lsp, disk_file),	/* Known on disk log file number. */
	ST_STATS_TUPLE(lsp, disk_offset),	/* Known on disk log file offset. */
	ST_STATS_TUPLE(lsp, maxcommitperflush),	/* Max number of commits in a flush. */
	ST_STATS_TUPLE(lsp, mincommitperflush),	/* Min number of commits in a flush. */
	ST_STATS_TUPLE(lsp, regsize),		/* Region size. */

        // End of list
        ERL_DRV_NIL, 
        ERL_DRV_LIST, 23+1,
        ERL_DRV_TUPLE, 2 
    };
    driver_send_term(port, pid, response, sizeof(response) / sizeof(response[0]));
}

static void send_mpool_fstat(ErlDrvPort port, ErlDrvTermData pid, DB_MPOOL_FSTAT *fsp)
{
    char *name = fsp->file_name ? fsp->file_name : "<null>";
    int name_len = strlen(name);
    ErlDrvTermData response[] = {
        ERL_DRV_ATOM, driver_mk_atom("fstat"),
        // Start of list
            ERL_DRV_ATOM, driver_mk_atom("name"),
            ERL_DRV_STRING, (ErlDrvTermData) name, name_len,
            ERL_DRV_TUPLE, 2,
	ST_STATS_TUPLE(fsp, map),		/* Pages from mapped files. */
	ST_STATS_TUPLE(fsp, cache_hit),		/* Pages found in the cache. */
	ST_STATS_TUPLE(fsp, cache_miss),	/* Pages not found in the cache. */
	ST_STATS_TUPLE(fsp, page_create),	/* Pages created in the cache. */
	ST_STATS_TUPLE(fsp, page_in),		/* Pages read in. */
	ST_STATS_TUPLE(fsp, page_out),		/* Pages written out. */
        // End of list
        ERL_DRV_NIL, 
        ERL_DRV_LIST, 7+1,
        ERL_DRV_TUPLE, 2 
    };
    driver_send_term(port, pid, response, sizeof(response) / sizeof(response[0]));    
}

static void async_cleanup_and_send_memp_stats(PortData* d, DB_MPOOL_STAT *gsp,
                                              DB_MPOOL_FSTAT **fsp)
{
    // Save the port and pid references -- we need copies independent from the PortData
    // structure. Once we release the port_lock after clearing the cmd, it's possible that
    // the port could go away without waiting on us to finish. This is acceptable, but we need
    // to be certain that there is no overlap of data between the two threads. driver_send_term
    // is safe to use from a thread, even if the port you're sending from has already expired.
    ErlDrvPort port = d->port;
    ErlDrvTermData pid = d->port_owner;
    async_cleanup(d);

    // First send the per-file stats
    int i;
    for (i = 0; fsp != NULL && fsp[i] != NULL; i++)
    {
        send_mpool_fstat(port, pid, fsp[i]);
    }

    // Then send the global stats
    ErlDrvTermData response[] = {
        ERL_DRV_ATOM, driver_mk_atom("ok"),
        // Start of list
	ST_STATS_TUPLE(gsp, gbytes),		/* Total cache size: GB. */
	ST_STATS_TUPLE(gsp, bytes),		/* Total cache size: B. */
	ST_STATS_TUPLE(gsp, ncache),		/* Number of cache regions. */
	ST_STATS_TUPLE(gsp, max_ncache),	/* Maximum number of regions. */
        ST_STATS_INT_TUPLE(gsp, mmapsize),	/* Maximum file size for mmap. */
        ST_STATS_INT_TUPLE(gsp, maxopenfd),	/* Maximum number of open fd's. */
        ST_STATS_INT_TUPLE(gsp, maxwrite),	/* Maximum buffers to write. */
	ST_STATS_TUPLE(gsp, maxwrite_sleep),	/* Sleep after writing max buffers. */
	ST_STATS_TUPLE(gsp, pages),		/* Total number of pages. */
	ST_STATS_TUPLE(gsp, map),		/* Pages from mapped files. */
	ST_STATS_TUPLE(gsp, cache_hit),		/* Pages found in the cache. */
	ST_STATS_TUPLE(gsp, cache_miss),	/* Pages not found in the cache. */
	ST_STATS_TUPLE(gsp, page_create),	/* Pages created in the cache. */
	ST_STATS_TUPLE(gsp, page_in),		/* Pages read in. */
	ST_STATS_TUPLE(gsp, page_out),		/* Pages written out. */
	ST_STATS_TUPLE(gsp, ro_evict),		/* Clean pages forced from the cache. */
	ST_STATS_TUPLE(gsp, rw_evict),		/* Dirty pages forced from the cache. */
	ST_STATS_TUPLE(gsp, page_trickle),	/* Pages written by memp_trickle. */
	ST_STATS_TUPLE(gsp, page_clean),	/* Clean pages. */
	ST_STATS_TUPLE(gsp, page_dirty),	/* Dirty pages. */
	ST_STATS_TUPLE(gsp, hash_buckets),	/* Number of hash buckets. */
	ST_STATS_TUPLE(gsp, hash_searches),	/* Total hash chain searches. */
	ST_STATS_TUPLE(gsp, hash_longest),	/* Longest hash chain searched. */
	ST_STATS_TUPLE(gsp, hash_examined),	/* Total hash entries searched. */
	ST_STATS_TUPLE(gsp, hash_nowait),	/* Hash lock granted with nowait. */
	ST_STATS_TUPLE(gsp, hash_wait),		/* Hash lock granted after wait. */
	ST_STATS_TUPLE(gsp, hash_max_nowait),	/* Max hash lock granted with nowait. */
	ST_STATS_TUPLE(gsp, hash_max_wait),	/* Max hash lock granted after wait. */
	ST_STATS_TUPLE(gsp, region_nowait),	/* Region lock granted with nowait. */
	ST_STATS_TUPLE(gsp, region_wait),	/* Region lock granted after wait. */
	ST_STATS_TUPLE(gsp, mvcc_frozen),	/* Buffers frozen. */
	ST_STATS_TUPLE(gsp, mvcc_thawed),	/* Buffers thawed. */
	ST_STATS_TUPLE(gsp, mvcc_freed),	/* Frozen buffers freed. */
	ST_STATS_TUPLE(gsp, alloc),		/* Number of page allocations. */
	ST_STATS_TUPLE(gsp, alloc_buckets),	/* Buckets checked during allocation. */
	ST_STATS_TUPLE(gsp, alloc_max_buckets),	/* Max checked during allocation. */
	ST_STATS_TUPLE(gsp, alloc_pages),	/* Pages checked during allocation. */
	ST_STATS_TUPLE(gsp, alloc_max_pages),	/* Max checked during allocation. */
	ST_STATS_TUPLE(gsp, io_wait),		/* Thread waited on buffer I/O. */
        ST_STATS_TUPLE(gsp, regsize),		/* Region size. */

        // End of list
        ERL_DRV_NIL, 
        ERL_DRV_LIST, 40+1,
        ERL_DRV_TUPLE, 2 
    };
    driver_send_term(port, pid, response, sizeof(response) / sizeof(response[0]));
}


static void async_cleanup_and_send_mutex_stats(PortData* d, DB_MUTEX_STAT *msp)
{
    // Save the port and pid references -- we need copies independent from the PortData
    // structure. Once we release the port_lock after clearing the cmd, it's possible that
    // the port could go away without waiting on us to finish. This is acceptable, but we need
    // to be certain that there is no overlap of data between the two threads. driver_send_term
    // is safe to use from a thread, even if the port you're sending from has already expired.
    ErlDrvPort port = d->port;
    ErlDrvTermData pid = d->port_owner;
    async_cleanup(d);

    ErlDrvTermData response[] = {
        ERL_DRV_ATOM, driver_mk_atom("ok"),
        // Start of list
	ST_STATS_TUPLE(msp, mutex_align),	/* Mutex alignment */
	ST_STATS_TUPLE(msp, mutex_tas_spins),	/* Mutex test-and-set spins */
	ST_STATS_TUPLE(msp, mutex_cnt),		/* Mutex count */
	ST_STATS_TUPLE(msp, mutex_free),	/* Available mutexes */
	ST_STATS_TUPLE(msp, mutex_inuse),	/* Mutexes in use */
	ST_STATS_TUPLE(msp, mutex_inuse_max),	/* Maximum mutexes ever in use */
	ST_STATS_TUPLE(msp, region_wait),	/* Region lock granted after wait. */
	ST_STATS_TUPLE(msp, region_nowait),	/* Region lock granted without wait. */
	ST_STATS_TUPLE(msp, regsize),		/* Region size. */
        // End of list
        ERL_DRV_NIL, 
        ERL_DRV_LIST, 9+1,
        ERL_DRV_TUPLE, 2 
    };
    driver_send_term(port, pid, response, sizeof(response) / sizeof(response[0]));
}

#define STATS_TUPLE(base, member)                       \
    ERL_DRV_ATOM, driver_mk_atom(#member),              \
        ERL_DRV_UINT, (base)->member,                   \
        ERL_DRV_TUPLE, 2

#define STATS_LSN_TUPLE(base, member)                               \
    ERL_DRV_ATOM, driver_mk_atom(#member),                          \
        ERL_DRV_UINT, (base)->member.file,                          \
        ERL_DRV_UINT, (base)->member.offset,                        \
        ERL_DRV_TUPLE, 2,                                           \
        ERL_DRV_TUPLE, 2

static void send_txn_tstat(ErlDrvPort port, ErlDrvTermData pid, DB_TXN_ACTIVE *tasp)
{
    char *name = tasp->name ? tasp->name : "<null>";
    int name_len = strlen(name);
    char tid_str[32];
    char *status_str;
    switch (tasp->status)
    {
        case TXN_ABORTED:
            status_str = "aborted";
            break;
        case TXN_COMMITTED:
            status_str = "committed";
            break;
        case TXN_PREPARED:
            status_str = "prepared";
            break;
        case TXN_RUNNING:
            status_str = "running";
            break;
        default:
            status_str = "undefined";
            break;
    }

    int tid_str_len = snprintf(tid_str, sizeof(tid_str), "%lu", (unsigned long) tasp->tid);

    ErlDrvTermData response[] = {
        ERL_DRV_ATOM, driver_mk_atom("txn"),
	STATS_TUPLE(tasp, txnid),		/* Transaction ID */
	STATS_TUPLE(tasp, parentid),		/* Transaction ID of parent */
	STATS_TUPLE(tasp, pid),			/* Process owning txn ID - pid_t */
            ERL_DRV_ATOM, driver_mk_atom("tid"),/* OSX has 32-bit ints in erlang, so return as */
            ERL_DRV_STRING, (ErlDrvTermData) tid_str, tid_str_len, /* a string */
        ERL_DRV_TUPLE, 2,
	STATS_LSN_TUPLE(tasp, lsn),		/* LSN when transaction began */
	STATS_LSN_TUPLE(tasp, read_lsn),	/* Read LSN for MVCC */
	STATS_TUPLE(tasp, mvcc_ref),		/* MVCC reference count */

        // Start of list
            ERL_DRV_ATOM, driver_mk_atom("status"),
            ERL_DRV_ATOM, driver_mk_atom(status_str),
        ERL_DRV_TUPLE, 2,

            ERL_DRV_ATOM, driver_mk_atom("name"),
            ERL_DRV_STRING, (ErlDrvTermData) name, name_len,
        ERL_DRV_TUPLE, 2,


        // End of list
        ERL_DRV_NIL, 
        ERL_DRV_LIST, 9+1,
        ERL_DRV_TUPLE, 2 
    };
    driver_send_term(port, pid, response, sizeof(response) / sizeof(response[0]));    
}

#define ST_STATS_LSN_TUPLE(base, member)                            \
    ERL_DRV_ATOM, driver_mk_atom(#member),                          \
        ERL_DRV_UINT, (base)->st_##member.file,                     \
        ERL_DRV_UINT, (base)->st_##member.offset,                   \
        ERL_DRV_TUPLE, 2,                                           \
        ERL_DRV_TUPLE, 2

static void async_cleanup_and_send_txn_stats(PortData* d, DB_TXN_STAT *tsp)
{
    // Save the port and pid references -- we need copies independent from the PortData
    // structure. Once we release the port_lock after clearing the cmd, it's possible that
    // the port could go away without waiting on us to finish. This is acceptable, but we need
    // to be certain that there is no overlap of data between the two threads. driver_send_term
    // is safe to use from a thread, even if the port you're sending from has already expired.
    ErlDrvPort port = d->port;
    ErlDrvTermData pid = d->port_owner;
    async_cleanup(d);

    // First send the array of active transactions */
    int i;
    for (i = 0; i < tsp->st_nactive; i++)
    {
        send_txn_tstat(port, pid, tsp->st_txnarray+i);
    }

    // Then send the global stats
    ErlDrvTermData response[] = {
        ERL_DRV_ATOM, driver_mk_atom("ok"),
        // Start of list
	ST_STATS_TUPLE(tsp, nrestores),		/* number of restored transactions
					           after recovery. */
	ST_STATS_LSN_TUPLE(tsp, last_ckp),	/* lsn of the last checkpoint */
        ST_STATS_TUPLE(tsp, time_ckp),	        /* time of last checkpoint (time_t to uint) */
	ST_STATS_TUPLE(tsp, last_txnid),	/* last transaction id given out */
	ST_STATS_TUPLE(tsp, maxtxns),		/* maximum txns possible */
	ST_STATS_TUPLE(tsp, naborts),		/* number of aborted transactions */
	ST_STATS_TUPLE(tsp, nbegins),		/* number of begun transactions */
	ST_STATS_TUPLE(tsp, ncommits),		/* number of committed transactions */
	ST_STATS_TUPLE(tsp, nactive),		/* number of active transactions */
	ST_STATS_TUPLE(tsp, nsnapshot),		/* number of snapshot transactions */
	ST_STATS_TUPLE(tsp, maxnactive),	/* maximum active transactions */
	ST_STATS_TUPLE(tsp, maxnsnapshot),	/* maximum snapshot transactions */
	ST_STATS_TUPLE(tsp, region_wait),	/* Region lock granted after wait. */
	ST_STATS_TUPLE(tsp, region_nowait),	/* Region lock granted without wait. */
        ST_STATS_TUPLE(tsp, regsize),		/* Region size. */
        // End of list
        ERL_DRV_NIL, 
        ERL_DRV_LIST, 15+1,
        ERL_DRV_TUPLE, 2 
    };
    driver_send_term(port, pid, response, sizeof(response) / sizeof(response[0]));
}

static void do_async_put(void* arg)
{
    // Payload is: <<DbRef:32, Flags:32, KeyLen:32, Key:KeyLen, ValLen:32, Val:ValLen>>
    PortData* d = (PortData*)arg;

    // Get the database reference and flags from the payload
    int dbref = UNPACK_INT(d->work_buffer, 0);
    DB* db = G_DATABASES[dbref].db;
    unsigned int flags = UNPACK_INT(d->work_buffer, 4);

    // Setup DBTs 
    DBT key;
    DBT value;
    memset(&key, '\0', sizeof(DBT));
    memset(&value, '\0', sizeof(DBT));

    // Parse payload into DBTs
    key.size = UNPACK_INT(d->work_buffer, 8);
    key.data = UNPACK_BLOB(d->work_buffer, 12);
    value.size = UNPACK_INT(d->work_buffer, 12 + key.size);
    value.data = UNPACK_BLOB(d->work_buffer, 12 + key.size + 4);

    // Check CRC in value payload - first 4 bytes are CRC of rest of bytes
    assert(value.size >= 4);
    uint32_t calc_crc32 = bdberl_crc32(value.data+4, value.size-4);
    uint32_t buf_crc32 = *(uint32_t*) value.data;
    
    int rc;
    if (calc_crc32 != buf_crc32)
    {
        DBGCMD(d, "CRC-32 error on put data - buffer %08X calculated %08X.", buf_crc32, calc_crc32);
        rc = ERROR_INVALID_VALUE;
    }
    else
    {
        // Execute the actual put. All databases are opened with AUTO_COMMIT, so if msg->port->txn
        // is NULL, the put will still be atomic
        DBGCMD(d, "db->put(%p, %p, %p, &p, %08X) key=%p(%d) value=%p(%d)",
               db, d->txn, &key, &value, flags, key.data, key.size, value.data, value.size);
        rc = db->put(db, d->txn, &key, &value, flags);
        DBGCMDRC(d, rc);
    }

    // If any error occurs while we have a txn action, abort it
    if (d->txn && rc)
    {
        abort_txn(d);
    }
    else if (d->txn && d->async_op == CMD_PUT_COMMIT)
    {
        // Put needs to be followed by a commit -- saves us another pass through the driver and
        // threadpool queues
        rc = d->txn->commit(d->txn, 0);

        // Regardless of the txn commit outcome, we still need to invalidate the transaction
        d->txn = 0;
    }

    async_cleanup_and_send_rc(d, rc);
}

static void do_async_get(void* arg)
{
    // Payload is: << DbRef:32, Flags:32, KeyLen:32, Key:KeyLen >>
    PortData* d = (PortData*)arg;

    // Get the database object, using the provided ref
    int dbref = UNPACK_INT(d->work_buffer, 0);
    DB* db = G_DATABASES[dbref].db;

    // Extract operation flags
    unsigned flags = UNPACK_INT(d->work_buffer, 4);
    
    // Setup DBTs 
    DBT key;
    DBT value;
    memset(&key, '\0', sizeof(DBT));
    memset(&value, '\0', sizeof(DBT));

    // Parse payload into DBT
    key.size = UNPACK_INT(d->work_buffer, 8);
    key.data = UNPACK_BLOB(d->work_buffer, 12);

    // Allocate a buffer for the output value
#ifdef USE_DRIVER_REALLOC
    value.ulen = 4096;
    value.data = driver_alloc(value.ulen);
    value.flags = DB_DBT_USERMEM;
#else
    value.flags = DB_DBT_MALLOC;
#endif
    
    int rc = db->get(db, d->txn, &key, &value, flags);
#ifdef USE_DRIVER_REALLOC
    while (rc == DB_BUFFER_SMALL)
    {
        // Grow our value buffer
        value.ulen = value.size;
        value.size = 0;
        value.data = driver_realloc(value.data, value.ulen);
        rc = db->get(db, d->txn, &key, &value, flags);
    }
#endif
    // Check CRC - first 4 bytes are CRC of rest of bytes
    if (0 == rc)
    {
        assert(value.size >= 4);
        uint32_t calc_crc32 = bdberl_crc32(value.data+4, value.size-4);
        uint32_t buf_crc32 = *(uint32_t*) value.data;

        if (calc_crc32 != buf_crc32)
        {
            DBGCMD(d, "CRC-32 error on get data - buffer %08X calculated %08X.",
                   buf_crc32, calc_crc32);
            rc = ERROR_INVALID_VALUE;
        }
    }

    // Cleanup transaction as necessary
    if (rc && rc != DB_NOTFOUND && d->txn)
    {
        d->txn->abort(d->txn);
        d->txn = 0;
    }

    async_cleanup_and_send_kv(d, rc, &key, &value);

    // Finally, clean up value buffer (driver_send_term made a copy)
#ifdef USE_DRIVER_REALLOC
    driver_free(value.data);
#else
    if (value.data)
        free(value.data);
#endif
}

static void do_async_txnop(void* arg)
{
    PortData* d = (PortData*)arg;

    // Execute the actual begin/commit/abort
    int rc = 0;
    if (d->async_op == CMD_TXN_BEGIN)
    {
        DBGCMD(d, "G_DB_ENV->txn_begin(%p, 0, %p, %08X)", G_DB_ENV, d->txn, d->async_flags);
        rc = G_DB_ENV->txn_begin(G_DB_ENV, 0, &(d->txn), d->async_flags);
        DBGCMD(d, "rc = %s (%d) d->txn = %p", rc == 0 ? "ok" : rc_to_atom_str(rc), rc, d->txn);

    }
    else if (d->async_op == CMD_TXN_COMMIT)
    {
        assert(NULL != d->txn);
        DBGCMD(d, "d->txn->txn_commit(%p, %08X)", d->txn, d->async_flags);
        rc = d->txn->commit(d->txn, d->async_flags);
        DBGCMDRC(d, rc);
        d->txn = 0;
    }
    else 
    {
        assert(d->async_op == CMD_TXN_ABORT);
        abort_txn(d);
    }

    async_cleanup_and_send_rc(d, rc);
}


static void do_async_cursor_get(void* arg)
{
    // Payload is: << DbRef:32, Flags:32, KeyLen:32, Key:KeyLen >>
    PortData* d = (PortData*)arg;
    assert(NULL != d->cursor);
    
    // Setup DBTs 
    DBT key;
    DBT value;
    memset(&key, '\0', sizeof(DBT));
    memset(&value, '\0', sizeof(DBT));

    // Determine what type of cursor get to perform
    int flags = 0;
    switch (d->async_op)
    {
    case CMD_CURSOR_NEXT: 
        flags = DB_NEXT; break;
    case CMD_CURSOR_PREV: 
        flags = DB_PREV; break;
    default:
        flags = DB_CURRENT; 
    }

    // Execute the operation
    DBGCMD(d, "d->cursor->get(%p, &key, &value, flags);");
    int rc = d->cursor->get(d->cursor, &key, &value, flags);
    DBGCMDRC(d, rc);

    // Check CRC - first 4 bytes are CRC of rest of bytes
    if (0 == rc)
    {
        assert(value.size >= 4);
        uint32_t calc_crc32 = bdberl_crc32(value.data+4, value.size-4);
        uint32_t file_crc32 = *(uint32_t*) value.data;

        if (calc_crc32 != file_crc32)
        {
            rc = ERROR_INVALID_VALUE;
        }
    }

    // Cleanup as necessary; any sort of failure means we need to close the cursor and abort
    // the transaction
    if (rc && rc != DB_NOTFOUND)
    {
        DBG("cursor flags=%d rc=%d\n", flags, rc);

        d->cursor->close(d->cursor);
        d->cursor = 0;
        abort_txn(d);
    }

    async_cleanup_and_send_kv(d, rc, &key, &value);
}

static void do_async_truncate(void* arg)
{
    // Payload is: <<DbRef:32>>
    PortData* d = (PortData*)arg;

    // Get the database reference and flags from the payload
    int rc = 0;

    if (d->async_dbref == -1)
    {
        DBG("Truncating all open databases...\r\n");

        // Iterate over the whole database list skipping null entries
        int i = 0; // I hate C
        for ( ; i < G_DATABASES_SIZE; ++i)
        {
            Database* database = &G_DATABASES[i];
            if (database != NULL && database->db != 0)
            {
                DB* db = database->db;
                u_int32_t count = 0;

                DBGCMD(d, "db->truncate(%p, %p, %p) dbref=%d", db, d->txn, &count, 0, i);
                rc = db->truncate(db, d->txn, &count, 0);
                DBGCMD(d, "rc = %s (%d) count=%d", 
                       rc == 0 ? "ok" : rc_to_atom_str(rc), rc, count);

                if (rc != 0)
                {
                    break;
                }
            }
        }
    }
    else
    {
        DB* db = G_DATABASES[d->async_dbref].db;
        u_int32_t count = 0;
        DBGCMD(d, "db->truncate(%p, %p, %p, 0) dbref=%d", db, d->txn, &count, d->async_dbref);
        rc = db->truncate(db, d->txn, &count, 0);
        DBGCMD(d, "rc = %s (%d) count=%d", rc == 0 ? "ok" : rc_to_atom_str(rc), rc, count);
    }

    // If any error occurs while we have a txn action, abort it
    if (d->txn && rc)
    {
        abort_txn(d);
    }

    async_cleanup_and_send_rc(d, rc);
}


static void do_async_stat(void* arg)
{
    // Payload is: << DbRef:32, Flags:32 >>
    PortData* d = (PortData*)arg;

    // Get the database object, using the provided ref
    DB* db = G_DATABASES[d->async_dbref].db;
    DBTYPE type = DB_UNKNOWN;
    int rc = db->get_type(db, &type);
    if (rc != 0)
    {
        async_cleanup_and_send_rc(d, rc);
        return;
    }

    void *sp = NULL;
    rc = db->stat(db, d->txn, &sp, d->async_flags);
    if (rc != 0 || sp == NULL)
    {
        async_cleanup_and_send_rc(d, rc);
    }
    else
    {
        switch(type)
        {
            case DB_BTREE: /*FALLTHRU*/
            case DB_RECNO:
                async_cleanup_and_send_btree_stats(d, type == DB_BTREE ? "btree" :"recno", sp);
                break;
            case DB_HASH:
                async_cleanup_and_send_hash_stats(d, sp);
                break;
#ifdef ENABLE_QUEUE
            case DB_QUEUE:
                async_cleanup_and_send_queue_stats(d, sp);
                break;
#endif
            default: 
                async_cleanup_and_send_rc(d, ERROR_INVALID_DB_TYPE);
                break;
        }
    }
 
    // Finally, clean up value buffer (driver_send_term made a copy)
    if (NULL != sp)
    {
        free(sp);
    }
}

static void do_async_lock_stat(void* arg)
{
    // Payload is: <<Flags:32 >>
    PortData* d = (PortData*)arg;

    DB_LOCK_STAT *lsp = NULL;
    int rc = G_DB_ENV->lock_stat(G_DB_ENV, &lsp, d->async_flags);
    if (rc != 0 || lsp == NULL)
    {
        async_cleanup_and_send_rc(d, rc);
    }
    else
    {
        async_cleanup_and_send_lock_stats(d, lsp);
    }
 
    // Finally, clean up lock stats
    if (NULL != lsp)
    {
        free(lsp);
    }
}

static void do_async_log_stat(void* arg)
{
    // Payload is: <<Flags:32 >>
    PortData* d = (PortData*)arg;

    DB_LOG_STAT *lsp = NULL;
    int rc = G_DB_ENV->log_stat(G_DB_ENV, &lsp, d->async_flags);
    if (rc != 0 || lsp == NULL)
    {
        async_cleanup_and_send_rc(d, rc);
    }
    else
    {
        async_cleanup_and_send_log_stats(d, lsp);
    }
 
    // Finally, clean up stats
    if (NULL != lsp)
    {
        free(lsp);
    }
}

static void do_async_memp_stat(void* arg)
{
    // Payload is: <<Flags:32 >>
    PortData* d = (PortData*)arg;

    DB_MPOOL_STAT *gsp = NULL;
    DB_MPOOL_FSTAT **fsp = NULL;
    int rc = G_DB_ENV->memp_stat(G_DB_ENV, &gsp, &fsp, d->async_flags);
    if (rc != 0 || gsp == NULL)
    {
        async_cleanup_and_send_rc(d, rc);
    }
    else
    {
        async_cleanup_and_send_memp_stats(d, gsp, fsp);
    }
 
    // Finally, clean up stats
    if (NULL != gsp)
    {
        free(gsp);
    }
    if (NULL != fsp)
    {
        free(fsp);
    }
}

static void do_async_mutex_stat(void* arg)
{
    // Payload is: <<Flags:32 >>
    PortData* d = (PortData*)arg;

    DB_MUTEX_STAT *msp = NULL;
    int rc = G_DB_ENV->mutex_stat(G_DB_ENV, &msp, d->async_flags);
    if (rc != 0 || msp == NULL)
    {
        async_cleanup_and_send_rc(d, rc);
    }
    else
    {
        async_cleanup_and_send_mutex_stats(d, msp);
    }
 
    // Finally, clean up stats
    if (NULL != msp)
    {
        free(msp);
    }
}


static void do_async_txn_stat(void* arg)
{
    // Payload is: <<Flags:32 >>
    PortData* d = (PortData*)arg;

    DB_TXN_STAT *tsp = NULL;
    int rc = G_DB_ENV->txn_stat(G_DB_ENV, &tsp, d->async_flags);
    if (rc != 0 || tsp == NULL)
    {
        async_cleanup_and_send_rc(d, rc);
    }
    else
    {
        async_cleanup_and_send_txn_stats(d, tsp);
    }
 
    // Finally, clean up stats
    if (NULL != tsp)
    {
        free(tsp);
    }
}


static void do_sync_data_dirs_info(PortData *d)
{
    // Get DB_HOME and find the real path
    const char *db_home = NULL;
    const char *data_dir = NULL;
    const char **data_dirs = NULL;
    char db_home_realpath[PATH_MAX+1];
    char data_dir_realpath[PATH_MAX+1];
    int got_db_home = 0;

    // Lookup the environment and add it if not explicitly included in the data_dirs
    int rc = G_DB_ENV->get_home(G_DB_ENV, &db_home);
    if (rc != 0 || NULL == db_home)
    {
        // If no db_home we'll have to rely on whatever the global environment is configured with
        got_db_home = 1;
    }
    else
    {
        if (NULL == realpath(db_home, db_home_realpath))
            rc = errno;
    }

    // Get the data first
    rc = G_DB_ENV->get_data_dirs(G_DB_ENV, &data_dirs);
    int i;
    for (i = 0; 0 == rc && NULL != data_dirs && NULL != data_dirs[i]; i++)
    {
        data_dir = data_dirs[i];

        if (!got_db_home)
        {
            // Get the real path of the data dir
            if (NULL == realpath(data_dir, data_dir_realpath))
            {
                rc = errno;
            }
            else
            {
                // Set got_db_home if it matches
                if (0 == strcmp(data_dir_realpath, db_home_realpath))
                {
                    got_db_home = 1;
                }
            }
        }
        
        if (0 == rc)
        {
            rc = send_dir_info(d->port, d->port_owner, data_dir);
        }
    }

    // BDB always searches the environment home too so add it to the list
    if (!got_db_home && rc == 0)
    {
        rc = send_dir_info(d->port, d->port_owner, db_home);
    }

    // Send the return code - will termiante the receive loop in bdberl.erl
    send_rc(d->port, d->port_owner, rc);
}


static void* zalloc(unsigned int size)
{
    void* res = driver_alloc(size);
    memset(res, '\0', size);
    return res;
}

#define zfree(p) driver_free(p)

static int add_portref(int dbref, ErlDrvPort port)
{
    PortList* current = G_DATABASES[dbref].ports;
    if (current)
    {
        PortList* last = 0;
        do
        {
            // If the current item matches our port, bail -- nothing to do here
            if (current->port == port)
            {
                return 0;
            }

            last = current;
            current = current->next;            
        } while (current != 0);

        // At the end of the list -- allocate a new entry for this port
        current = (PortList*)zalloc(sizeof(PortList));
        current->port = port;
        last->next = current;
        return 1;
    }
    else
    {
        // Current was initially NULL, so alloc the first one and add it.
        current = zalloc(sizeof(PortList));
        current->port = port;
        G_DATABASES[dbref].ports = current;
        return 1;
    }
}

static int del_portref(int dbref, ErlDrvPort port)
{
    PortList* current = G_DATABASES[dbref].ports;
    PortList* last = 0;
    assert(NULL != current);
    while (current)
    {
        if (current->port == port)
        {
            // Found our match -- look back and connect the last item to our next
            if (last)
            {
                last->next = current->next;
            }
            else
            {
                G_DATABASES[dbref].ports = current->next;
            }

            // Delete this entry
            zfree(current);
            return 1;
        }

        last = current;
        current = current->next;
    }

    // Didn't delete anything
    return 0;
}

/**
 * Add a db reference to a port's DbRefList. Returns 1 if added; 0 if already present
 */
static int add_dbref(PortData* data, int dbref)
{
    DbRefList* current = data->dbrefs;
    if (current)
    {
        DbRefList* last = 0;
        do
        {
            if (current->dbref == dbref)
            {
                return 0;
            }

            last = current;
            current = current->next;
        } while (current != 0);

        // At the end of the list -- allocate a new entry 
        current = zalloc(sizeof(DbRefList));
        current->dbref = dbref;
        last->next = current;
        return 1;
    }
    else
    {
        // Current was initially NULL, so alloc the first one
        current = zalloc(sizeof(DbRefList));
        current->dbref = dbref;
        data->dbrefs = current;
        return 1;
    }
}

/**
 * Delete a db reference from a port's DbRefList. Returns 1 if deleted; 0 if not 
 */
static int del_dbref(PortData* data, int dbref)
{
    DbRefList* current = data->dbrefs;
    DbRefList* last = 0;
    assert(NULL != current);

    while (current)
    {
        if (current->dbref == dbref)
        {
            // Found our match -- look back and connect the last item to our next
            if (last)
            {
                last->next = current->next;
            }
            else
            {
                data->dbrefs = current->next;
            }

            // Delete this entry
            zfree(current);
            return 1;
        }

        last = current;
        current = current->next;
    }

    // Didn't delete anything
    return 0;
}

/**
 * Validate that a provided dbref is currently opened by a port. Return 1 if true; 0 if false.
 */
static int has_dbref(PortData* data, int dbref) 
{
    DbRefList* current = data->dbrefs;
    while (current)
    {
        if (current->dbref == dbref)
        {
            return 1;
        }

        current = current->next;
    }
    return 0;
}

/**
 * Allocate a Database structure; find first available slot in G_DATABASES and return the
 * index of it. If no free slots are available, return -1 
 */
static int alloc_dbref()
{
    int i;
    for (i = 0; i < G_DATABASES_SIZE; i++)
    {
        if (G_DATABASES[i].db == 0)
        {
            return i;
        }
    }

    return -1;
}


/**
 * Utility thread sleep - returns true if being signalled to exit
 * otherwise false if timeout exceeded.
 */
int util_thread_usleep(unsigned int usecs)
{
    fd_set fds;
    struct timeval sleep_until;
    struct timeval sleep_for;
    struct timeval now;
    struct timeval tv;
    int done;
    int nfds = (G_BDBERL_PIPE[0] > G_BDBERL_PIPE[1] ? G_BDBERL_PIPE[0] : G_BDBERL_PIPE[1]) + 1;

    memset(&sleep_for, 0, sizeof(sleep_for));
    sleep_for.tv_sec  = usecs / 1000000;
    sleep_for.tv_usec = usecs % 1000000;

    gettimeofday(&now, NULL);
    timeradd(&now, &sleep_for, &sleep_until);

    do
    {
        FD_ZERO(&fds);
        FD_SET(G_BDBERL_PIPE[0], &fds);  // read fd of pipe

        // Check if we have slept long enough
        gettimeofday(&now, NULL);
        if (timercmp(&now, &sleep_until, >))
        {
            done = 1;
        }
        else // take a nap
        {
            // work out the remaining time to sleep on the fd for - make sure that this time
            // is less than or equal to the original sleep time requested, just in
            // case the system time is being adjusted.  If the adjustment would result
            // in a longer wait then cap it at the sleep_for time.
            timersub(&sleep_until, &now, &tv);
            if (timercmp(&tv, &sleep_for, >))
            {
                memcpy(&tv, &sleep_for, sizeof(tv));
            }

            done = 1;
            if (-1 == select(nfds, &fds, NULL, NULL,  &tv))
            {
                if (EINTR == errno) // a signal woke up select, back to sleep for us
                {
                    done = 0;
                }
                // any other signals can return to the caller to fail safe as it
                // doesn't matter if the util threads get woken up more often
            }
            else if (FD_ISSET(G_BDBERL_PIPE[0], &fds))
            {
                done = 1;
            }
        }
    } while (!done);

    return FD_ISSET(G_BDBERL_PIPE[0], &fds);
}

/**
 * Thread function that runs the deadlock checker periodically
 */
static void* deadlock_check(void* arg)
{
    while(G_DEADLOCK_CHECK_ACTIVE)
    {
        // Run the lock detection
        int count = 0;
        int rc = G_DB_ENV->lock_detect(G_DB_ENV, 0, DB_LOCK_DEFAULT, &count);
        if (0 != rc)
        {
            DBG("lock_detect returned %s(%d)\n", db_strerror(rc), rc);
        }
        if (count > 0)
        {
            DBG("Rejected deadlocks: %d\r\n", count);
        }

        if (G_DEADLOCK_CHECK_INTERVAL > 0)
        {
            util_thread_usleep(G_DEADLOCK_CHECK_INTERVAL * 1000);
        }
    }

    DBG("Deadlock checker exiting.\r\n");
    return 0;
}

/**
 * Thread function that does trickle writes or checkpointing at fixed intervals.
 */
static void* checkpointer(void* arg)
{
    time_t last_checkpoint_time = time(0);
    time_t last_trickle_time = time(0);

    while (G_CHECKPOINT_ACTIVE)
    {
        time_t now = time(0);
        if (now - last_checkpoint_time > G_CHECKPOINT_INTERVAL)
        {
            // Time to checkpoint and cleanup log files
            int checkpoint_rc = G_DB_ENV->txn_checkpoint(G_DB_ENV, 0, 0, 0);

            // Mark the time before starting log_archive so we can know how long it took
            time_t log_now = time(0);
            int log_rc = G_DB_ENV->log_archive(G_DB_ENV, NULL, DB_ARCH_REMOVE);
            time_t finish_now = time(0);

            // Bundle up the results and elapsed time into a message for the logger
            ErlDrvTermData response[] = { ERL_DRV_ATOM, driver_mk_atom("bdb_checkpoint_stats"),
                                          ERL_DRV_UINT, log_now - now,        /* Elapsed seconds for checkpoint */
                                          ERL_DRV_UINT, finish_now - log_now, /* Elapsed seconds for log_archive */
                                          ERL_DRV_INT, checkpoint_rc,         /* Return code of checkpoint */
                                          ERL_DRV_INT, log_rc,                /* Return code of log_archive */
                                          ERL_DRV_TUPLE, 5};
            send_log_message(response, sizeof(response));

            // Note the time of this checkpoint completion
            last_checkpoint_time = finish_now;
        }
        else if (now - last_trickle_time > G_TRICKLE_INTERVAL)
        {
            // Time to run the trickle operation again
            int pages_wrote = 0;
            int rc = G_DB_ENV->memp_trickle(G_DB_ENV, G_TRICKLE_PERCENTAGE, &pages_wrote);
            time_t finish_now = time(0);

            // Bundle up the results and elapsed time into a message for the logger
            ErlDrvTermData response[] = { ERL_DRV_ATOM, driver_mk_atom("bdb_trickle_stats"),
                                          ERL_DRV_UINT, finish_now - now,        /* Elapsed seconds for trickle */
                                          ERL_DRV_UINT, pages_wrote,             /* Number of pages flushed */
                                          ERL_DRV_INT, rc,                       /* Return code of checkpoint */
                                          ERL_DRV_TUPLE, 4};
            send_log_message(response, sizeof(response));

            // Note the time of this trickle completion
            last_trickle_time = finish_now;
        }

        // Always sleep for one second
        util_thread_usleep(1000000);
    }

    return 0;
}

static void bdb_errcall(const DB_ENV* dbenv, const char* errpfx, const char* msg)
{
    ErlDrvTermData response[] = { ERL_DRV_ATOM, driver_mk_atom("bdb_error_log"),
                                  ERL_DRV_STRING, (ErlDrvTermData)msg, (ErlDrvUInt)strlen(msg),
                                  ERL_DRV_TUPLE, 2};
    send_log_message(response, sizeof(response));
}

static void bdb_msgcall(const DB_ENV* dbenv, const char* msg)
{
    ErlDrvTermData response[] = { ERL_DRV_ATOM, driver_mk_atom("bdb_info_log"),
                                  ERL_DRV_STRING, (ErlDrvTermData)msg, (ErlDrvUInt)strlen(msg),
                                  ERL_DRV_TUPLE, 2};
    send_log_message(response, sizeof(response));
}

static void send_log_message(ErlDrvTermData* msg, int elements)
{
    if (G_LOG_PORT)
    {
        READ_LOCK(G_LOG_RWLOCK);
        driver_send_term(G_LOG_PORT, G_LOG_PID, msg, elements / sizeof(msg[0]));
        READ_UNLOCK(G_LOG_RWLOCK);
    }
}


