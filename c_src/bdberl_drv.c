/* -------------------------------------------------------------------
 *
 * bdberl: Berkeley DB Driver for Erlang
 * Copyright (c) 2008-9 The Hive http://www.thehive.com/
 * Authors: Dave "dizzyd" Smith <dizzyd@dizzyd.com>
 *          Phil Toland <phil.toland@gmail.com>
 *          Jon Meredith <jon@jonmeredith.com>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
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
#include "bdberl_stats.h"
#include "bin_helper.h"

/**
 * Driver functions
 */
static ErlDrvData bdberl_drv_start(ErlDrvPort port, char* buffer);

static void bdberl_drv_stop(ErlDrvData handle);

static void bdberl_drv_finish();

static int bdberl_drv_control(ErlDrvData handle, unsigned int cmd, 
                              char* inbuf, int inbuf_sz, 
                              char** outbuf, int outbuf_sz);

/** 
 * Driver Entry
 */
ErlDrvEntry bdberl_drv_entry = 
{
    NULL,			/* F_PTR init, N/A */
    bdberl_drv_start,		/* L_PTR start, called when port is opened */
    bdberl_drv_stop,		/* F_PTR stop, called when port is closed */
    NULL,			/* F_PTR output, called when erlang has sent */
    NULL,                       /* F_PTR ready_input, called when input descriptor ready */
    NULL,			/* F_PTR ready_output, called when output descriptor ready */
    "bdberl_drv",               /* driver_name */
    bdberl_drv_finish,          /* F_PTR finish, called when unloaded */
    NULL,			/* handle */
    bdberl_drv_control,		/* F_PTR control, port_command callback */
    NULL,			/* F_PTR timeout, reserved */
    NULL,                       /* F_PTR outputv, reserved */
    NULL,                       /* F_PTR ready_async */
    NULL,                       /* F_PTR flush */
    NULL,                       /* F_PTR call */
    NULL,                       /* F_PTR event */
    ERL_DRV_EXTENDED_MARKER,        
    ERL_DRV_EXTENDED_MAJOR_VERSION, 
    ERL_DRV_EXTENDED_MINOR_VERSION,
    ERL_DRV_FLAG_USE_PORT_LOCKING,
    NULL,                        /* Reserved */
    NULL                         /* F_PTR process_exit */
};

/**
 * Function prototypes
 */
static int check_non_neg_env(char *env, unsigned int *val_ptr);
static int check_pos_env(char *env, unsigned int *val_ptr);

static int open_database(const char* name, DBTYPE type, unsigned int flags, PortData* data, int* dbref_res);
static int close_database(int dbref, unsigned flags, PortData* data);
static void check_all_databases_closed();

static int delete_database(const char* name, PortData *data);

static void get_info(int target, void* values, BinHelper* bh);

static void do_async_put(void* arg);
static void do_async_get(void* arg);
static void do_async_txnop(void* arg);
static void do_async_cursor_get(void* arg);
static void do_async_truncate(void* arg);
static void do_sync_data_dirs_info(PortData *p);
static void do_sync_driver_info(PortData *d);

static int send_dir_info(ErlDrvPort port, ErlDrvTermData pid, const char *path);

static int add_dbref(PortData* data, int dbref);
static int del_dbref(PortData* data, int dbref);

static int add_portref(int dbref, ErlDrvPort port);
static int del_portref(int dbref, ErlDrvPort port);

static int alloc_dbref();
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
 * G_DATABASES_MUTEX.
 */
static Database*     G_DATABASES        = 0;
static unsigned int  G_DATABASES_SIZE   = 0;
static ErlDrvMutex*  G_DATABASES_MUTEX  = 0;
static hive_hash*    G_DATABASES_NAMES  = 0;


/**
 * Deadlock detector thread variables. We run a single thread per VM to detect deadlocks within
 * our global environment. G_DEADLOCK_CHECK_INTERVAL is the time between runs in milliseconds.
 */
static ErlDrvTid    G_DEADLOCK_THREAD         = 0;
static unsigned int G_DEADLOCK_CHECK_ACTIVE   = 1;
static unsigned int G_DEADLOCK_CHECK_INTERVAL = 100;  /* 100 milliseconds */


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
static unsigned int G_NUM_GENERAL_THREADS = 10;
static unsigned int G_NUM_TXN_THREADS = 10;
static TPool* G_TPOOL_GENERAL = NULL;
static TPool* G_TPOOL_TXNS    = NULL;


/**
 * Helpful macros
 */
#ifdef DEBUG
#  define DBG(...) fprintf(stderr, __VA_ARGS__)
#  define DBGCMD(P, ...)   bdberl_dbgcmd(P, __VA_ARGS__)
#  define DBGCMDRC(P, ...) bdberl_dbgcmdrc(P, __VA_ARGS__)
static void bdberl_dbgcmd(PortData *d, const char *fmt, ...);
static void bdberl_dbgcmdrc(PortData *d, int rc);
#else
#  define DBG(arg1,...)
#  define DBGCMD(d, fmt, ...)
#  define DBGCMDRC(d, rc) { while (0) { rc++; } }  // otherwise get unused variable error
#endif


#define LOCK_DATABASES(P)                                               \
    do                                                                  \
    {                                                                   \
        DBG("threadid %p port %p: locking G_DATABASES\r\n", erl_drv_thread_self(), P);   \
        erl_drv_mutex_lock(G_DATABASES_MUTEX);                          \
        DBG("threadid %p port %p: locked G_DATABASES\r\n", erl_drv_thread_self(), P);    \
    } while(0)

#define UNLOCK_DATABASES(P)                                             \
    do                                                                  \
    {                                                                   \
        DBG("threadid %p port %p: unlocking G_DATABASES\r\n", erl_drv_thread_self(), P); \
        erl_drv_mutex_unlock(G_DATABASES_MUTEX);                        \
        DBG("threadid %p port %p: unlocked G_DATABASES\r\n", erl_drv_thread_self(), P);  \
    } while (0)                                                         


#define READ_LOCK(L) erl_drv_rwlock_rlock(L)
#define READ_UNLOCK(L) erl_drv_rwlock_runlock(L)
#define WRITE_LOCK(L) erl_drv_rwlock_rwlock(L)
#define WRITE_UNLOCK(L) erl_drv_rwlock_rwunlock(L)


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
        DBG("G_DB_ENV->open(%p, 0, %08X, 0)", &G_DB_ENV, flags);
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
        assert(pipe(G_BDBERL_PIPE) == 0);

        // Use the BDBERL_MAX_DBS environment value to determine the max # of
        // databases to permit the VM to open at once. Defaults to 1024.
        G_DATABASES_SIZE = 1024;
        check_pos_env("BDBERL_MAX_DBS", &G_DATABASES_SIZE);

        // Use the BDBERL_TRICKLE_TIME and BDBERL_TRICKLE_PERCENTAGE to control how often
        // the trickle writer runs and what percentage of pages should be flushed.
        check_pos_env("BDBERL_TRICKLE_TIME", &G_TRICKLE_INTERVAL);
        check_pos_env("BDBERL_TRICKLE_PERCENTAGE", &G_TRICKLE_PERCENTAGE);

        // Set the deadlock interval
        check_pos_env("BDBERL_DEADLOCK_CHECK_INTERVAL", &G_DEADLOCK_CHECK_INTERVAL);

        // Initialize default page size
        unsigned int page_size;
        if (check_pos_env("BDBERL_PAGE_SIZE", &page_size))
        {
            if (page_size != 0 && ((page_size & (~page_size +1)) == page_size))
            {
                G_PAGE_SIZE = page_size;
            }
            else
            {
                fprintf(stderr, "Ignoring \"BDBERL_PAGE_SIZE\" value %u - not power of 2\r\n",
                    page_size);
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
        G_DATABASES_MUTEX = erl_drv_mutex_create("bdberl_drv: G_DATABASES_MUTEX");
        G_DATABASES_NAMES = hive_hash_new(G_DATABASES_SIZE);            

        // Startup deadlock check thread
        erl_drv_thread_create("bdberl_drv_deadlock_checker", &G_DEADLOCK_THREAD, 
                              &deadlock_check, 0, 0);

        // Use the BDBERL_CHECKPOINT_TIME environment value to determine the
        // interval between transaction checkpoints. Defaults to 1 hour.
        check_pos_env("BDBERL_CHECKPOINT_TIME", &G_CHECKPOINT_INTERVAL);

        // Startup checkpoint thread
        erl_drv_thread_create("bdberl_drv_checkpointer", &G_CHECKPOINT_THREAD,
                              &checkpointer, 0, 0);

        // Startup our thread pools
        check_pos_env("BDBERL_NUM_GENERAL_THREADS", &G_NUM_GENERAL_THREADS);
        G_TPOOL_GENERAL = bdberl_tpool_start(G_NUM_GENERAL_THREADS);

        check_pos_env("BDBERL_NUM_TXN_THREADS", &G_NUM_TXN_THREADS);
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
        DBGCMD(d, "Stopping port %p - cancelling async job %p\r\n", d->port, d->async_job);

        // Drop the lock prior to starting the wait for the async process
        erl_drv_mutex_unlock(d->port_lock);

        bdberl_tpool_cancel(d->async_pool, d->async_job);
        DBGCMD(d, "Canceled async job for port: %p\r\n", d->port);
    }
    else
    {
        // If there was no async job, drop the lock -- not needed
        erl_drv_mutex_unlock(d->port_lock);
    }
    
    // Cleanup the port lock
    erl_drv_mutex_destroy(d->port_lock);

    // If a cursor is open, close it
    DBG("Stopping port %p - cleaning up cursors (%p) and transactions (%p)\r\n", d->port,
        d->cursor, d->txn);

    if (d->cursor)
    {
        d->cursor->close(d->cursor);
    }

    // If a txn is currently active, terminate it. 
    abort_txn(d);

    // Close all the databases we previously opened
    DBG("Stopping port %p - closing all dbrefs\r\n", d->port);
    while (d->dbrefs)
    {
        int dbref = d->dbrefs->dbref;
        if (close_database(dbref, 0, d) != ERROR_NONE)
        {
            DBG("Stopping port %p could not close dbref %d\r\n", d->port, dbref);
        }
    }

    // If this port was registered as the endpoint for logging, go ahead and 
    // remove it. Note that we don't need to lock to check this since we only
    // unregister if it's already initialized to this port.
    if (G_LOG_PORT == d->port)
    {
        DBG("Stopping port %p - removing logging port\r\n", d->port);

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
    if (G_TPOOL_GENERAL != NULL)
    {
        bdberl_tpool_stop(G_TPOOL_GENERAL);
        G_TPOOL_GENERAL = NULL;
    }

    if (G_TPOOL_TXNS != NULL)
    {
        bdberl_tpool_stop(G_TPOOL_TXNS);
        G_TPOOL_TXNS = NULL;
    }

    // Signal the utility threads time is up
    G_TRICKLE_ACTIVE = 0;
    G_DEADLOCK_CHECK_ACTIVE = 0;
    G_CHECKPOINT_ACTIVE = 0;

    // Close the writer fd on the pipe to signal finish to the utility threads
    if (G_BDBERL_PIPE[1] != -1)
    {
        close(G_BDBERL_PIPE[1]);
        G_BDBERL_PIPE[1] = -1;
    }

    // Wait for the deadlock checker to shutdown -- then wait for it
    if (G_DEADLOCK_THREAD != 0)
    {
        erl_drv_thread_join(G_DEADLOCK_THREAD, 0);
        G_DEADLOCK_THREAD = 0;
    }

    // Wait for the checkpointer to shutdown -- then wait for it
    if (G_CHECKPOINT_THREAD != 0)
    {
        erl_drv_thread_join(G_CHECKPOINT_THREAD, 0);
        G_CHECKPOINT_THREAD = 0;
    }

    // Close the reader fd on the pipe now utility threads are closed
    if (G_BDBERL_PIPE[0] != -1)
    {
        close(G_BDBERL_PIPE[0]);
    }
    G_BDBERL_PIPE[0] = -1;

    // Cleanup and shut down the BDB environment. Note that we assume
    // all ports have been released and thuse all databases/txns/etc are also gone.
    if (G_DB_ENV != NULL)
    {
        check_all_databases_closed();
        G_DB_ENV->close(G_DB_ENV, 0);
        G_DB_ENV = NULL;
    }
    if (G_DATABASES != NULL)
    {
        driver_free(G_DATABASES);
        G_DATABASES = NULL;
    }
    if (G_DATABASES_MUTEX != NULL)
    {
        erl_drv_mutex_destroy(G_DATABASES_MUTEX);
        G_DATABASES_MUTEX = NULL;
    }

    if (G_DATABASES_NAMES != NULL)
    {
        hive_hash_destroy(G_DATABASES_NAMES);
        G_DATABASES_NAMES = NULL;
    }
    
    // Release the logging rwlock
    if (G_LOG_RWLOCK != NULL)
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
            bdberl_send_rc(d->port, d->port_owner, rc);
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
        bdberl_send_rc(d->port, d->port_owner, rc);
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
        bdberl_txn_tpool_run(&do_async_txnop, d, 0, &d->async_job);

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
        bdberl_txn_tpool_run(&do_async_txnop, d, 0, &d->async_job);

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
            bdberl_send_rc(d->port, d->port_owner, ERROR_NO_TXN);
            RETURN_INT(0, outbuf);
        }

        // Inbuf is: << DbRef:32, Rest/binary>>
        int dbref = UNPACK_INT(inbuf, 0);

        // Make sure this port currently has dbref open -- if it doesn't, error out. Of note,
        // if it's in our list, we don't need to grab the RWLOCK, as we don't have to worry about
        // the underlying handle disappearing since we have a reference.
        if (bdberl_has_dbref(d, dbref))
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
            d->async_dbref = dbref;
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
            bdberl_general_tpool_run(fn, d, 0, &d->async_job);
        
            // Let caller know that the operation is in progress
            // Outbuf is: <<0:32>>
            RETURN_INT(0, outbuf);
        }
        else
        {
            // Invalid dbref
            bdberl_send_rc(d->port, d->port_owner, ERROR_INVALID_DBREF);
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
        if (bdberl_has_dbref(d, dbref))
        {
            // Grab the database handle and open the cursor
            DB* db = G_DATABASES[dbref].db;
            int rc = db->cursor(db, d->txn, &(d->cursor), flags);
            bdberl_send_rc(d->port, d->port_owner, rc);
            RETURN_INT(0, outbuf);
        }
        else
        {
            bdberl_send_rc(d->port, d->port_owner, ERROR_INVALID_DBREF);
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
        bdberl_general_tpool_run(&do_async_cursor_get, d, 0, &d->async_job);

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
        bdberl_send_rc(d->port, d->port_owner, rc);
        RETURN_INT(0, outbuf);
    }
    case CMD_REMOVE_DB:
    {
        FAIL_IF_ASYNC_PENDING(d, outbuf);
        FAIL_IF_TXN_OPEN(d, outbuf);

        // Inbuf is: << dbname/bytes, 0:8 >>
        const char* dbname = UNPACK_STRING(inbuf, 0);
        int rc = delete_database(dbname, d);
        bdberl_send_rc(d->port, d->port_owner, rc);
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
        if (dbref == -1 || bdberl_has_dbref(d, dbref))
        {
            // Mark the port as busy and then schedule the appropriate async operation
            d->async_op = cmd;
            d->async_dbref = dbref;
            bdberl_general_tpool_run(&do_async_truncate, d, 0, &d->async_job);

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
    case CMD_DB_STAT:          /*FALLTHRU*/
    case CMD_DB_STAT_PRINT:    /*FALLTHRU*/
    case CMD_ENV_STAT_PRINT:   /*FALLTHRU*/
    case CMD_LOCK_STAT:        /*FALLTHRU*/
    case CMD_LOCK_STAT_PRINT:  /*FALLTHRU*/
    case CMD_LOG_STAT:         /*FALLTHRU*/
    case CMD_LOG_STAT_PRINT:   /*FALLTHRU*/
    case CMD_MEMP_STAT:        /*FALLTHRU*/
    case CMD_MEMP_STAT_PRINT:  /*FALLTHRU*/
    case CMD_MUTEX_STAT:       /*FALLTHRU*/
    case CMD_MUTEX_STAT_PRINT: /*FALLTHRU*/
    case CMD_TXN_STAT:         /*FALLTHRU*/
    case CMD_TXN_STAT_PRINT:   /*FALLTHRU*/
    {
        FAIL_IF_ASYNC_PENDING(d, outbuf);
        return bdberl_stats_control(d, cmd, inbuf, inbuf_sz, outbuf, outbuf_sz);
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
        if (rc == 0 && NULL == lg_dir)
        {
            rc = G_DB_ENV->get_home(G_DB_ENV, &lg_dir);
        }
        // Send info if we can get a dir, otherwise return the error
        if (rc == 0)
        {
            // send a dirinfo message - will send an error message on a NULL lg_dir
            send_dir_info(d->port, d->port_owner, lg_dir);
        }
        else
        {
            bdberl_send_rc(d->port, d->port_owner, rc);
        }

        // Let caller know that the operation is in progress
        // Outbuf is: <<0:32>>
        RETURN_INT(0, outbuf);
    }
    case CMD_DRIVER_INFO:
    {
        FAIL_IF_ASYNC_PENDING(d, outbuf);

        do_sync_driver_info(d);
       
        // Let caller know that the operation is in progress
        // Outbuf is: <<0:32>>
        RETURN_INT(0, outbuf);
    }
    }
    *outbuf = 0;
    return 0;
}


// Check if an environment variable is set to a non-negative value (>=0)
// Returns 1 and sets the destination of val_ptr to the converted value
// Otherwise returns 0
static int check_non_neg_env(char *env, unsigned int *val_ptr)
{
    char val_str[64];
    size_t val_size = sizeof(val_str);

    if (erl_drv_getenv(env, val_str, &val_size) >= 0)
    {
        errno = 0;
        long long val = strtoll(val_str, NULL, 0);
        if (val == 0 && errno == EINVAL)
        {
            fprintf(stderr, "Ignoring \"%s\" value \"%s\" - invalid value\r\n", env, val_str);
            return 0;
        }
        if (val <= 0 || val > UINT_MAX)
        {
            fprintf(stderr, "Ignoring \"%s\" value \"%lld\" - out of range\r\n", env, val);
            return 0;
        }
        unsigned int uival = (unsigned int) val;
        DBG("Using \"%s\" value %u\r\n", env, uival);
        *val_ptr = uival;
        return 1;
    }
    else
    {
        return 0;
    }
}


// Check if an environment variable is set to a positive value (>0)
// Returns 1 and sets the destination of val_ptr to the converted value
// Otherwise returns 0

static int check_pos_env(char *env, unsigned int *val_ptr)
{
    unsigned int original_val = *val_ptr;
    if (check_non_neg_env(env, val_ptr))
    {
        if (*val_ptr > 0)
        {
            return 1;
        }
        else
        {
            fprintf(stderr, "Ignoring \"%s\" value \"%u\" - out of range\r\n", env, *val_ptr);
            *val_ptr = original_val;
            return 0;
        }
    }
    else
    {
        return 0;
    }
}

DB_ENV* bdberl_db_env(void)
{
    assert(G_DB_ENV != NULL);
    return G_DB_ENV;
}

DB* bdberl_lookup_dbref(int dbref)
{
    assert(G_DATABASES != NULL);
    assert(dbref >= 0);
    assert(dbref < G_DATABASES_SIZE);
    assert(G_DATABASES[dbref].db != NULL);
    return G_DATABASES[dbref].db;
}

void bdberl_general_tpool_run(TPoolJobFunc main_fn, PortData* d, TPoolJobFunc cancel_fn,
    TPoolJob** job_ptr)
{
    d->async_pool = G_TPOOL_GENERAL;
    bdberl_tpool_run(d->async_pool, main_fn, d, NULL, job_ptr);
}

void bdberl_txn_tpool_run(TPoolJobFunc main_fn, PortData* d, TPoolJobFunc cancel_fn,
    TPoolJob** job_ptr)
{
    d->async_pool = G_TPOOL_TXNS;
    bdberl_tpool_run(d->async_pool, main_fn, d, NULL, job_ptr);
}

static int open_database(const char* name, DBTYPE type, unsigned int flags, PortData* data, int* dbref_res)
{
    *dbref_res = -1;

    LOCK_DATABASES(data->port);

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
            // Add a reference to this port
            add_portref(dbref, data->port);

            // Release RW lock and return the ref
            UNLOCK_DATABASES(data->port);
            *dbref_res = dbref;
            return 0;
        }
        else
        {
            // Already in our list of opened databases -- unlock and return the reference
            UNLOCK_DATABASES(data->port);
            *dbref_res = dbref;
            return 0;
        }
    }
    else
    { 
        // Database hasn't been created while we were waiting on write lock, so
        // create/open it

        // Find the first available slot in G_DATABASES; the index will be our
        // reference for database operations
        int dbref = alloc_dbref();
        if (dbref < 0)
        {
            // No more slots available 
            UNLOCK_DATABASES(data->port);
            return ERROR_MAX_DBS;
        }

        // Create the DB handle
        DB* db = NULL;
        DBGCMD(data, "db_create(&db, %p, 0);", G_DB_ENV);
        int rc = db_create(&db, G_DB_ENV, 0);
        DBGCMD(data, "rc = %s (%d) db = %p", rc == 0 ? "ok" : bdberl_rc_to_atom_str(rc), rc, db);
        if (rc != 0)
        {
            // Failure while creating the database handle -- drop our lock and return 
            // the code
            UNLOCK_DATABASES(data->port);
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
            UNLOCK_DATABASES(data->port);
            return rc;
        }

        // Database is open. Store all the data into the allocated ref
        assert(db != NULL);
        G_DATABASES[dbref].db = db;
        G_DATABASES[dbref].name = strdup(name);
        G_DATABASES[dbref].ports = zalloc(sizeof(PortList));
        G_DATABASES[dbref].ports->port = data->port;

        // Make entry in hash table of names
        hive_hash_add(G_DATABASES_NAMES, G_DATABASES[dbref].name, &(G_DATABASES[dbref]));

        // Drop the write lock
        UNLOCK_DATABASES(data->port);

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
        LOCK_DATABASES(data->port);

        assert(G_DATABASES[dbref].db != 0);
        assert(G_DATABASES[dbref].ports != 0);

        // Now disassociate this port from the database's port list
        assert(del_portref(dbref, data->port) == 1);

        // Finally, if there are no other references to the database, close out
        // the database completely
        Database* database = &G_DATABASES[dbref];
        int rc = ERROR_NONE;
        if (database->ports == 0)
        {
            // Close out the BDB handle
            DBGCMD(data, "database->db->close(%p, %08x) (for dbref %d)", database->db, flags, dbref);
            rc = database->db->close(database->db, flags);
            DBGCMDRC(data, rc);

            // Remove the entry from the names map
            hive_hash_remove(G_DATABASES_NAMES, database->name);
            free((char*)database->name);

            // Zero out the whole record
            memset(database, '\0', sizeof(Database));
        }

        UNLOCK_DATABASES(data->port);
        return rc;
    }
    else
    {
        return ERROR_INVALID_DBREF;
    }
}


// Called on driver shutdown to ensure all databases are closed.
// This should be unnecessary if all the dbref/port code has worked properly,
// but it is very important for BDB to shutdown cleanly so a final check can't hurt.
static void check_all_databases_closed()
{
    LOCK_DATABASES((ErlDrvPort)-1); // use unlikely port number - have no port here.

    int dbref;
    int rc;
    for (dbref = 0; dbref < G_DATABASES_SIZE; dbref++)
    {
        Database* database = &G_DATABASES[dbref];
        if (database->ports != NULL)
        {
            fprintf(stderr, "BDBERL: Ports still open on '%s' dbref %d\r\n",
                    database->name ? database->name : "no name", dbref);
        }

        if (database->db != NULL)
        {
            int flags = 0;
            DBG("final db->close(%p, %08x) (for dbref %d)", database->db, flags, dbref);
            rc = database->db->close(database->db, flags);
            DBG(" = %s (%d)\r\n", rc == 0 ? "ok" : bdberl_rc_to_atom_str(rc), rc);
        }
    }

    UNLOCK_DATABASES((ErlDrvPort)-1);
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

static int delete_database(const char* name, PortData *data)
{
    // Go directly to a write lock on the global databases structure
    LOCK_DATABASES(data->port);

    // Make sure the database is not opened by anyone
    if (hive_hash_get(G_DATABASES_NAMES, name))
    {
        UNLOCK_DATABASES(data->port);
        return ERROR_DB_ACTIVE;
    }

    // Good, database doesn't seem to be open -- attempt the delete
    DBG("Attempting to delete database: %s\r\n", name);
    int rc = G_DB_ENV->dbremove(G_DB_ENV, 0, name, 0, DB_AUTO_COMMIT);
    UNLOCK_DATABASES(data->port);

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
        if (dir == NULL)
        {
            if (G_DB_ENV->get_home(G_DB_ENV, &dir) != 0)
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

void bdberl_async_cleanup(PortData* d)
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
char *bdberl_rc_to_atom_str(int rc)
{
    char *error = erl_errno_id(rc);
    if (error != NULL && strcmp("unknown", error) != 0)
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

    if (path == NULL)
    {
        rc = EINVAL;
    }
    else if (statvfs(path, &svfs) != 0)
    {
        rc = errno;
    }
    else
    {
        rc = 0;
    }

    if (rc != 0)
    {
        bdberl_send_rc(port, pid, rc);
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


void bdberl_send_rc(ErlDrvPort port, ErlDrvTermData pid, int rc)
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
        char *error = bdberl_rc_to_atom_str(rc);
        if (error != NULL)
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

void bdberl_async_cleanup_and_send_rc(PortData* d, int rc)
{
    // Save the port and pid references -- we need copies independent from the PortData
    // structure. Once we release the port_lock after clearing the cmd, it's possible that
    // the port could go away without waiting on us to finish. This is acceptable, but we need
    // to be certain that there is no overlap of data between the two threads. driver_send_term
    // is safe to use from a thread, even if the port you're sending from has already expired.
    ErlDrvPort port = d->port;
    ErlDrvTermData pid = d->port_owner;

    bdberl_async_cleanup(d);
    bdberl_send_rc(port, pid, rc);
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

    bdberl_async_cleanup(d);

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
        char *error = bdberl_rc_to_atom_str(rc);
        if (error != NULL)
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


static void do_async_put(void* arg)
{
    // Payload is: <<DbRef:32, Flags:32, KeyLen:32, Key:KeyLen, ValLen:32, Val:ValLen>>
    PortData* d = (PortData*)arg;

    // Get the database reference and flags from the payload
    int dbref = UNPACK_INT(d->work_buffer, 0);
    DB* db = bdberl_lookup_dbref(dbref);
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
        DBGCMD(d, "db->put(%p, %p, %p, %p, %08X) dbref %d key=%p(%d) value=%p(%d)",
               db, d->txn, &key, &value, flags, dbref, key.data, key.size, value.data, value.size);
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

    bdberl_async_cleanup_and_send_rc(d, rc);
}

static void do_async_get(void* arg)
{
    // Payload is: << DbRef:32, Flags:32, KeyLen:32, Key:KeyLen >>
    PortData* d = (PortData*)arg;
    
    // Get the database object, using the provided ref
    int dbref = UNPACK_INT(d->work_buffer, 0);
    DB* db = bdberl_lookup_dbref(dbref);
    
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
    value.flags = DB_DBT_MALLOC;
    
    int rc = db->get(db, d->txn, &key, &value, flags);
    
    // Check CRC - first 4 bytes are CRC of rest of bytes
    if (rc == 0)
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
    free(value.data);
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
        DBGCMD(d, "rc = %s (%d) d->txn = %p", rc == 0 ? "ok" : bdberl_rc_to_atom_str(rc), rc, d->txn);

    }
    else if (d->async_op == CMD_TXN_COMMIT)
    {
        assert(d->txn != NULL);
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

    bdberl_async_cleanup_and_send_rc(d, rc);
}


static void do_async_cursor_get(void* arg)
{
    // Payload is: << DbRef:32, Flags:32, KeyLen:32, Key:KeyLen >>
    PortData* d = (PortData*)arg;
    assert(d->cursor != NULL);
    
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
    DBGCMD(d, "d->cursor->get(%p, %p, %p, %08X);", d->cursor, &key, &value, flags);
    int rc = d->cursor->get(d->cursor, &key, &value, flags);
    DBGCMDRC(d, rc);

    // Check CRC - first 4 bytes are CRC of rest of bytes
    if (rc == 0)
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

                DBGCMD(d, "db->truncate(%p, %p, %p, 0) dbref=%d", db, d->txn, &count, i);
                rc = db->truncate(db, d->txn, &count, 0);
                DBGCMD(d, "rc = %s (%d) count=%d", 
                       rc == 0 ? "ok" : bdberl_rc_to_atom_str(rc), rc, count);

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
        DBGCMD(d, "rc = %s (%d) count=%d", rc == 0 ? "ok" : bdberl_rc_to_atom_str(rc), 
               rc, count);
    }

    // If any error occurs while we have a txn action, abort it
    if (d->txn && rc)
    {
        abort_txn(d);
    }

    bdberl_async_cleanup_and_send_rc(d, rc);
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
    if (rc != 0 || db_home == NULL)
    {
        // If no db_home we'll have to rely on whatever the global environment is configured with
        got_db_home = 1;
    }
    else
    {
        if (realpath(db_home, db_home_realpath) == NULL)
            rc = errno;
    }

    // Get the data first
    rc = G_DB_ENV->get_data_dirs(G_DB_ENV, &data_dirs);
    int i;
    for (i = 0; rc == 0 && data_dirs != NULL && data_dirs[i] != NULL; i++)
    {
        data_dir = data_dirs[i];

        if (!got_db_home)
        {
            // Get the real path of the data dir
            if (realpath(data_dir, data_dir_realpath) == NULL)
            {
                rc = errno;
            }
            else
            {
                // Set got_db_home if it matches
                if (strcmp(data_dir_realpath, db_home_realpath) == 0)
                {
                    got_db_home = 1;
                }
            }
        }
        
        if (rc == 0)
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
    bdberl_send_rc(d->port, d->port_owner, rc);
}


// Send bdberl specific driver info
static void do_sync_driver_info(PortData *d)
{
    unsigned int txn_pending;
    unsigned int txn_active;
    unsigned int general_pending;
    unsigned int general_active;
    bdberl_tpool_job_count(G_TPOOL_GENERAL, &general_pending, &general_active);
    bdberl_tpool_job_count(G_TPOOL_TXNS, &txn_pending, &txn_active);

    ErlDrvPort port = d->port;
    ErlDrvTermData pid = d->port_owner;
    ErlDrvTermData response[] = {
        ERL_DRV_ATOM, driver_mk_atom("ok"),
        // Start of list
        ERL_DRV_ATOM, driver_mk_atom("databases_size"),
        ERL_DRV_UINT, G_DATABASES_SIZE,
        ERL_DRV_TUPLE, 2,
        ERL_DRV_ATOM, driver_mk_atom("deadlock_interval"),
        ERL_DRV_UINT, G_DEADLOCK_CHECK_INTERVAL,
        ERL_DRV_TUPLE, 2,
        ERL_DRV_ATOM, driver_mk_atom("trickle_interval"),
        ERL_DRV_UINT, G_TRICKLE_INTERVAL,
        ERL_DRV_TUPLE, 2,
        ERL_DRV_ATOM, driver_mk_atom("trickle_percentage"),
        ERL_DRV_UINT, G_TRICKLE_PERCENTAGE,
        ERL_DRV_TUPLE, 2,
        ERL_DRV_ATOM, driver_mk_atom("checkpoint_interval"),
        ERL_DRV_UINT, G_CHECKPOINT_INTERVAL,
        ERL_DRV_TUPLE, 2,
        ERL_DRV_ATOM, driver_mk_atom("num_general_threads"),
        ERL_DRV_UINT, G_NUM_GENERAL_THREADS,
        ERL_DRV_TUPLE, 2,
        ERL_DRV_ATOM, driver_mk_atom("num_txn_threads"),
        ERL_DRV_UINT, G_NUM_TXN_THREADS,
        ERL_DRV_TUPLE, 2,
        ERL_DRV_ATOM, driver_mk_atom("general_jobs_pending"),
        ERL_DRV_UINT, general_pending,
        ERL_DRV_TUPLE, 2,
        ERL_DRV_ATOM, driver_mk_atom("general_jobs_active"),
        ERL_DRV_UINT, general_active,
        ERL_DRV_TUPLE, 2,
        ERL_DRV_ATOM, driver_mk_atom("txn_jobs_pending"),
        ERL_DRV_UINT, txn_pending,
        ERL_DRV_TUPLE, 2,
        ERL_DRV_ATOM, driver_mk_atom("txn_jobs_active"),
        ERL_DRV_UINT, txn_active,
        ERL_DRV_TUPLE, 2,
        // End of list
        ERL_DRV_NIL, 
        ERL_DRV_LIST, 11+1,
        ERL_DRV_TUPLE, 2
    };
    driver_send_term(port, pid, response, sizeof(response) / sizeof(response[0]));
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
    DBG("Adding port %p to dbref %d\r\n", port, dbref);
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
    DBG("Deleting port %p from dbref %d\r\n", port, dbref);
    PortList* current = G_DATABASES[dbref].ports;
    PortList* last = 0;
    assert(current != NULL);
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
    DBG("Adding dbref %d to port %p\r\n", dbref, data->port);
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
    DBG("Deleting dbref %d from port %p\r\n", dbref, data->port);

    DbRefList* current = data->dbrefs;
    DbRefList* last = 0;
    assert(current != NULL);

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
int bdberl_has_dbref(PortData* data, int dbref) 
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
            assert(G_DATABASES[i].db == NULL);
            assert(G_DATABASES[i].ports == NULL);
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
            if (select(nfds, &fds, NULL, NULL,  &tv) == -1)
            {
                if (errno == EINTR) // a signal woke up select, back to sleep for us
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
        if (rc != 0)
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

#ifdef DEBUG
static void bdberl_dbgcmd(PortData *d, const char *fmt, ...)
{
    char buf[1024];

    va_list ap;
    va_start(ap, fmt);
    vsnprintf(buf, sizeof(buf), fmt, ap);
    va_end(ap);

    (void)fprintf(stderr, "threadid %p port %p: %s\r\n", erl_drv_thread_self(), d->port, buf);
}

static void bdberl_dbgcmdrc(PortData *d, int rc)
{
    (void)fprintf(stderr, "threadid %p port %p: rc = %s (%d)\r\n",
                  erl_drv_thread_self(), d->port, rc == 0 ? "ok" : bdberl_rc_to_atom_str(rc), rc);
}
#endif // DEBUG
