/* -------------------------------------------------------------------
 *
 * bdberl: Berkeley DB Driver for Erlang
 * Copyright (c) 2008 The Hive.  All rights reserved.
 *
 * ------------------------------------------------------------------- */

#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <stdint.h>
#include <time.h>
#include <fcntl.h>

#include "hive_hash.h"
#include "bdberl_drv.h"
#include "bin_helper.h"

/**
 * Function prototypes
 */
static int open_database(const char* name, DBTYPE type, unsigned flags, PortData* data, int* errno);
static int close_database(int dbref, unsigned flags, PortData* data);
static int delete_database(const char* name);

static void tune_system(int target, void* values, BinHelper* bh);

static void do_async_put(void* arg);
static void do_async_get(void* arg);
static void do_async_txnop(void* arg);
static void do_async_cursor_get(void* arg);

static int add_dbref(PortData* data, int dbref);
static int del_dbref(PortData* data, int dbref);
static int has_dbref(PortData* data, int dbref);

static int add_portref(int dbref, ErlDrvPort port);
static int del_portref(int dbref, ErlDrvPort port);

static int alloc_dbref();

static void* zalloc(unsigned int size);

static void* deadlock_check(void* arg);
static void* trickle_write(void* arg);
static void* txn_checkpoint(void* arg);

static void send_ok_or_error(ErlDrvPort port, ErlDrvTermData pid, int rc);

/**
 * Global instance of DB_ENV; only a single one exists per O/S process.
 */
static DB_ENV* G_DB_ENV;


/**
 * Global variable to track the return code from opening the DB_ENV. We track this
 * value so as to provide a useful error code when the user attempts to open the
 * port and it fails due to an error that occurred when opening the environment.
 */
static int G_DB_ENV_ERROR;


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
static Database*     G_DATABASES;
static int           G_DATABASES_SIZE;
static ErlDrvRWLock* G_DATABASES_RWLOCK;
static hive_hash*    G_DATABASES_NAMES;


/**
 * Deadlock detector thread variables. We run a single thread per VM to detect deadlocks within
 * our global environment. G_DEADLOCK_CHECK_INTERVAL is the time between runs in milliseconds.
 */
static ErlDrvTid    G_DEADLOCK_THREAD;
static unsigned int G_DEADLOCK_CHECK_ACTIVE   = 1;
static unsigned int G_DEADLOCK_CHECK_INTERVAL = 100; /* Milliseconds between checks */


/**
 * Trickle writer for dirty pages. We run a single thread per VM to perform background
 * trickling of dirty pages to disk. G_TRICKLE_INTERVAL is the time between runs in seconds.
 */
static ErlDrvTid    G_TRICKLE_THREAD;
static unsigned int G_TRICKLE_ACTIVE     = 1;
static unsigned int G_TRICKLE_INTERVAL   = 60 * 15; /* Seconds between trickle writes */
static unsigned int G_TRICKLE_PERCENTAGE = 10;      /* Desired % of clean pages in cache */


/**
 * Transaction checkpoint monitor. We run a single thread per VM to flush transaction
 * logs into the backing data store. G_CHECKPOINT_INTERVAL is the time between runs in seconds.
 * TODO The interval should be configurable.
 */
static ErlDrvTid    G_CHECKPOINT_THREAD;
static unsigned int G_CHECKPOINT_ACTIVE   = 1;
static unsigned int G_CHECKPOINT_INTERVAL = 60 * 60; /* Seconds between checkpoints */


/**
 *
 */
static TPool* G_TPOOL_GENERAL;
static TPool* G_TPOOL_TXNS;


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

#ifdef DEBUG
#  define DBG printf
#else
#  define DBG(arg1,...)
#endif


DRIVER_INIT(bdberl_drv) 
{
    DBG("DRIVER INIT\n");
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

    // Initialize global environment -- use environment variable DB_HOME to 
    // specify where the working directory is
    db_env_create(&G_DB_ENV, 0);

    G_DB_ENV_ERROR = G_DB_ENV->open(G_DB_ENV, 0, flags, 0);
    if (G_DB_ENV_ERROR == 0)
    {
        // Use the BDBERL_MAX_DBS environment value to determine the max # of
        // databases to permit the VM to open at once. Defaults to 1024.
        G_DATABASES_SIZE = 1024;
        char* max_dbs_str = getenv("BDBERL_MAX_DBS"); /* TODO: Use erl_drv_getenv */
        if (max_dbs_str != 0)
        {
            G_DATABASES_SIZE = atoi(max_dbs_str);
            if (G_DATABASES_SIZE <= 0)
            {
                G_DATABASES_SIZE = 1024;
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

        // Startup trickle write thread
        erl_drv_thread_create("bdberl_drv_trickle_write", &G_TRICKLE_THREAD,
                              &trickle_write, 0, 0);

        // Use the BDBERL_CHECKPOINT_TIME environment value to determine the
        // interval between transaction checkpoints. Defaults to 1 hour.
        char* cp_int_str = getenv("BDBERL_CHECKPOINT_TIME"); /* TODO: Use erl_drv_getenv */
        if (cp_int_str != 0)
        {
            G_CHECKPOINT_INTERVAL = atoi(cp_int_str);
            if (G_CHECKPOINT_INTERVAL <= 0)
            {
                G_CHECKPOINT_INTERVAL = 60 * 60;
            }
        }

        // Startup checkpoint thread
        erl_drv_thread_create("bdberl_drv_txn_checkpoint", &G_CHECKPOINT_THREAD,
                              &txn_checkpoint, 0, 0);

        // Startup our thread pools
        // TODO: Make configurable/adjustable
        G_TPOOL_GENERAL = bdberl_tpool_start(10);
        G_TPOOL_TXNS    = bdberl_tpool_start(10);
    }
    else
    {
        // Something bad happened while initializing BDB; in this situation we 
        // cleanup and set the environment to zero. Attempts to open ports will
        // fail and the user will have to sort out how to resolve the issue.
        G_DB_ENV->close(G_DB_ENV, 0);
        G_DB_ENV = 0;
    }

    return &bdberl_drv_entry;
}

static ErlDrvData bdberl_drv_start(ErlDrvPort port, char* buffer)
{
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

    return (ErlDrvData)d;
}

static void bdberl_drv_stop(ErlDrvData handle)
{
    PortData* d = (PortData*)handle;

    // Grab the port lock, in case we have an async job running
    erl_drv_mutex_lock(d->port_lock);

    // If there is an async job pending, we need to cancel it. The cancel operation will
    // block until the job has either been removed or has run
    if (d->async_job)
    {
        // Drop the lock prior to starting the wait for the async process
        erl_drv_mutex_unlock(d->port_lock);

        DBG("Cancelling async job for port: %p\n", d->port);
        bdberl_tpool_cancel(d->async_pool, d->async_job);
        DBG("Canceled async job for port: %p\n", d->port);
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
    if (d->txn)
    {
        d->txn->abort(d->txn);
    }

    // Close all the databases we previously opened
    while (d->dbrefs)
    {
        close_database(d->dbrefs->dbref, 0, d);
    }

    DBG("Stopped port: %p\n", d->port);
    
    // Release the port instance data
    driver_free(d->work_buffer);
    driver_free(handle);
}

static void bdberl_drv_finish()
{
    // Stop the thread pools
    bdberl_tpool_stop(G_TPOOL_GENERAL);
    bdberl_tpool_stop(G_TPOOL_TXNS);

    // Signal the trickle write thread to shutdown
    G_TRICKLE_ACTIVE = 0;
    erl_drv_thread_join(G_TRICKLE_THREAD, 0);

    // Signal the deadlock checker to shutdown -- then wait for it
    G_DEADLOCK_CHECK_ACTIVE = 0;
    erl_drv_thread_join(G_DEADLOCK_THREAD, 0);

    // Signal the checkpointer to shutdown -- then wait for it
    G_CHECKPOINT_ACTIVE = 0;
    erl_drv_thread_join(G_CHECKPOINT_THREAD, 0);

    // Cleanup and shut down the BDB environment. Note that we assume
    // all ports have been released and thuse all databases/txns/etc are also gone.
    G_DB_ENV->close(G_DB_ENV, 0);    
    driver_free(G_DATABASES);
    erl_drv_rwlock_destroy(G_DATABASES_RWLOCK);
    hive_hash_destroy(G_DATABASES_NAMES);

    DBG("DRIVER_FINISH\n");
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
        int status;
        int rc = open_database(name, type, flags, d, &dbref);
        if (rc == 0)
        {
            status = STATUS_OK;
        }
        else
        {
            status = STATUS_ERROR;
            dbref = rc;
        }

        // Pack the status and dbref (or errno) into a binary and return it
        // Outbuf is: <<Status:8, DbRef:32>>
        BinHelper bh;
        bin_helper_init(&bh);
        bin_helper_push_byte(&bh, status);
        bin_helper_push_int32(&bh, dbref);
        RETURN_BH(bh, outbuf);
    }
    case CMD_CLOSE_DB:
    {
        FAIL_IF_ASYNC_PENDING(d, outbuf);

        // Fail if a cursor is open
        if (d->cursor != 0)
        {
            RETURN_INT(ERROR_CURSOR_OPEN, outbuf);
        }

        // Fail if a txn is open
        if (d->txn != 0)
        {
            RETURN_INT(ERROR_TXN_OPEN, outbuf);
        }

        // Take the provided dbref and attempt to close it
        // Inbuf is: <<DbRef:32, Flags:32/unsigned>>
        int dbref = UNPACK_INT(inbuf, 0);
        unsigned flags = (unsigned) UNPACK_INT(inbuf, 4);

        int rc = close_database(dbref, flags, d);

        // Outbuf is: <<Rc:32>>
        RETURN_INT(rc, outbuf);
    }
    case CMD_TXN_BEGIN:
    {
        FAIL_IF_ASYNC_PENDING(d, outbuf);

        // If we already have a txn open, fail
        if (d->txn != 0)
        {
            RETURN_INT(ERROR_TXN_OPEN, outbuf);
        }

        // Inbuf is <<Flags:32/unsigned>>
        unsigned flags = UNPACK_INT(inbuf, 0);

        // Outbuf is <<Rc:32>>
        int rc = G_DB_ENV->txn_begin(G_DB_ENV, 0, &(d->txn), flags);
        RETURN_INT(rc, outbuf);
    }
    case CMD_TXN_COMMIT:
    case CMD_TXN_ABORT:
    {
        FAIL_IF_ASYNC_PENDING(d, outbuf);

        // If we don't already have a txn open, fail
        if (d->txn == 0)
        {
            RETURN_INT(ERROR_NO_TXN, outbuf);
        }

        // Setup async command and schedule it on the txns threadpool
        d->async_op = cmd;
        if (cmd == CMD_TXN_COMMIT)
        {
            d->async_flags = UNPACK_INT(inbuf, 0);
        }
        d->async_pool = G_TPOOL_TXNS;
        d->async_job  = bdberl_tpool_run(G_TPOOL_TXNS, &do_async_txnop, d, 0);

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
            RETURN_INT(ERROR_NO_TXN, outbuf);
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
            else if (cmd == CMD_GET)
            {
                fn = &do_async_get;
            }
            d->async_pool = G_TPOOL_GENERAL;
            d->async_job = bdberl_tpool_run(G_TPOOL_GENERAL, fn, d, 0);

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
    case CMD_TUNE:
    {
        // Inbuf is: << Target:32, Values/binary >>
        int target = UNPACK_INT(inbuf, 0);
        char* values = UNPACK_BLOB(inbuf, 4);

        // Execute the tuning -- the result to send back to the caller is wrapped
        // up in the provided binhelper
        BinHelper bh;
        tune_system(target, values, &bh);
        RETURN_BH(bh, outbuf);
    }
    case CMD_CURSOR_OPEN:
    {
        FAIL_IF_ASYNC_PENDING(d, outbuf);        

        if (d->cursor)
        {
            RETURN_INT(ERROR_CURSOR_OPEN, outbuf);
        }

        // Inbuf is << DbRef:32, Flags:32 >>
        int dbref = UNPACK_INT(inbuf, 0);
        int flags = UNPACK_INT(inbuf, 4);

        // Make sure we have a reference to the requested database
        if (has_dbref(d, dbref))
        {
            // Grab the database handle and open the cursor
            DB* db = G_DATABASES[dbref].db;
            int rc = db->cursor(db, d->txn, &(d->cursor), flags);
            RETURN_INT(rc, outbuf);
        }
        else
        {
            RETURN_INT(ERROR_INVALID_DBREF, outbuf);
        }
    }
    case CMD_CURSOR_CURR:
    case CMD_CURSOR_NEXT:
    case CMD_CURSOR_PREV:
    {
        FAIL_IF_ASYNC_PENDING(d, outbuf);

        // Fail if no cursor currently open
        if (!d->cursor)
        {
            RETURN_INT(ERROR_NO_CURSOR, outbuf);
        }
        
        // Schedule the operation
        d->async_op = cmd;
        d->async_pool = G_TPOOL_GENERAL;
        d->async_job  = bdberl_tpool_run(G_TPOOL_GENERAL, &do_async_cursor_get, d, 0);

        // Let caller know operation is in progress
        RETURN_INT(0, outbuf);
    }
    case CMD_CURSOR_CLOSE:
    {
        FAIL_IF_ASYNC_PENDING(d, outbuf);

        // Fail if no cursor open
        if (!d->cursor)
        {
            RETURN_INT(ERROR_NO_CURSOR, outbuf);
        }

        // It's possible to get a deadlock when closing a cursor -- in that situation we also
        // need to go ahead and abort the txn
        int rc = d->cursor->close(d->cursor);
        if (d->txn && (rc == DB_LOCK_NOTGRANTED || rc == DB_LOCK_DEADLOCK))
        {
            d->txn->abort(d->txn);
            d->txn = 0;
        }

        // Regardless of what happens, clear out the cursor pointer
        d->cursor = 0;

        RETURN_INT(0, outbuf);
    }
    case CMD_REMOVE_DB:
    {
        FAIL_IF_ASYNC_PENDING(d, outbuf);

        // Fail if a txn is open
        if (d->txn != 0)
        {
            RETURN_INT(ERROR_TXN_OPEN, outbuf);
        }

        // Inbuf is: << dbname/bytes, 0:8 >>
        const char* dbname = UNPACK_STRING(inbuf, 0);
        int rc = delete_database(dbname);
        RETURN_INT(rc, outbuf);
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
        DB* db;
        int rc = db_create(&db, G_DB_ENV, 0);
        if (rc != 0)
        {
            // Failure while creating the database handle -- drop our lock and return 
            // the code
            WRITE_UNLOCK(G_DATABASES_RWLOCK);
            return rc;
        }

        // Attempt to open our database
        rc = db->open(db, 0, name, 0, type, flags, 0);
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
            DBG("Closing actual database for dbref %d\n", dbref);
            // Close out the BDB handle
            database->db->close(database->db, flags);
        
            // Remove the entry from the names map
            hive_hash_remove(G_DATABASES_NAMES, database->name);
            free((char*)database->name);

            // Zero out the whole record
            memset(database, '\0', sizeof(Database));
        }

        WRITE_UNLOCK(G_DATABASES_RWLOCK);
        return 0;
    }
    else
    {
        return ERROR_INVALID_DBREF;
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
    DBG("Attempting to delete database: %s\n", name);
    int rc = G_DB_ENV->dbremove(G_DB_ENV, 0, name, 0, DB_AUTO_COMMIT);
    WRITE_UNLOCK(G_DATABASES_RWLOCK);
    return rc;
}

/**
 * Given a target system parameter/action adjust/return the requested value
 */
static void tune_system(int target, void* values, BinHelper* bh)
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
    case SYSP_TXN_TIMEOUT_SET:
    {
        unsigned int timeout = UNPACK_INT(values, 0);
        int rc = G_DB_ENV->set_timeout(G_DB_ENV, timeout, DB_SET_TXN_TIMEOUT);
        bin_helper_init(bh);
        bin_helper_push_int32(bh, rc);
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
    }
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

    // Execute the actual put. All databases are opened with AUTO_COMMIT, so if msg->port->txn
    // is NULL, the put will still be atomic
    int rc = db->put(db, d->txn, &key, &value, flags);
    
    // If any error occurs while we have a txn action, abort it
    if (d->txn && rc)
    {
        d->txn->abort(d->txn);
        d->txn = 0;
    }
    else if (d->txn && d->async_op == CMD_PUT_COMMIT)
    {
        // Put needs to be followed by a commit -- saves us another pass through the driver and
        // threadpool queues
        rc = d->txn->commit(d->txn, 0);

        // Regardless of the txn commit outcome, we still need to invalidate the transaction
        d->txn = 0;
    }

    // Save the port and pid references -- we need copies independent from the PortData
    // structure. Once we release the port_lock after clearing the cmd, it's possible that
    // the port could go away without waiting on us to finish. This is acceptable, but we need
    // to be certain that there is no overlap of data between the two threads. driver_send_term
    // is safe to use from a thread, even if the port you're sending from has already expired.
    ErlDrvPort port = d->port;
    ErlDrvTermData port_owner = d->port_owner;

    // Release the port for another operation
    d->async_pool = 0;
    d->async_job  = 0;
    d->work_buffer_offset = 0;
    erl_drv_mutex_lock(d->port_lock);
    d->async_op = CMD_NONE;
    erl_drv_mutex_unlock(d->port_lock);

    // TODO: May need to tag the messages a bit more explicitly so that if another async
    // job runs to completion before the message gets delivered we don't mis-interpret this
    // response code.
    send_ok_or_error(port, port_owner, rc);
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
    value.data = driver_alloc(4096);
    value.ulen = 4096;
    value.flags = DB_DBT_USERMEM;
    
    int rc = db->get(db, d->txn, &key, &value, flags);
    while (rc == DB_BUFFER_SMALL)
    {
        // Grow our value buffer and try again
        value.data = driver_realloc(value.data, value.size);
        value.ulen = value.size;
        rc = db->get(db, d->txn, &key, &value, flags);
    }

    // Cleanup transaction as necessary
    if (rc && rc != DB_NOTFOUND && d->txn)
    {
        d->txn->abort(d->txn);
        d->txn = 0;
    }

    // Save the port and pid references -- we need copies independent from the PortData
    // structure. Once we release the port_lock after clearing the cmd, it's possible that
    // the port could go away without waiting on us to finish. This is acceptable, but we need
    // to be certain that there is no overlap of data between the two threads. driver_send_term
    // is safe to use from a thread, even if the port you're sending from has already expired.
    ErlDrvPort port = d->port;
    ErlDrvTermData port_owner = d->port_owner;

    // Release the port for another operation
    d->async_pool = 0;
    d->async_job  = 0;
    d->work_buffer_offset = 0;
    erl_drv_mutex_lock(d->port_lock);
    d->async_op = CMD_NONE;
    erl_drv_mutex_unlock(d->port_lock);

    // Notify port of result
    if (rc == 0)
    {
        ErlDrvTermData response[] = { ERL_DRV_ATOM, driver_mk_atom("ok"),
                                      ERL_DRV_BUF2BINARY, (ErlDrvTermData)value.data, (ErlDrvUInt)value.size,
                                      ERL_DRV_TUPLE, 2};
        driver_send_term(port, port_owner, response, sizeof(response) / sizeof(response[0]));
    }
    else if (rc == DB_NOTFOUND)
    {
        ErlDrvTermData response[] = { ERL_DRV_ATOM, driver_mk_atom("not_found") };
        driver_send_term(port, port_owner, response, sizeof(response) / sizeof(response[0]));
    }
    else
    {
        ErlDrvTermData response[] = { ERL_DRV_ATOM, driver_mk_atom("error"),
                                      ERL_DRV_INT, rc,
                                      ERL_DRV_TUPLE, 2};
        driver_send_term(port, port_owner, response, sizeof(response) / sizeof(response[0]));
    }

    // Finally, clean up value buffer (driver_send_term made a copy)
    driver_free(value.data);
}

static void do_async_txnop(void* arg)
{
    PortData* d = (PortData*)arg;

    // Execute the actual commit/abort
    int rc = 0;
    if (d->async_op == CMD_TXN_COMMIT)
    {
        rc = d->txn->commit(d->txn, d->async_flags);
    }
    else 
    {
        rc = d->txn->abort(d->txn);
    }

    // The transaction is now invalid, regardless of the outcome. 
    d->txn = 0;

    // Save the port and pid references -- we need copies independent from the PortData
    // structure. Once we release the port_lock after clearing the cmd, it's possible that
    // the port could go away without waiting on us to finish. This is acceptable, but we need
    // to be certain that there is no overlap of data between the two threads. driver_send_term
    // is safe to use from a thread, even if the port you're sending from has already expired.
    ErlDrvPort port = d->port;
    ErlDrvTermData port_owner = d->port_owner;

    // Release the port for another operation
    d->async_pool = 0;
    d->async_job  = 0;
    d->work_buffer_offset = 0;
    erl_drv_mutex_lock(d->port_lock);
    d->async_op = CMD_NONE;
    erl_drv_mutex_unlock(d->port_lock);

    // TODO: May need to tag the messages a bit more explicitly so that if another async
    // job runs to completion before the message gets delivered we don't mis-interpret this
    // response code.
    send_ok_or_error(port, port_owner, rc);
}


static void do_async_cursor_get(void* arg)
{
    // Payload is: << DbRef:32, Flags:32, KeyLen:32, Key:KeyLen >>
    PortData* d = (PortData*)arg;

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
    int rc = d->cursor->get(d->cursor, &key, &value, flags);

    // Cleanup as necessary; any sort of failure means we need to close the cursor and abort
    // the transaction
    if (rc && rc != DB_NOTFOUND)
    {
        d->cursor->close(d->cursor);
        d->cursor = 0;
        if (d->txn)
        {
            d->txn->abort(d->txn);
            d->txn = 0;
        }
    }

    // Save the port and pid references -- we need copies independent from the PortData
    // structure. Once we release the port_lock after clearing the cmd, it's possible that
    // the port could go away without waiting on us to finish. This is acceptable, but we need
    // to be certain that there is no overlap of data between the two threads. driver_send_term
    // is safe to use from a thread, even if the port you're sending from has already expired.
    ErlDrvPort port = d->port;
    ErlDrvTermData port_owner = d->port_owner;

    // Release the port for another operation
    d->async_pool = 0;
    d->async_job  = 0;
    erl_drv_mutex_lock(d->port_lock);
    d->async_op = CMD_NONE;
    erl_drv_mutex_unlock(d->port_lock);

    // Notify port of result
    if (rc == 0)
    {
        ErlDrvTermData response[] = { ERL_DRV_ATOM, driver_mk_atom("ok"),
                                      ERL_DRV_BUF2BINARY, (ErlDrvTermData)key.data, (ErlDrvUInt)key.size,
                                      ERL_DRV_BUF2BINARY, (ErlDrvTermData)value.data, (ErlDrvUInt)value.size,
                                      ERL_DRV_TUPLE, 3};
        driver_send_term(port, port_owner, response, sizeof(response) / sizeof(response[0]));
    }
    else if (rc == DB_NOTFOUND)
    {
        ErlDrvTermData response[] = { ERL_DRV_ATOM, driver_mk_atom("not_found") };
        driver_send_term(port, port_owner, response, sizeof(response) / sizeof(response[0]));
    }
    else
    {
        ErlDrvTermData response[] = { ERL_DRV_ATOM, driver_mk_atom("error"),
                                      ERL_DRV_INT, rc,
                                      ERL_DRV_TUPLE, 2};
        driver_send_term(port, port_owner, response, sizeof(response) / sizeof(response[0]));
    }
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

        // At the end of the list -- allocate a new entry for this por
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
 * Thread function that runs the deadlock checker periodically
 */
static void* deadlock_check(void* arg)
{
    while(G_DEADLOCK_CHECK_ACTIVE)
    {
        // Run the lock detection
        int count = 0;
        G_DB_ENV->lock_detect(G_DB_ENV, 0, DB_LOCK_DEFAULT, &count);
        if (count > 0)
        {
            DBG("Rejected deadlocks: %d\n", count);
        }

        // TODO: Use nanosleep
        usleep(G_DEADLOCK_CHECK_INTERVAL * 1000);
    }

    DBG("Deadlock checker exiting.\n");
    return 0;
}

/**
 * Thread function that trickle writes dirty pages to disk
 */
static void* trickle_write(void* arg)
{
    int elapsed_secs = 0;
    while(G_TRICKLE_ACTIVE)
    {
        if (elapsed_secs == G_TRICKLE_INTERVAL)
        {
            // Enough time has passed -- time to run the trickle operation again
            int pages_wrote = 0;
            G_DB_ENV->memp_trickle(G_DB_ENV, G_TRICKLE_PERCENTAGE, &pages_wrote);
            DBG("Wrote %d pages to achieve %d trickle\n", pages_wrote, G_TRICKLE_PERCENTAGE);

            // Reset the counter
            elapsed_secs = 0;
        }
        else
        {
            // TODO: Use nanosleep
            usleep(1000 * 1000);    /* Sleep for 1 second */
            elapsed_secs++;
        }
    }

    DBG("Trickle writer exiting.\n");
    return 0;
}

/**
 * Thread function that flushes transaction logs to the backing store
 */
static void* txn_checkpoint(void* arg)
{
    DBG("Checkpoint interval: %d seconds\n", G_CHECKPOINT_INTERVAL);
    while (G_CHECKPOINT_ACTIVE)
    {
        int ret = 0;
        if ((ret = G_DB_ENV->txn_checkpoint(G_DB_ENV, 0, 0, 0)) != 0)
        {
            G_DB_ENV->err(G_DB_ENV, ret, "checkpoint thread");
        }

#ifdef DEBUG
        time_t tm = time(NULL);
        printf("Transaction checkpoint complete at %s\n", ctime(&tm));
#endif

        sleep(G_CHECKPOINT_INTERVAL);
    }

    DBG("Checkpointer exiting.\n");
    return 0;
}

static void send_ok_or_error(ErlDrvPort port, ErlDrvTermData pid, int rc)
{
    if (rc == 0)
    {        
        ErlDrvTermData response[] = {ERL_DRV_ATOM, driver_mk_atom("ok")};
        driver_send_term(port, pid, response, sizeof(response) / sizeof(response[0]));
    }
    else
    {
        ErlDrvTermData response[] = { ERL_DRV_ATOM, driver_mk_atom("error"),
                                      ERL_DRV_INT, rc,
                                      ERL_DRV_TUPLE, 2};
        driver_send_term(port, pid, response, sizeof(response) / sizeof(response[0]));
    }       
}
