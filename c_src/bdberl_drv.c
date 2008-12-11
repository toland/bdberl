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
static int open_database(const char* name, DBTYPE type, PortData* data, int* errno);
static int close_database(int dbref, PortData* data);

static void do_async_put(void* arg);
static void do_async_get(void* arg);
static void do_async_txnop(void* arg);

static int add_dbref(PortData* data, int dbref);
static int del_dbref(PortData* data, int dbref);
static int has_dbref(PortData* data, int dbref);

static int add_portref(int dbref, ErlDrvPort port);
static int del_portref(int dbref, ErlDrvPort port);

static int alloc_dbref();

static void* zalloc(unsigned int size);

static void signal_port(PortData* d);
static void* deadlock_check(void* arg);

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
 * G_DEADLOCK_* a
 */
static ErlDrvTid    G_DEADLOCK_THREAD;
static unsigned int G_DEADLOCK_CHECK_ACTIVE = 1;
static unsigned int G_DEADLOCK_CHECK_INTERVAL = 100;

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

#define RETURN_BH(bh, outbuf) *outbuf = (char*)bh.bin; return bh.bin->orig_size;

#define RETURN_INT(val, outbuf) {             \
        BinHelper bh;                         \
        bin_helper_init(&bh, 4);              \
        bin_helper_push_int32(&bh, val);      \
        RETURN_BH(bh, outbuf); }

DRIVER_INIT(bdberl_drv) 
{
    printf("DRIVER INIT\n");
    // Setup flags we'll use to init the environment
    int flags = 
        DB_INIT_LOCK |          /* Enable support for locking */
        DB_INIT_TXN |           /* Enable support for transactions */
        DB_INIT_MPOOL |         /* Enable support for memory pools */
        DB_RECOVER |            /* Enable support for recovering from failures */
        DB_CREATE |             /* Create files as necessary */
        DB_REGISTER |           /* Run recovery if needed */
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
        int rc = G_DB_ENV->set_flags(G_DB_ENV, DB_TIME_NOTGRANTED, 1);
        printf("TIME_NOT_GRANTED rc: %d\n", rc);

        // BDB is setup -- allocate structures for tracking databases
        G_DATABASES = (Database*) driver_alloc(sizeof(Database) * G_DATABASES_SIZE);
        memset(G_DATABASES, '\0', sizeof(Database) * G_DATABASES_SIZE);
        G_DATABASES_RWLOCK = erl_drv_rwlock_create("bdberl_drv: G_DATABASES_RWLOCK");
        G_DATABASES_NAMES = hive_hash_new(G_DATABASES_SIZE);            

        // Startup deadlock check thread
        erl_drv_thread_create("bdberl_drv_deadlock_checker", &G_DEADLOCK_THREAD, 
                              &deadlock_check, 0, 0);

        // Startup our thread pools
        // TODO: Make configurable/adjustable
        G_TPOOL_GENERAL = bdberl_tpool_start(5);
        G_TPOOL_TXNS    = bdberl_tpool_start(5);
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

    // Setup a pair of pipes for notification purposes
    assert(pipe(d->pipe_fds) == 0);

    // Make sure both pipes are configured non-blocking    
    assert(fcntl(d->pipe_fds[0], F_SETFL, O_NONBLOCK) == 0);
    assert(fcntl(d->pipe_fds[1], F_SETFL, O_NONBLOCK) == 0);

    // Make sure port is running in binary mode
    set_port_control_flags(port, PORT_CONTROL_FLAG_BINARY);

    return (ErlDrvData)d;
}

static void bdberl_drv_stop(ErlDrvData handle)
{
    PortData* d = (PortData*)handle;
//    printf("%p: stop\n", d->port);

    // If there is an async job pending, we need to cancel it. The cancel operation will
    // block until the job has either been removed or has run
    if (d->async_job)
    {
        printf("Cancelling async job for port: %p\n", d->port);
        bdberl_tpool_cancel(d->async_pool, d->async_job);
        driver_select(d->port, (ErlDrvEvent)d->pipe_fds[0], DO_READ, 0);
        printf("Canceled async job for port: %p\n", d->port);
    }

    // If a txn is currently active, terminate it. We _must_ do it synchronously (unfortunately) as
    // there doesn't seem to be a to do an async op while stopping the driver.
    if (d->txn)
    {
        d->txn->abort(d->txn);
    }

    // Close all the databases we previously opened
    while (d->dbrefs)
    {
        close_database(d->dbrefs->dbref, d);
    }


    close(d->pipe_fds[0]);
    close(d->pipe_fds[1]);
    printf("Stopped port: %p\n", d->port);
    
    // Release the port instance data
    driver_free(handle);
}

static void bdberl_drv_finish()
{
    // Stop the thread pools
    bdberl_tpool_stop(G_TPOOL_GENERAL);
    bdberl_tpool_stop(G_TPOOL_TXNS);

    // Signal the deadlock checker to shutdown -- then wait for it
    G_DEADLOCK_CHECK_ACTIVE = 0;
    erl_drv_thread_join(G_DEADLOCK_THREAD, 0);

    // Cleanup and shut down the BDB environment. Note that we assume
    // all ports have been released and thuse all databases/txns/etc are also gone.
    G_DB_ENV->close(G_DB_ENV, 0);    
    driver_free(G_DATABASES);
    erl_drv_rwlock_destroy(G_DATABASES_RWLOCK);
    hive_hash_destroy(G_DATABASES_NAMES);

    printf("DRIVER_FINISH\n");
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
        // Inbuf is: <<Type:8, Name/bytes, 0:8>>
        DBTYPE type = (DBTYPE)((char)*inbuf);
        char* name = (char*)(inbuf+1);
        int dbref;
        int status;
        int rc = open_database(name, type, d, &dbref);
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
        bin_helper_init(&bh, 5);
        bin_helper_push_byte(&bh, status);
        bin_helper_push_int32(&bh, dbref);
        RETURN_BH(bh, outbuf);
    }
    case CMD_CLOSE_DB:
    {
        // TODO: If data is inflight, fail. Abort any open txns.

        // Take the provided dbref and attempt to close it
        int dbref = *((int*)inbuf);
        int rc = close_database(dbref, d);
        
        // Outbuf is: <<Rc:32>>
        RETURN_INT(rc, outbuf);
    }
    case CMD_TXN_BEGIN:
    {
        // If an async operation is pending, fail
        if (d->async_op != CMD_NONE)
        {
            RETURN_INT(ERROR_ASYNC_PENDING, outbuf);
        }

        // If we already have a txn open, fail
        if (d->txn != 0)
        {
            RETURN_INT(ERROR_TXN_OPEN, outbuf);
        }

        // Outbuf is <<Rc:32>>
        int rc = G_DB_ENV->txn_begin(G_DB_ENV, 0, &(d->txn), 0);
        RETURN_INT(rc, outbuf);
    }
    case CMD_TXN_COMMIT:
    case CMD_TXN_ABORT:
    {
        // If an async operation is pending, fail
        if (d->async_op != CMD_NONE)
        {
            RETURN_INT(ERROR_ASYNC_PENDING, outbuf);
        }

        // If we don't already have a txn open, fail
        if (d->txn == 0)
        {
            RETURN_INT(ERROR_NO_TXN, outbuf);
        }

        // Allocate operation structure
        AsyncData* adata = zalloc(sizeof(AsyncData));
        adata->port = d;

        // Update port data to indicate we have an operation in progress
        d->async_op = cmd;

        // Schedule async operation to execute the commit/abort
        d->async_data = adata;
        d->async_pool = G_TPOOL_TXNS;
        d->async_job = bdberl_tpool_run(G_TPOOL_TXNS, &do_async_txnop, adata, 0);

        // Watch for events on the output pipe
        // TODO: Can we do this just once ?!
        driver_select(d->port, (ErlDrvEvent)d->pipe_fds[0], DO_READ, 1);

        // Outbuf is <<Rc:32>>
        RETURN_INT(0, outbuf);
    }
    case CMD_PUT:
    case CMD_GET:
    {
        // If another async op is pending, fail
        if (d->async_op != CMD_NONE)
        {
            RETURN_INT(ERROR_ASYNC_PENDING, outbuf);
        }

        // Inbuf is: << DbRef:32, Rest/binary>>
        int dbref = *((int*)inbuf);

        // Make sure this port currently has dbref open -- if it doesn't, error out. Of note,
        // if it's in our list, we don't need to grab the RWLOCK, as we don't have to worry about
        // the underlying handle disappearing since we have a reference.
        if (has_dbref(d, dbref))
        {
            // Allocate operation structure 
            void* thread_data = zalloc(sizeof(AsyncData) + inbuf_sz);
            AsyncData* adata = (AsyncData*)thread_data;
            adata->port = d;
            adata->db = G_DATABASES[dbref].db;
            adata->payload = thread_data + sizeof(AsyncData);

            // Copy the payload into place
            memcpy(adata->payload, inbuf, inbuf_sz);

            // Mark the port as busy and then schedule the appropriate async operation
            d->async_op = cmd;

            TPoolJobFunc fn;
            if (cmd == CMD_PUT)
            {
                fn = &do_async_put;
            }
            else if (cmd == CMD_GET)
            {
                fn = &do_async_get;
            }

            d->async_data = adata;
            d->async_pool = G_TPOOL_GENERAL;
            d->async_job = bdberl_tpool_run(G_TPOOL_GENERAL, fn, adata, 0);

            // Watch for events on the output pipe
            // TODO: Can we do this just once ?!
            driver_select(d->port, (ErlDrvEvent)d->pipe_fds[0], DO_READ, 1);

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
    }
    *outbuf = 0;
    return 0;
}

static void bdberl_drv_ready_input(ErlDrvData handle, ErlDrvEvent event)
{
    PortData* d = (PortData*)handle;
//    printf("%p: ready_input; cmd = %d; rc = %d\n", d->port, d->async_op, 
//           ((AsyncData*)d->async_data)->rc);

    // Empty out the queue
    int readbuf;
    while (read((int)event, &readbuf, sizeof(readbuf)) > 0) { ; }
    driver_select(d->port, event, DO_READ, 0);

    // The async op has completed running on the thread pool -- process the results
    switch (d->async_op) 
    {
    case CMD_PUT:
    case CMD_TXN_COMMIT:
    case CMD_TXN_ABORT:
    {
        AsyncData* adata = (AsyncData*)d->async_data;

        // Extract return code == if it's zero, send back "ok" to driver process; otherwise
        // send a {error, Reason} tuple
        if (adata->rc == 0)
        {
            ErlDrvTermData response[] = { ERL_DRV_ATOM, driver_mk_atom("ok") };
            driver_output_term(d->port, response, sizeof(response) / sizeof(response[0]));
        }
        else
        {
            ErlDrvTermData response[] = { ERL_DRV_ATOM, driver_mk_atom("error"),
                                       ERL_DRV_INT, adata->rc,
                                       ERL_DRV_TUPLE, 2};
            driver_output_term(d->port, response, sizeof(response) / sizeof(response[0]));
        }

        // If this was a commit/abort, or a deadlock occurred while in a transaction,
        // clear out the handle -- it's already invalid
        if (d->async_op == CMD_TXN_COMMIT || d->async_op == CMD_TXN_ABORT ||
            (d->txn && (adata->rc == DB_LOCK_NOTGRANTED || adata->rc == DB_LOCK_DEADLOCK)))
        {
            d->txn = 0;
        } 

        // Cleanup async data and mark the port as not busy
        driver_free(d->async_data);
        d->async_data = 0;
        d->async_op = CMD_NONE;
        d->async_job = 0;
        d->async_pool = 0;
        break;
    }
    case CMD_GET:
    {
        // Extract return code == if it's zero, send back {ok, Payload} or not_found to driver
        // process; otherwise send a {error, Reason} tuple
        AsyncData* adata = (AsyncData*)d->async_data;
        if (adata->rc == 0)
        {
            ErlDrvTermData response[] = { ERL_DRV_ATOM, driver_mk_atom("ok"),
                                          ERL_DRV_BUF2BINARY, (ErlDrvTermData)adata->payload, (ErlDrvUInt)adata->payload_sz,
                                          ERL_DRV_TUPLE, 2};
            driver_output_term(d->port, response, sizeof(response) / sizeof(response[0]));
        }
        else if (adata->rc == DB_NOTFOUND)
        {
            ErlDrvTermData response[] = { ERL_DRV_ATOM, driver_mk_atom("not_found") };
            driver_output_term(d->port, response, sizeof(response) / sizeof(response[0]));
        }
        else
        {
            ErlDrvTermData response[] = { ERL_DRV_ATOM, driver_mk_atom("error"),
                                       ERL_DRV_INT, adata->rc,
                                       ERL_DRV_TUPLE, 2};
            driver_output_term(d->port, response, sizeof(response) / sizeof(response[0]));
        }

        // If a deadlock occurred while in a transaction, clear out the handle -- it's
        // already invalid
        if (d->txn && (adata->rc == DB_LOCK_DEADLOCK || adata->rc == DB_LOCK_NOTGRANTED))
        {
            d->txn = 0;
        }

        // Cleanup async data and mark the port as not busy
        driver_free(d->async_data);
        d->async_data = 0;
        d->async_op = CMD_NONE;
        d->async_job = 0;
        d->async_pool = 0;
        break;
    }
    }
}

static int open_database(const char* name, DBTYPE type, PortData* data, int* dbref_res)
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
        rc = db->open(db, 0, name, 0, type, DB_CREATE | DB_AUTO_COMMIT | DB_THREAD, 0);
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

static int close_database(int dbref, PortData* data)
{
//    printf("Closing %d for port %p\n", dbref, data->port);

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
            printf("Closing actual database for dbref %d\n", dbref);
            // Close out the BDB handle
            database->db->close(database->db, 0);
        
            // Remove the entry from the names map
            hive_hash_remove(G_DATABASES_NAMES, database->name);
            free((char*)database->name);

            // Zero out the whole record
            memset(database, '\0', sizeof(Database));
        }

        WRITE_UNLOCK(G_DATABASES_RWLOCK);
        return 1;
    }

    return 0;
}

static void do_async_put(void* arg)
{
    AsyncData* adata = (AsyncData*)arg;
//    printf("%p: do_async_put\n", adata->port->port);
    
    // Setup DBTs 
    DBT key;
    DBT value;
    memset(&key, '\0', sizeof(DBT));
    memset(&value, '\0', sizeof(DBT));

    // Parse payload into DBTs
    // Payload is: << DbRef:32, KeyLen:32, Key:KeyLen, ValLen:32, Val:ValLen>>
    key.size = *((int*)(adata->payload + 4));
    key.data = (void*)(adata->payload + 8);
    value.size = *((int*)(adata->payload + 8 + key.size));
    value.data = (void*)(adata->payload + 8 + key.size + 4);

    // Execute the actual put -- we'll process the result back in the driver_async_ready function
    // All databases are opened with AUTO_COMMIT, so if msg->port->txn is NULL, the put will still
    // be atomic
    adata->rc = adata->db->put(adata->db, adata->port->txn, &key, &value, 0);
    
    // If any error occurs while we have a txn action, abort it
    if (adata->port->txn && adata->rc)
    {
        adata->port->txn->abort(adata->port->txn);
    }

    // Enqueue a signal for the port to know that the operation is complete
    signal_port(adata->port);
}

static void do_async_get(void* arg)
{
    AsyncData* adata = (AsyncData*)arg;
//    printf("%p: do_async_get\n", adata->port->port);

    // Setup DBTs 
    DBT key;
    DBT value;
    memset(&key, '\0', sizeof(DBT));
    memset(&value, '\0', sizeof(DBT));

    // Parse payload into DBT
    // Payload is: << DbRef:32, KeyLen:32, Key:KeyLen >>
    key.size = *((int*)(adata->payload + 4));
    key.data = (void*)(adata->payload + 8);

    // Allocate memory to hold the value -- hard code initial size to 4k 
    // TODO: Make this smarter!
    value.data = zalloc(4096);
    value.ulen = 4096;
    value.flags = DB_DBT_USERMEM;
    
    int rc = adata->db->get(adata->db, adata->port->txn, &key, &value, 0);
    while (rc == DB_BUFFER_SMALL)
    {
        // Grow our value buffer and try again
        value.data = driver_realloc(value.data, value.size);
        value.ulen = value.size;
        rc = adata->db->get(adata->db, adata->port->txn, &key, &value, 0);
    }

    adata->payload = value.data;
    adata->payload_sz = value.size; // Not ulen -- we want the actual data size
    adata->rc = rc;

    // If any error occurs while we have a txn action, abort it
    if (adata->rc != DB_NOTFOUND && adata->port->txn && adata->rc)
    {
        adata->port->txn->abort(adata->port->txn);
    }

    signal_port(adata->port);
}

static void do_async_txnop(void* arg)
{
    AsyncData* adata = (AsyncData*)arg;
//    printf("%p: do_async_txnop\n", adata->port->port);

    // Execute the actual commit/abort
    if (adata->port->async_op == CMD_TXN_COMMIT)
    {
        adata->rc = adata->port->txn->commit(adata->port->txn, 0);
    }
    else 
    {
        adata->rc = adata->port->txn->abort(adata->port->txn);
    }

    signal_port(adata->port);
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

static void signal_port(PortData* d) 
{
    int flag = 1;
    write(d->pipe_fds[1], &flag, sizeof(flag));
}

/**
 * Thread function that runs the deadlock checker periodically
 */
static void* deadlock_check(void* arg)
{
    while(G_DEADLOCK_CHECK_ACTIVE)
    {
        // Run the lock detection
        G_DB_ENV->lock_detect(G_DB_ENV, 0, DB_LOCK_DEFAULT, 0);

        // TODO: Use nanosleep
        usleep(G_DEADLOCK_CHECK_INTERVAL * 1000);
    }

    printf("Deadlock checker exiting.\n");
    return 0;
}
