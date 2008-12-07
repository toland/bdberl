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

#include "hive_hash.h"
#include "bdberl_drv.h"

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


DRIVER_INIT(bdberl_drv) 
{
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
        char* max_dbs_str = getenv("BDBERL_MAX_DBS");
        if (max_dbs_str != 0)
        {
            G_DATABASES_SIZE = atoi(max_dbs_str);
            if (G_DATABASES_SIZE <= 0)
            {
                G_DATABASES_SIZE = 1024;
            }
        }

        // BDB is setup -- allocate structures for tracking databases
        G_DATABASES = (Database*) driver_alloc(sizeof(Database) * G_DATABASES_SIZE);
        memset(G_DATABASES, '\0', sizeof(Database) * G_DATABASES_SIZE);
        G_DATABASES_RWLOCK = erl_drv_rwlock_create("bdberl_drv: G_DATABASES_RWLOCK");
        G_DATABASES_NAMES = hive_hash_new(G_DATABASES_SIZE);            
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
    PortData* d = (PortData*)driver_alloc(sizeof(PortData));
    memset(d, '\0', sizeof(PortData));

    // Save handle to the port
    d->port = port;

    // Make sure port is running in binary mode
    set_port_control_flags(port, PORT_CONTROL_FLAG_BINARY);

    return (ErlDrvData)d;
}

static void bdberl_drv_stop(ErlDrvData handle)
{
    // Release the port instance data
    driver_free(handle);
}

static int bdberl_drv_control(ErlDrvData handle, unsigned int cmd, 
                              char* inbuf, int inbuf_sz, 
                              char** outbuf, int outbuf_sz)
{
    //PortData* d = (PortData*)handle;
    DB* dbp;
    db_create(&dbp, NULL, 0);

    switch(cmd)
    {
    }
    *outbuf = 0;
    return 0;
}

static void bdberl_drv_ready_async(ErlDrvData handle, ErlDrvThreadData thread_data)
{
}

static void bdberl_drv_process_exit(ErlDrvData handle, ErlDrvMonitor *monitor)
{
}
