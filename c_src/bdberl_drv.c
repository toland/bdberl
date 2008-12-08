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
 * Function prototypes
 */
static int open_database(const char* name, DBTYPE type, PortData* data, int* errno);
static int close_database(int dbref, PortData* data);

static int add_dbref(PortData* data, int dbref);
static int del_dbref(PortData* data, int dbref);

static int add_portref(int dbref, ErlDrvPort port);
static int del_portref(int dbref, ErlDrvPort port);

static int alloc_dbref();

static void* zalloc(unsigned int size);

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
 * Helpful macros
 */
#define READ_LOCK(L) erl_drv_rwlock_rlock(L)
#define READ_UNLOCK(L) erl_drv_rwlock_runlock(L)
#define PROMOTE_READ_LOCK(L) { erl_drv_rwlock_runlock(L); erl_drv_rwlock_rwlock(L); }
#define WRITE_LOCK(L) erl_drv_rwlock_rwlock(L)
#define WRITE_UNLOCK(L) erl_drv_rwlock_rwunlock(L)


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

    // Make sure port is running in binary mode
    set_port_control_flags(port, PORT_CONTROL_FLAG_BINARY);

    return (ErlDrvData)d;
}

static void bdberl_drv_stop(ErlDrvData handle)
{
    PortData* d = (PortData*)handle;

    // TODO: Terminate any txns

    // Close all the databases we previously opened
    while (d->dbrefs)
    {
        close_database(d->dbrefs->dbref, d);
    }
    
    // Release the port instance data
    driver_free(handle);
}

static void bdberl_drv_finish()
{
    // Driver is unloading -- cleanup and shut down the BDB environment. Note that we assume
    // all ports have been released and thuse all databases/txns/etc are also gone.
    G_DB_ENV->close(G_DB_ENV, 0);
    
    driver_free(G_DATABASES);
    erl_drv_rwlock_destroy(G_DATABASES_RWLOCK);
    hive_hash_destroy(G_DATABASES_NAMES);
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
        // Expect: Type:8, Name/bytes, NULL:8
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
        // Byte 0   : Status
        // Byte 1..4: dbref/errno
        ErlDrvBinary* result = driver_alloc_binary(5);
        result->orig_bytes[0] = status;
        memcpy(result->orig_bytes+1, (char*)&dbref, sizeof(dbref));
        *outbuf = (char*)result;
        return result->orig_size;
    }
    case CMD_CLOSE_DB:
    {
        // TODO: If data is inflight, fail. Abort any open txns.

        // Take the provided dbref and attempt to close it
        int dbref = *((int*)inbuf);
        int rc = close_database(dbref, d);
        
        // Setup to return the rc
        ErlDrvBinary* result = driver_alloc_binary(4);
        memcpy(result->orig_bytes, (char*)&rc, sizeof(rc));
        *outbuf = (char*)result;
        return result->orig_size;
    }
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
    printf("Closing %d for port %d\n", dbref, (int)data->port);

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
