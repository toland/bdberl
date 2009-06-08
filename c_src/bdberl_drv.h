/* -------------------------------------------------------------------
 *
 * bdberl: Berkeley DB Driver for Erlang
 * Copyright (c) 2008 The Hive.  All rights reserved.
 *
 * ------------------------------------------------------------------- */
#ifndef _BDBERL_DRV
#define _BDBERL_DRV

#include "erl_driver.h"
#include "db.h"
#include "bdberl_tpool.h"
#include "bdberl_crc32.h"
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
 * Command codes
 */
#define CMD_NONE             0
#define CMD_OPEN_DB          1
#define CMD_CLOSE_DB         2
#define CMD_TXN_BEGIN        3
#define CMD_TXN_COMMIT       4
#define CMD_TXN_ABORT        5
#define CMD_GET              6
#define CMD_PUT              7
#define CMD_GETINFO          8
#define CMD_CURSOR_OPEN      9
#define CMD_CURSOR_CURR     10
#define CMD_CURSOR_NEXT     11
#define CMD_CURSOR_PREV     12
#define CMD_CURSOR_CLOSE    13
#define CMD_PUT_COMMIT      14
#define CMD_REMOVE_DB       15
#define CMD_TRUNCATE        16
#define CMD_REGISTER_LOGGER 17
#define CMD_DB_STAT         18
#define CMD_DB_STAT_PRINT   19
#define CMD_ENV_STAT_PRINT  20
#define CMD_LOCK_STAT       21
#define CMD_LOCK_STAT_PRINT 22
#define CMD_LOG_STAT        23
#define CMD_LOG_STAT_PRINT  24
#define CMD_MEMP_STAT       25
#define CMD_MEMP_STAT_PRINT 26
#define CMD_MUTEX_STAT       27
#define CMD_MUTEX_STAT_PRINT 28
#define CMD_TXN_STAT         29
#define CMD_TXN_STAT_PRINT   30
#define CMD_DATA_DIRS_INFO   31
#define CMD_LOG_DIR_INFO     32

/**
 * Command status values
 */
#define STATUS_OK    0
#define STATUS_ERROR 1

/**
 * Database Types (see db.h)
 */
#define DB_TYPE_BTREE DB_BTREE  /* 1 */
#define DB_TYPE_HASH  DB_HASH   /* 2 */

/**
 * Error codes -- chosen so that we do not conflict with other packages, particularly
 * db.h. We use error namespace from -29000 to -29500.
 */
#ifndef ERROR_NONE
#  define ERROR_NONE        0
#endif
#define ERROR_MAX_DBS       (-29000) /* System can not open any further databases  */
#define ERROR_ASYNC_PENDING (-29001) /* Async operation already pending on this port */
#define ERROR_INVALID_DBREF (-29002) /* DbRef not currently opened by this port */
#define ERROR_TXN_OPEN      (-29003) /* Transaction already active on this port */
#define ERROR_NO_TXN        (-29004) /* No transaction open on this port */
#define ERROR_CURSOR_OPEN   (-29005) /* Cursor already active on this port */
#define ERROR_NO_CURSOR     (-29006) /* No cursor open on this port */
#define ERROR_DB_ACTIVE     (-29007) /* Database is currently active; operation requires otherwise */
#define ERROR_INVALID_CMD   (-29008) /* Invalid command code requested */
#define ERROR_INVALID_DB_TYPE  (-29009) /* Invalid database type */
#define ERROR_INVALID_VALUE (-29010) /* Invalid CRC-32 on value */

/**
 * System information ids
 */
#define SYSP_CACHESIZE_GET               1
#define SYSP_TXN_TIMEOUT_GET             2
#define SYSP_DATA_DIR_GET                3
#define SYSP_LOG_DIR_GET                 4

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

typedef struct _DbRefList
{
    unsigned int dbref;
    struct _DbRefList* next;
} DbRefList;


typedef struct _PortList
{
    ErlDrvPort port;
    struct _PortList* next;
} PortList;


typedef struct
{
    DB*  db;
    const char* name;
    PortList* ports;
} Database;


/**
 * Structure for holding port instance data
 */
typedef struct 
{
    ErlDrvPort port;

    ErlDrvMutex* port_lock;     /* Mutex for this port (to permit async jobs to safely update this
                                 * structure) */

    ErlDrvTermData port_owner;  /* Pid of the port owner */

    DbRefList* dbrefs;          /* List of databases that this port has opened  */

    DB_TXN* txn;             /* Transaction handle for this port; each port may only have 1 txn
                              * active */

    DBC* cursor;            /* Active cursor handle; each port may have only 1 cursor active */

    int async_dbref;            /* Db reference for async operations */

    int async_op;               /* Value indicating what async op is pending */

    int async_flags;            /* Flags for the async op command */

    TPoolJob* async_job;   /* Active job on the thread pool */

    TPool* async_pool;     /* Pool the async job is running on */

    void* work_buffer;
    
    unsigned int work_buffer_sz;

    unsigned int work_buffer_offset;

} PortData;

#endif
