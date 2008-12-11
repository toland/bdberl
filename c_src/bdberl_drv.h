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

/**
 * Driver functions
 */
static ErlDrvData bdberl_drv_start(ErlDrvPort port, char* buffer);

static void bdberl_drv_stop(ErlDrvData handle);

static void bdberl_drv_finish();

static int bdberl_drv_control(ErlDrvData handle, unsigned int cmd, 
                              char* inbuf, int inbuf_sz, 
                              char** outbuf, int outbuf_sz);

static void bdberl_drv_ready_input(ErlDrvData handle, ErlDrvEvent ev);


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
#define CMD_TUNE             8


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
#define ERROR_MAX_DBS       (-29000) /* System can not open any further databases  */
#define ERROR_ASYNC_PENDING (-29001) /* Async operation already pending on this port */
#define ERROR_INVALID_DBREF (-29002) /* DbRef not currently opened by this port */
#define ERROR_TXN_OPEN      (-29003) /* Transaction already active on this port */
#define ERROR_NO_TXN        (-29004) /* No transaction open on this port */


/**
 * Tunable system parameters/actions
 */
#define SYSP_CACHESIZE_SET               0
#define SYSP_CACHESIZE_GET               1
#define SYSP_TXN_TIMEOUT_SET             2
#define SYSP_TXN_TIMEOUT_GET             3
#define SYSP_TXN_TIMEOUT             1
#define SYSP_DEADLOCK_CHECK_INTERVAL 2
#define SYSP_TRICKLE_INTERVAL        3
#define SYSP_TRICKLE_PERCENTAGE      4

/** 
 * Driver Entry
 */
ErlDrvEntry bdberl_drv_entry = 
{
    NULL,			/* F_PTR init, N/A */
    bdberl_drv_start,		/* L_PTR start, called when port is opened */
    bdberl_drv_stop,		/* F_PTR stop, called when port is closed */
    NULL,			/* F_PTR output, called when erlang has sent */
    bdberl_drv_ready_input,     /* F_PTR ready_input, called when input descriptor ready */
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

    DbRefList* dbrefs;     /* List of databases that this port has opened  */

    DB_TXN* txn;           /* Transaction handle for this port; each port may only have 1 txn
                            * active */

    int pipe_fds[2];       /* Array of pipe fds for signaling purposes */

    int async_op;          /* Value indicating what async op is pending */

    void* async_data;      /* Opaque point to data used during async op */

    TPoolJob* async_job;   /* Active job on the thread pool */

    TPool* async_pool;     /* Pool the async job is running on */

} PortData;


typedef struct
{
    PortData* port;       /* Port that originated this request -- READ ONLY! */
    int rc;                     /* Return code from operation */
    DB* db;                     /* Database to use for data storage/retrieval */
    void* payload;              /* Packed key/value data */
    int payload_sz;             /* Size of payload */
} AsyncData;



#endif
