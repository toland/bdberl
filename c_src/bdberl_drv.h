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

/**
 * Driver functions
 */
static ErlDrvData bdberl_drv_start(ErlDrvPort port, char* buffer);

static void bdberl_drv_stop(ErlDrvData handle);

static int bdberl_drv_control(ErlDrvData handle, unsigned int cmd, 
                              char* inbuf, int inbuf_sz, 
                              char** outbuf, int outbuf_sz);

static void bdberl_drv_ready_async(ErlDrvData handle, ErlDrvThreadData thread_data);

static void bdberl_drv_process_exit(ErlDrvData handle, ErlDrvMonitor *monitor);

/**
 * Command codes
 */
#define CMD_OPEN_DB          0
#define CMD_CLOSE_DB         1
#define CMD_TXN_BEGIN        2
#define CMD_TXN_COMMIT       3
#define CMD_TXN_ABORT        4
#define CMD_GET              5
#define CMD_PUT              6
#define CMD_PUT_ATOMIC       7

/** 
 * Driver Entry
 */
ErlDrvEntry bdberl_drv_entry = 
{
    NULL,			/* F_PTR init, N/A */
    bdberl_drv_start,		/* L_PTR start, called when port is opened */
    bdberl_drv_stop,		/* F_PTR stop, called when port is closed */
    NULL,			/* F_PTR output, called when erlang has sent */
    NULL,			/* F_PTR ready_input, called when input descriptor ready */
    NULL,			/* F_PTR ready_output, called when output descriptor ready */
    "bdberl_drv",               /* driver_name */
    NULL,			/* F_PTR finish, called when unloaded */
    NULL,			/* handle */
    bdberl_drv_control,		/* F_PTR control, port_command callback */
    NULL,			/* F_PTR timeout, reserved */
    NULL,                       /* F_PTR outputv, reserved */
    bdberl_drv_ready_async,     /* F_PTR ready_async */
    NULL,                       /* F_PTR flush */
    NULL,                       /* F_PTR call */
    NULL,                       /* F_PTR event */
    ERL_DRV_EXTENDED_MARKER,        
    ERL_DRV_EXTENDED_MAJOR_VERSION, 
    ERL_DRV_EXTENDED_MINOR_VERSION,
    ERL_DRV_FLAG_USE_PORT_LOCKING,
    NULL,                        /* Reserved */
    bdberl_drv_process_exit      /* F_PTR process_exit */
};

typedef struct
{
    unsigned int dbref;
    struct DbRefList* next;
} DbRefList;


typedef struct 
{
    ErlDrvPort port;
    struct PortList* next;
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

    DB_TXN* txn;         /* Transaction handle for this port; each port may only have 1 txn
                          * active */

    int     in_flight;    /* Flag indicating if this port has an operation pending on the async
                           * pool. */

    DbRefList* dbrefs;    /* List of databases that this port has opened  */

} PortData;




#endif
