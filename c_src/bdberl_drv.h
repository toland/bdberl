/* -------------------------------------------------------------------
 *
 * bdberl: Berkeley DB Driver for Erlang
 * Copyright (c) 2008-9 The Hive http://www.thehive.com/
 * Authors: Dave "dizzyd" Smith <dizzyd@dizzyd.com>
 *          Phil Toland <phil.toland@gmail.com>
 *          Jon Meredith <jmeredith@thehive.com>
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
#ifndef _BDBERL_DRV
#define _BDBERL_DRV

#include "erl_driver.h"
#include "db.h"
#include "bdberl_tpool.h"
#include "bdberl_crc32.h"
#include "bin_helper.h"


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
#define CMD_DRIVER_INFO      33

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

/**
 * Function Prototypes
 */

void bdberl_async_cleanup(PortData* d);
void bdberl_send_rc(ErlDrvPort port, ErlDrvTermData pid, int rc);
void bdberl_async_cleanup_and_send_rc(PortData* d, int rc);

char* bdberl_rc_to_atom_str(int rc);
DB_ENV* bdberl_db_env(void);
DB* bdberl_lookup_dbref(int dbref);
int bdberl_has_dbref(PortData* data, int dbref);

void bdberl_general_tpool_run(TPoolJobFunc main_fn,  PortData* d, TPoolJobFunc cancel_fn,
    TPoolJob** job_ptr);
void bdberl_txn_tpool_run(TPoolJobFunc main_fn,  PortData* d, TPoolJobFunc cancel_fn,
    TPoolJob** job_ptr);

/**
 * Helpful macros
 */
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
        bdberl_send_rc(d->port, d->port_owner, ERROR_CURSOR_OPEN);     \
        RETURN_INT(0, outbuf);                                  \
    }}
#define FAIL_IF_NO_CURSOR(d, outbuf) {                          \
    if (NULL == d->cursor)                                      \
    {                                                           \
        bdberl_send_rc(d->port, d->port_owner, ERROR_NO_CURSOR);       \
        RETURN_INT(0, outbuf);                                  \
    }}

#define FAIL_IF_TXN_OPEN(d, outbuf) {                           \
        if (NULL != d->txn)                                     \
    {                                                           \
        bdberl_send_rc(d->port, d->port_owner, ERROR_TXN_OPEN);        \
        RETURN_INT(0, outbuf);                                  \
    }}
#define FAIL_IF_NO_TXN(d, outbuf) {                             \
        if (NULL == d->txn)                                     \
    {                                                           \
        bdberl_send_rc(d->port, d->port_owner, ERROR_NO_TXN);          \
        RETURN_INT(0, outbuf);                                  \
    }}

#endif //_BDBERL_DRV
