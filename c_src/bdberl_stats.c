/* -------------------------------------------------------------------
 *
 * bdberl: Berkeley DB Driver for Erlang
 * Copyright (c) 2008-9 The Hive.  All rights reserved.
 *
 * ------------------------------------------------------------------- */

#include <assert.h>
#include <string.h>
#include "bdberl_drv.h"
#include "bdberl_stats.h"

/**
 * Function prototypes
 */

static void do_async_stat(void* arg);
static void do_async_lock_stat(void* arg);
static void do_async_log_stat(void* arg);
static void do_async_memp_stat(void* arg);
static void do_async_mutex_stat(void* arg);
static void do_async_txn_stat(void* arg);


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
    bdberl_async_cleanup(d);

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
    bdberl_async_cleanup(d);

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
    bdberl_async_cleanup(d);

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
    bdberl_async_cleanup(d);

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
    bdberl_async_cleanup(d);

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
    bdberl_async_cleanup(d);

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
    bdberl_async_cleanup(d);

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
    bdberl_async_cleanup(d);

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


static void do_async_stat(void* arg)
{
    // Payload is: << DbRef:32, Flags:32 >>
    PortData* d = (PortData*)arg;

    // Get the database object, using the provided ref
    DB* db = bdberl_lookup_dbref(d->async_dbref);
    DBTYPE type = DB_UNKNOWN;
    int rc = db->get_type(db, &type);
    if (rc != 0)
    {
        bdberl_async_cleanup_and_send_rc(d, rc);
        return;
    }

    void *sp = NULL;
    rc = db->stat(db, d->txn, &sp, d->async_flags);
    if (rc != 0 || sp == NULL)
    {
        bdberl_async_cleanup_and_send_rc(d, rc);
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
                bdberl_async_cleanup_and_send_rc(d, ERROR_INVALID_DB_TYPE);
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
    int rc = bdberl_db_env()->lock_stat(bdberl_db_env(), &lsp, d->async_flags);
    if (rc != 0 || lsp == NULL)
    {
        bdberl_async_cleanup_and_send_rc(d, rc);
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
    int rc = bdberl_db_env()->log_stat(bdberl_db_env(), &lsp, d->async_flags);
    if (rc != 0 || lsp == NULL)
    {
        bdberl_async_cleanup_and_send_rc(d, rc);
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
    int rc = bdberl_db_env()->memp_stat(bdberl_db_env(), &gsp, &fsp, d->async_flags);
    if (rc != 0 || gsp == NULL)
    {
        bdberl_async_cleanup_and_send_rc(d, rc);
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
    int rc = bdberl_db_env()->mutex_stat(bdberl_db_env(), &msp, d->async_flags);
    if (rc != 0 || msp == NULL)
    {
        bdberl_async_cleanup_and_send_rc(d, rc);
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
    int rc = bdberl_db_env()->txn_stat(bdberl_db_env(), &tsp, d->async_flags);
    if (rc != 0 || tsp == NULL)
    {
        bdberl_async_cleanup_and_send_rc(d, rc);
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


int bdberl_stats_control(PortData* d, unsigned int cmd, 
                         char* inbuf, int inbuf_sz, 
                         char** outbuf, int outbuf_sz)
{
    switch(cmd)
    {
        case CMD_DB_STAT:
        {
            FAIL_IF_ASYNC_PENDING(d, outbuf);

            // Inbuf is << DbRef:32, Flags:32 >>
            int dbref = UNPACK_INT(inbuf, 0);

            // Make sure this port currently has dbref open -- if it doesn't, error out. Of note,
            // if it's in our list, we don't need to grab the RWLOCK, as we don't have to worry about
            // the underlying handle disappearing since we have a reference.
            if (bdberl_has_dbref(d, dbref))
            {
                // Mark the port as busy and then schedule the appropriate async operation
                d->async_dbref = dbref;
                d->async_op = cmd;
                d->async_flags = UNPACK_INT(inbuf, 4);
                bdberl_general_tpool_run(&do_async_stat, d, 0, &d->async_job);

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
            if (bdberl_has_dbref(d, dbref))
            {
                DB* db = bdberl_lookup_dbref(dbref);
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
            int rc = bdberl_db_env()->stat_print(bdberl_db_env(), flags);
            RETURN_INT(rc, outbuf);
        }
        case CMD_LOCK_STAT:
        {
            FAIL_IF_ASYNC_PENDING(d, outbuf);

            // Mark the port as busy and then schedule the appropriate async operation
            d->async_op = cmd;
            d->async_flags = UNPACK_INT(inbuf, 0);
            bdberl_general_tpool_run(&do_async_lock_stat, d, 0, &d->async_job);
        
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
            int rc = bdberl_db_env()->lock_stat_print(bdberl_db_env(), flags);
            RETURN_INT(rc, outbuf);
        }
        case CMD_LOG_STAT:
        {
            FAIL_IF_ASYNC_PENDING(d, outbuf);

            // Inbuf is <<Flags:32 >>

            // Mark the port as busy and then schedule the appropriate async operation
            d->async_op = cmd;
            d->async_flags = UNPACK_INT(inbuf, 0);
            bdberl_general_tpool_run(&do_async_log_stat, d, 0, &d->async_job);
        
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
            int rc = bdberl_db_env()->log_stat_print(bdberl_db_env(), flags);
            RETURN_INT(rc, outbuf);
        }
        case CMD_MEMP_STAT:
        {
            FAIL_IF_ASYNC_PENDING(d, outbuf);

            // Inbuf is <<Flags:32 >>
      
            // Mark the port as busy and then schedule the appropriate async operation
            d->async_op = cmd;
            d->async_flags = UNPACK_INT(inbuf, 0);
            bdberl_general_tpool_run(&do_async_memp_stat, d, 0, &d->async_job);
        
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
            int rc = bdberl_db_env()->memp_stat_print(bdberl_db_env(), flags);
            RETURN_INT(rc, outbuf);
        }
        case CMD_MUTEX_STAT:
        {
            FAIL_IF_ASYNC_PENDING(d, outbuf);

            // Inbuf is <<Flags:32 >>

            // Mark the port as busy and then schedule the appropriate async operation
            d->async_op = cmd;
            d->async_flags = UNPACK_INT(inbuf, 0);
            bdberl_general_tpool_run(&do_async_mutex_stat, d, 0, &d->async_job);
        
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
            int rc = bdberl_db_env()->mutex_stat_print(bdberl_db_env(), flags);
            RETURN_INT(rc, outbuf);
        }
        case CMD_TXN_STAT:
        {
            FAIL_IF_ASYNC_PENDING(d, outbuf);

            // Inbuf is <<Flags:32 >>
            // Mark the port as busy and then schedule the appropriate async operation
            d->async_op = cmd;
            d->async_flags = UNPACK_INT(inbuf, 0);
            bdberl_general_tpool_run(&do_async_txn_stat, d, 0, &d->async_job);
        
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
            int rc = bdberl_db_env()->txn_stat_print(bdberl_db_env(), flags);
            RETURN_INT(rc, outbuf);
        }
        default:
            abort();
    }
}

