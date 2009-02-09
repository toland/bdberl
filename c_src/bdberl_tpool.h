/* -------------------------------------------------------------------
 *
 * bdberl: Thread Pool 
 * Copyright (c) 2008 The Hive.  All rights reserved.
 *
 * ------------------------------------------------------------------- */
#ifndef _BDBERL_TPOOL_DRV
#define _BDBERL_TPOOL_DRV

#include "erl_driver.h"

typedef void (*TPoolJobFunc)(void* arg);

typedef struct _TPoolJob
{
    TPoolJobFunc main_fn;      /* Function to invoke for this job */

    TPoolJobFunc cancel_fn;    /* Function that gets invoked if job is canceled before it can run */

    void* arg;                  /* Input data for the function */

    unsigned int running;       /* Flag indicating if the job is currently running */

    unsigned int canceled;      /* Flag indicating if the job was canceled */

    struct _TPoolJob* next;     /* Next job in the queue */

} TPoolJob;


typedef struct
{
    ErlDrvMutex* lock;

    ErlDrvCond* work_cv;

    ErlDrvCond* cancel_cv;

    TPoolJob* pending_jobs;

    TPoolJob* last_pending_job;

    TPoolJob* active_jobs;

    unsigned int pending_job_count;

    unsigned int active_job_count;

    ErlDrvTid* threads;

    unsigned int thread_count;

    unsigned int active_threads;

    unsigned int shutdown;
    
} TPool;

TPool* bdberl_tpool_start(unsigned int thread_count);

void   bdberl_tpool_stop(TPool* tpool);

void bdberl_tpool_run(TPool* tpool, TPoolJobFunc main_fn, void* arg, TPoolJobFunc cancel_fn,
    TPoolJob** job_ptr);

void bdberl_tpool_cancel(TPool* tpool, TPoolJob* job);


#endif
