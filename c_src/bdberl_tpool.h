/* -------------------------------------------------------------------
 *
 * bdberl: Thread Pool 
 * Copyright (c) 2008-9 The Hive http://www.thehive.com/
 * Authors: Dave "dizzyd" Smith <dizzyd@dizzyd.com>
 *          Phil Toland <phil.toland@gmail.com>
 *          Jon Meredith <jon@jonmeredith.com>
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

void bdberl_tpool_job_count(TPool* tpool, unsigned int *pending_count_ptr, 
                            unsigned int *active_count_ptr);

#endif
