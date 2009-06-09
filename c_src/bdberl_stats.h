/* -------------------------------------------------------------------
 *
 * bdberl: Berkeley DB Driver for Erlang - Stats
 * Copyright (c) 2008-9 The Hive.  All rights reserved.
 *
 * ------------------------------------------------------------------- */
#ifndef _BDBERL_STATS
#define _BDBERL_STATS

/**
 * Prototypes in bdberl_stats.c
 */
int bdberl_stats_control(PortData* d, unsigned int cmd, 
                         char* inbuf, int inbuf_sz, 
                         char** outbuf, int outbuf_sz);

#endif // _BDBERL_STATS
