/* -------------------------------------------------------------------
 *
 * bdberl: Berkeley DB Driver for Erlang - Stats
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
#ifndef _BDBERL_STATS
#define _BDBERL_STATS

/**
 * Prototypes in bdberl_stats.c
 */
int bdberl_stats_control(PortData* d, unsigned int cmd, 
                         char* inbuf, int inbuf_sz, 
                         char** outbuf, int outbuf_sz);

#endif // _BDBERL_STATS
