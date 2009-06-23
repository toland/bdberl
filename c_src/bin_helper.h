/* -------------------------------------------------------------------
 *
 * bin_helper: ErlDrvBinary helper functions
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
#ifndef _BIN_HELPER
#define _BIN_HELPER

#include "erl_driver.h"

typedef struct
{
    ErlDrvBinary* bin;
    unsigned int offset;
} BinHelper;

void bin_helper_init(BinHelper* bh);
void bin_helper_push_byte(BinHelper* bh, int value);
void bin_helper_push_int32(BinHelper* bh, int value);
void bin_helper_push_string(BinHelper* bh, const char* string);

#endif
