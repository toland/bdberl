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
#include "bin_helper.h"

#include <string.h>
#include <stdlib.h>


static void bin_helper_check_size(BinHelper* bh, int space_needed)
{
    if (bh->bin && (bh->offset + space_needed > bh->bin->orig_size))
    {
        bh->bin = driver_realloc_binary(bh->bin, bh->offset + space_needed);
    }
    else if (!bh->bin)
    {
        bh->bin = driver_alloc_binary(space_needed);
    }
}

void bin_helper_init(BinHelper* bh)
{
    bh->bin = 0;
    bh->offset = 0;
}

void bin_helper_push_byte(BinHelper* bh, int value)
{
    bin_helper_check_size(bh, 1);
    bh->bin->orig_bytes[bh->offset] = (char)value;
    bh->offset++;
}

void bin_helper_push_int32(BinHelper* bh, int value)
{
    bin_helper_check_size(bh, 4);
    memcpy(bh->bin->orig_bytes+(bh->offset), (char*)&value, 4);
    bh->offset += 4;
}

void bin_helper_push_string(BinHelper* bh, const char* string)
{
    if (NULL == string)
    {
        string = "<null>";
    }
    int sz = strlen(string);
    bin_helper_check_size(bh, sz+1);
    strncpy(bh->bin->orig_bytes+(bh->offset), string, sz+1);
    bh->offset += sz + 1;
}
