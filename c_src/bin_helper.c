/* -------------------------------------------------------------------
 *
 * bin_helper: ErlDrvBinary helper functions
 * Copyright (c) 2008 The Hive.  All rights reserved.
 *
 * ------------------------------------------------------------------- */
#include "bin_helper.h"

#include <string.h>
#include <stdlib.h>


static void bin_helper_check_size(BinHelper* bh, int space_needed)
{
    if (bh->offset + space_needed > bh->bin->orig_size)
    {
        bh->bin = driver_realloc_binary(bh->bin, bh->offset + space_needed);
    }
}

void bin_helper_init(BinHelper* bh, unsigned int size)
{
    bh->bin = driver_alloc_binary(size);
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
    int sz = strlen(string);
    bin_helper_check_size(bh, sz+1);
    strncpy(bh->bin->orig_bytes+(bh->offset), string, sz+1);
    bh->offset += sz + 1;
}
