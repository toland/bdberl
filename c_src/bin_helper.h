/* -------------------------------------------------------------------
 *
 * bin_helper: ErlDrvBinary helper functions
 * Copyright (c) 2008 The Hive.  All rights reserved.
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

void bin_helper_init(BinHelper* bh, unsigned int size);
void bin_helper_push_byte(BinHelper* bh, int value);
void bin_helper_push_int32(BinHelper* bh, int value);
void bin_helper_push_string(BinHelper* bh, const char* string);

#endif
