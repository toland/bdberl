/* -------------------------------------------------------------------
 *
 * bdberl: CRC checks
 * Copyright (c) 2008-9 The Hive.  All rights reserved.
 *
 * ------------------------------------------------------------------- */
#ifndef _BDBERL_CRC32
#define _BDBERL_CRC32

#include <stdint.h>

uint32_t bdberl_crc32(const unsigned char *blk_adr, unsigned int blk_len);

#endif // _BDBERL_CRC32
