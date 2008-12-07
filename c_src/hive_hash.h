/* Copyright 2006 David Crawshaw, released under the new BSD license.
 * Version 2, from http://www.zentus.com/c/hash.html */

/* Changed from just "hash" to "hive_hash" to reduce collisions when linked
 * in with erlang 
 *
 * Dave Smith (dsmith@thehive.com) 12/08
 */

#ifndef __HIVE_HASH__
#define __HIVE_HASH__

/* Opaque structure used to represent hashtable. */
typedef struct hive_hash hive_hash;

/* Create new hashtable. */
hive_hash * hive_hash_new(unsigned int size);

/* Free hashtable. */
void hive_hash_destroy(hive_hash *h);

/* Add key/value pair. Returns non-zero value on error (eg out-of-memory). */
int hive_hash_add(hive_hash *h, const char *key, void *value);

/* Return value matching given key. */
void * hive_hash_get(hive_hash *h, const char *key);

/* Remove key from table, returning value. */
void * hive_hash_remove(hive_hash *h, const char *key);

/* Returns total number of keys in the hashtable. */
unsigned int hive_hash_size(hive_hash *h);

#endif
