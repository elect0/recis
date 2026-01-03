#ifndef HASH_TABLE_H
#define HASH_TABLE_H

#include "bytes.h"
#include <stddef.h>

struct RObj;
typedef struct RObj r_obj;

typedef struct Node_ {
  Bytes *key;
  r_obj *value;
  struct Node_ *next;
} Node;

typedef struct HashTable_ {
  Node **buckets;
  size_t size;
  size_t count;
} HashTable;

r_obj *create_hash_object();
HashTable *hash_table_create(size_t size);
void hash_table_set(HashTable *hash_table, Bytes *key, r_obj *val);
r_obj *hash_table_get(HashTable *hash_table, Bytes *key);
int hash_table_del(HashTable *hash_table, Bytes *key);
void hash_table_destroy(HashTable *hash_table);

#endif // !HASH_TABLE_H
