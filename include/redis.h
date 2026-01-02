#ifndef HASH_TABLE_H
#define HASH_TABLE_H

#include <stddef.h>

// Bitmasks for SET command
#define OBJ_SET_NO_FLAGS 0
#define OBJ_SET_NX (1 << 0)
#define OBJ_SET_XX (1 << 1)
#define OBJ_SET_EX (1 << 2)
#define OBJ_SET_PX (1 << 3)
#define OBJ_SET_KEEPTTL (1 << 4)
#define OBJ_SET_GET (1 << 5)
#define OBJ_SET_IFEQ (1 << 6)
#define OBJ_SET_IFNE (1 << 7)

typedef enum {
  STRING = 0,
  LIST = 1,
  SET = 2,
  HASH = 3,
  INT = 4,
  DOUBLE = 5,
  ZSET = 6,
  COMMAND = 7,
} obj_type;

typedef struct RObj {
  obj_type type;
  void *data;
} r_obj;

typedef struct Node_ {
  char *key;
  r_obj *value;
  struct Node_ *next;
} Node;

typedef struct {
  Node **buckets;
  size_t size;
  size_t count;
} HashTable;

r_obj *create_string_object(const char *str);
r_obj *create_int_object(long long value);
r_obj *create_double_object(double value);
void free_object(r_obj *o);

r_obj *create_hash_object();
HashTable *hash_table_create(size_t size);
void hash_table_set(HashTable *hash_table, const char *key, r_obj *val);
r_obj *hash_table_get(HashTable *hash_table, const char *key);
int hash_table_del(HashTable *hash_table, const char *key);
void hash_table_destroy(HashTable *hash_table);

#endif // !HASH_TABLE_H
