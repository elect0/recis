#ifndef HASH_TABLE_H
#define HASH_TABLE_H

#include <stddef.h>
typedef enum { STRING = 0, LIST = 1, SET = 2, HASH = 3, INT = 4 } obj_type;

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
void free_object(r_obj *o);

HashTable *hash_table_create(size_t size);
void hash_table_set(HashTable *hash_table, const char *key, r_obj *val);
r_obj *hash_table_get(HashTable *hash_table, const char *key);
int hash_table_del(HashTable *hash_table, const char *key);
void hash_table_destroy(HashTable *hash_table);


#endif // !HASH_TABLE_H
