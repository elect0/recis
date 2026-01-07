#include "../include/client.h"
#include "../include/hnsw.h"
#include "../include/list.h"
#include "../include/recis.h"
#include "../include/set.h"
#include "../include/zset.h"
#include "string.h"
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

unsigned long hash(const Bytes *key) {
  unsigned int val = 0;
  unsigned int tmp;

  for (uint32_t i = 0; i < key->length; i++) {
    val = (val << 4) + key->data[i];
    if ((tmp = (val & 0xf0000000))) {
      val = val ^ (tmp >> 24);
      val = val ^ tmp;
    }
  }

  return val;
}

static void hash_table_resize(HashTable *hash_table) {
  size_t new_size = hash_table->size * 2;

  Node **new_buckets = calloc(new_size, sizeof(Node *));
  if (new_buckets == NULL)
    return;

  for (size_t i = 0; i < hash_table->size; i++) {
    Node *node = hash_table->buckets[i];
    while (node) {
      Node *next_node = node->next;

      unsigned int new_index = hash(node->key) % new_size;

      node->next = new_buckets[new_index];
      new_buckets[new_index] = node;

      node = next_node;
    }
  }

  free(hash_table->buckets);

  hash_table->buckets = new_buckets;
  hash_table->size = new_size;

  return;
}

r_obj *create_hash_object() {
  r_obj *o;
  if ((o = (r_obj *)malloc(sizeof(r_obj))) == NULL)
    return NULL;

  o->type = HASH;
  o->data = hash_table_create(64);

  if (o->data == NULL) {
    free(o);
    return NULL;
  }

  return o;
}

HashTable *hash_table_create(size_t size) {
  HashTable *hash_table;

  if ((hash_table = (HashTable *)malloc(sizeof(HashTable))) == NULL) {
    return NULL;
  }

  hash_table->buckets = calloc(size, sizeof(Node *));

  hash_table->size = size;
  hash_table->count = 0;

  return hash_table;
}

r_obj *create_string_object(const char *str, uint32_t length) {
  r_obj *o;
  if ((o = (r_obj *)malloc(sizeof(r_obj))) == NULL) {
    return NULL;
  }
  o->type = STRING;

  Bytes *b = create_bytes_object(str, length);
  if (b == NULL) {
    free(o);
    return NULL;
  }

  o->data = b;

  return o;
}

r_obj *create_int_object(long long value) {
  r_obj *o;
  if ((o = (r_obj *)malloc(sizeof(r_obj))) == NULL) {
    return NULL;
  }

  o->type = INT;
  long long *ptr = malloc(sizeof(long long));
  *ptr = value;
  o->data = ptr;
  return o;
}

r_obj *create_double_object(double value) {
  r_obj *o;
  if ((o = (r_obj *)malloc(sizeof(r_obj))) == NULL)
    return NULL;

  o->type = DOUBLE;
  double *ptr = malloc(sizeof(double));
  *ptr = value;
  o->data = ptr;
  return o;
}

void hash_table_set(HashTable *hash_table, Bytes *key, r_obj *val) {

  if (hash_table->count >= hash_table->size) {
    hash_table_resize(hash_table);
  }

  unsigned int slot = hash(key) % hash_table->size;

  Node *entry = hash_table->buckets[slot];
  while (entry) {
    if (bytes_equal(key, entry->key) == 1) {
      free_object(entry->value);
      entry->value = val;

      return;
    }
    entry = entry->next;
  }

  Node *new_node;
  if ((new_node = (Node *)malloc(sizeof(Node))) == NULL) {
    return;
  }

  new_node->key = bytes_dup(key);
  new_node->value = val;
  new_node->next = hash_table->buckets[slot];

  hash_table->buckets[slot] = new_node;
  hash_table->count++;
}

r_obj *hash_table_get(HashTable *hash_table, Bytes *key) {

  unsigned int slot = hash(key) % hash_table->size;

  Node *entry = hash_table->buckets[slot];
  while (entry) {
    if (bytes_equal(key, entry->key) == 1) {
      return entry->value;
    }
    entry = entry->next;
  }

  return NULL;
}

void free_object(r_obj *o) {

  if (o == NULL)
    return;

  switch (o->type) {
  case STRING:
    if (o->data) {
      free_bytes_object((Bytes *)o->data);
    }
    break;
  case INT:
    if (o->data)
      free(o->data);
    break;
  case LIST:
    if (o->data)
      list_destroy((List *)o->data);
    break;
  case ZSET:
    zset_destroy((ZSet *)o->data);
    break;
  case SET:
    hash_table_destroy((Set *)o->data);
    break;
  case HASH:
    hash_table_destroy((HashTable *)o->data);
    break;
  case VECTOR:
    vector_free((Vector *)o->data);
    break;
  case HNSW:
    hnsw_free((HNSWIndex *)o->data);
  }

  free(o);
}

int hash_table_del(HashTable *hash_table, Bytes *key) {
  unsigned int slot = hash(key) % hash_table->size;
  Node *entry = hash_table->buckets[slot];
  Node *prev = NULL;

  while (entry) {
    if (bytes_equal(entry->key, key) == 1) {
      if (prev == NULL) {
        hash_table->buckets[slot] = entry->next;
      } else {
        prev->next = entry->next;
      }

      free_object(entry->value);

      free_bytes_object(entry->key);
      free(entry);

      hash_table->count--;
      return 1;
    }

    prev = entry;
    entry = entry->next;
  }

  return 0;
}

void hash_table_destroy(HashTable *hash_table) {
  if (!hash_table)
    return;

  unsigned int i;
  for (i = 0; i < hash_table->size; i++) {
    Node *entry = hash_table->buckets[i];
    while (entry) {
      Node *next = entry->next;

      if (entry->key) {
        free_bytes_object(entry->key);
      }

      if (entry->value) {
        free_object(entry->value);
      }

      free(entry);

      entry = next;
    }
  }

  free(hash_table->buckets);

  free(hash_table);
}
