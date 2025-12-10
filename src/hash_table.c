#include "../include/hash_table.h"
#include "string.h"
#include <stdio.h>
#include <stdlib.h>

unsigned long hash(const char *key) {
  unsigned int val = 0;
  const char *ptr = key;
  unsigned int tmp;

  while (*ptr != '\0') {
    val = (val << 4) + (*ptr);

    if ((tmp = (val & 0xf0000000))) {
      val = val ^ (tmp >> 24);
      val = val ^ tmp;
    }
    ptr++;
  }
  return val;
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

r_obj *create_string_object(const char *str) {
  r_obj *o;
  if ((o = (r_obj *)malloc(sizeof(r_obj))) == NULL) {
    return NULL;
  }
  o->type = STRING;
  o->data = strdup(str);

  return o;
}

void hash_table_set(HashTable *hash_table, const char *key, r_obj *val) {
  unsigned int slot = hash(key) % hash_table->size;

  Node *entry = hash_table->buckets[slot];
  while (entry) {
    if (strcmp(entry->key, key) == 0) {
      entry->value = val;
      return;
    }
    entry = entry->next;
  }

  Node *new_node;
  if ((new_node = (Node *)malloc(sizeof(Node))) == NULL) {
    return;
  }

  new_node->key = strdup(key);
  new_node->value = val;
  new_node->next = hash_table->buckets[slot];

  hash_table->buckets[slot] = new_node;
  hash_table->count++;
}

r_obj *hash_table_get(HashTable *hash_table, const char *key) {

  unsigned int slot = hash(key) % hash_table->size;

  Node *entry = hash_table->buckets[slot];
  while (entry) {
    if (strcmp(entry->key, key) == 0) {
      return entry->value;
    }
    entry = entry->next;
  }

  return NULL;
}

void free_object(r_obj *o) {
  if (o == NULL)
    return;
  if (o->data)
    free(o->data);
  free(o);
}
