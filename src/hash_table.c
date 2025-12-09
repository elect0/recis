#include "../include/hash_table.h"
#include "string.h"
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
  if((new_node = (Node *)malloc(sizeof(Node))) == NULL) {
    return;
  }

  new_node->key = strdup(key);
  new_node->value = val;
  new_node->next = hash_table->buckets[slot];

  hash_table->buckets[slot] = new_node;
  hash_table->count++;
}
