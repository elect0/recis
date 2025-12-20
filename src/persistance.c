#include "../include/persistance.h"
#include <stdio.h>
#include <string.h>

#define RDB_TYPE_STRING 0
#define RDB_TYPE_LIST 1
#define RDB_TYPE_SET 2

void rdb_save(HashTable *db, char *filename){
  FILE *fp = fopen(filename, "wb"); // write binary
  if (!fp) {
    printf("[ERROR] Couldn't open file for writing: %s\n", filename);
    return;
  }

  for (size_t i = 0; i < db->size; i++) {
    Node *node = db->buckets[i];
    while (node) {
      char *key = node->key;
      r_obj *val = (r_obj *)node->value;

      unsigned char type = (unsigned char)val->type;
      fwrite(&type, sizeof(unsigned char), 1, fp);

      int key_len = strlen(key);
      fwrite(&key_len, sizeof(int), 1, fp);

      fwrite(key, key_len, 1, fp);

      if (val->type == STRING) {
        char *str_val = (char *)val->data;
        int val_len = strlen(str_val);

        fwrite(&val_len, sizeof(int), 1, fp);
        fwrite(str_val, val_len, 1, fp);
      } else if (val->type == SET) {
        HashTable *set_ht = (HashTable *)val->data;

        int count = set_ht->count;
        fwrite(&count, sizeof(int), 1, fp);

        for (size_t j = 0; j < set_ht->size; j++) {
          Node *set_node = set_ht->buckets[j];
          while (set_node) {
            char *member = set_node->key;
            int member_len = strlen(member);

            fwrite(&member_len, sizeof(int), 1, fp);
            fwrite(member, member_len, 1, fp);

            set_node = set_node->next;
          }
        }
      }
      // todo: add list;
      node = node->next;
    }
  }
  fclose(fp);
  printf("RDB save completed.");
}
