#include "../include/persistance.h"
#include "../include/list.h"
#include "../include/set.h"
#include "../include/zset.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define RDB_TYPE_STRING 0
#define RDB_TYPE_LIST 1
#define RDB_TYPE_SET 2
#define RDB_TYPE_ZSET 6

void rdb_save(HashTable *db, HashTable *expires, char *filename) {
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

      r_obj *expire_entry = hash_table_get(expires, key);
      long long expire_time = 0;

      if (expire_entry != NULL) {
        expire_time = (long long)expire_entry->data;
      }

      unsigned char type = (unsigned char)val->type;
      fwrite(&type, sizeof(unsigned char), 1, fp);

      fwrite(&expire_time, sizeof(long long), 1, fp);

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
      } else if (val->type == LIST) {
        List *list = (List *)val->data;

        int size = list->size;
        fwrite(&size, sizeof(int), 1, fp);

        ListNode *member;
        for (member = list->head; member != NULL; member = member->next) {
          char *item = member->value;
          int item_len = strlen(item);

          fwrite(&item_len, sizeof(int), 1, fp);
          fwrite(item, item_len, 1, fp);
        }
      } else if (val->type == ZSET) {
        ZSet *zs = (ZSet *)val->data;
        ZSkipList *zsl = zs->zsl;

        unsigned int length = zsl->length;
        fwrite(&length, sizeof(unsigned int), 1, fp);

        ZSkipListNode *node = zsl->head->level[0].forward;
        while (node) {
          int mem_len = strlen(node->element);
          fwrite(&mem_len, sizeof(int), 1, fp);
          fwrite(node->element, mem_len, 1, fp);

          fwrite(&node->score, sizeof(double), 1, fp);

          node = node->level[0].forward;
        }
      }
      node = node->next;
    }
  }
  fclose(fp);
  printf("RDB save completed.");
}

void rdb_load(HashTable *db, HashTable *expires, char *filename) {
  FILE *fp = fopen(filename, "rb");
  if (!fp) {
    printf("[ERROR] Couldn't open file for reading: %s\n", filename);
    return;
  }

  printf("[RDB] Loading data from disk...\n");

  unsigned char type;
  while (fread(&type, sizeof(unsigned char), 1, fp)) {

    long long expire_time;
    fread(&expire_time, sizeof(long long), 1, fp);

    int key_len;
    fread(&key_len, sizeof(int), 1, fp);

    char *key = (char *)malloc(key_len + 1);
    fread(key, key_len, 1, fp);
    key[key_len] = '\0';

    printf("[RDB] Loading key: %s, type: %d\n", key, type);

    if (type == RDB_TYPE_STRING) {
      int val_len;
      fread(&val_len, sizeof(int), 1, fp);

      char *val_str = (char *)malloc(val_len + 1);
      fread(val_str, val_len, 1, fp);
      val_str[val_len] = '\0';

      r_obj *o = create_string_object(val_str);

      hash_table_set(db, strdup(key), o);
    } else if (type == RDB_TYPE_SET) {
      int count;
      if (fread(&count, sizeof(int), 1, fp) != 1)
        break;

      r_obj *o = create_set_object();

      for (int i = 0; i < count; i++) {
        int member_len;
        fread(&member_len, sizeof(int), 1, fp);

        char *member = (char *)malloc(member_len + 1);
        fread(member, member_len, 1, fp);
        member[member_len] = '\0';

        set_add((HashTable *)o->data, member);
      }

      hash_table_set(db, strdup(key), o);
    } else if (type == RDB_TYPE_LIST) {
      int size;
      if (fread(&size, sizeof(int), 1, fp) != 1)
        break;

      r_obj *o = create_list_object();
      List *list = (List *)o->data;

      for (int i = 0; i < size; i++) {
        int val_len;
        fread(&val_len, sizeof(int), 1, fp);

        char *val_str = malloc(val_len + 1);
        fread(val_str, val_len, 1, fp);

        val_str[val_len] = '\0';

        list_ins_node_tail(list, val_str);
      }
      hash_table_set(db, strdup(key), o);
    } else if (type == RDB_TYPE_ZSET) {
      unsigned int length;
      if (fread(&length, sizeof(unsigned int), 1, fp) != 1)
        break;


      r_obj *o = create_zset_object();
      ZSet *zs = (ZSet *)o->data;

      for (unsigned int i = 0; i < length; i++) {
        int mem_len;
        fread(&mem_len, sizeof(int), 1, fp);
        char *member = malloc(mem_len + 1);
        fread(member, mem_len, 1, fp);
        member[mem_len] = '\0';

        double score;
        fread(&score, sizeof(double), 1, fp);

        zset_add(zs, member, score);

        free(member);

        hash_table_set(db, strdup(key), o);
      }
    }

    if (expire_time > 0) {
      time_t now = time(NULL);
      long long current_ms = now * 1000;

      if (expire_time < current_ms) {
        hash_table_del(db, key);
      } else {
        hash_table_set(expires, strdup(key), create_int_object(expire_time));
      }
    }

    free(key);
  }

  fclose(fp);
  printf("RDB data loaded successfully\n");
}
