#include "../include/persistance.h"
#include "../include/list.h"
#include "../include/recis.h"
#include "../include/set.h"
#include "../include/zset.h"
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define RDB_TYPE_STRING 0
#define RDB_TYPE_LIST 1
#define RDB_TYPE_SET 2
#define RDB_TYPE_HASH 3
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
      Bytes *key = node->key;
      r_obj *val = (r_obj *)node->value;

      r_obj *expire_entry = hash_table_get(expires, key);
      uint64_t expire_time = 0;

      if (expire_entry != NULL) {
        expire_time = (long long)expire_entry->data;
      }

      unsigned char type = (unsigned char)val->type;
      fwrite(&type, sizeof(unsigned char), 1, fp);

      fwrite(&expire_time, sizeof(uint64_t), 1, fp);

      uint32_t key_len = key->length;

      fwrite(&key_len, sizeof(uint32_t), 1, fp);

      fwrite(key->data, key_len, 1, fp);

      if (val->type == STRING) {
        Bytes *b = (Bytes *)val->data;

        char *str_val = b->data;
        uint32_t val_len = b->length;

        fwrite(&val_len, sizeof(uint32_t), 1, fp);
        fwrite(str_val, val_len, 1, fp);
      } else if (val->type == SET) {
        HashTable *set_ht = (Set *)val->data;

        uint64_t count = (uint64_t)set_ht->count;
        fwrite(&count, sizeof(uint64_t), 1, fp);

        for (size_t j = 0; j < set_ht->size; j++) {
          Node *set_node = set_ht->buckets[j];
          while (set_node) {
            Bytes *member = set_node->key;
            uint32_t member_len = member->length;

            fwrite(&member_len, sizeof(uint32_t), 1, fp);
            fwrite(member->data, member_len, 1, fp);

            set_node = set_node->next;
          }
        }
      } else if (val->type == HASH) {
        HashTable *ht = (HashTable *)val->data;

        uint64_t count = (uint64_t)ht->count;
        fwrite(&count, sizeof(uint64_t), 1, fp);

        for (size_t j = 0; j < ht->size; j++) {
          Node *set_node = ht->buckets[j];
          while (set_node) {
            Bytes *field = set_node->key;
            uint32_t field_len = field->length;

            fwrite(&field_len, sizeof(uint32_t), 1, fp);
            fwrite(field->data, field_len, 1, fp);

            r_obj *value_o = (r_obj *)set_node->value;
            Bytes *value_bytes = (Bytes *)value_o->data;
            uint32_t value_len = value_bytes->length;

            fwrite(&value_len, sizeof(uint32_t), 1, fp);
            fwrite(value_bytes->data, value_len, 1, fp);

            set_node = set_node->next;
          }
        }

      } else if (val->type == LIST) {
        List *list = (List *)val->data;

        uint64_t size = (uint64_t)list->size;
        fwrite(&size, sizeof(uint64_t), 1, fp);

        ListNode *member;
        for (member = list->head; member != NULL; member = member->next) {
          r_obj *item_o = member->value;
          Bytes *item_bytes = item_o->data;
          uint32_t item_len = item_bytes->length;

          fwrite(&item_len, sizeof(uint32_t), 1, fp);
          fwrite(item_bytes->data, item_len, 1, fp);
        }
      } else if (val->type == ZSET) {
        ZSet *zs = (ZSet *)val->data;
        ZSkipList *zsl = zs->zsl;

        uint64_t length = (uint64_t)zsl->length;
        fwrite(&length, sizeof(uint64_t), 1, fp);

        ZSkipListNode *node = zsl->head->level[0].forward;
        while (node) {
          Bytes *element = node->element;
          uint32_t mem_len = element->length;
          fwrite(&mem_len, sizeof(uint32_t), 1, fp);
          fwrite(element->data, mem_len, 1, fp);

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

    uint64_t expire_time;
    fread(&expire_time, sizeof(uint64_t), 1, fp);

    uint32_t key_len;
    fread(&key_len, sizeof(uint32_t), 1, fp);

    char *key = (char *)malloc(key_len + 1);
    fread(key, key_len, 1, fp);
    key[key_len] = '\0';

    printf("[RDB] Loading key: %s, type: %d\n", key, type);

    if (type == RDB_TYPE_STRING) {
      uint32_t val_len;
      fread(&val_len, sizeof(uint32_t), 1, fp);

      char *val_str = (char *)malloc(val_len + 1);
      fread(val_str, val_len, 1, fp);
      val_str[val_len] = '\0';

      r_obj *o = create_string_object(val_str, val_len);

      hash_table_set(db, create_bytes_object(val_str, val_len), o);
      free(val_str);
    } else if (type == RDB_TYPE_SET) {
      uint64_t count;
      if (fread(&count, sizeof(uint64_t), 1, fp) != 1)
        break;

      r_obj *o = create_set_object();

      for (uint64_t i = 0; i < count; i++) {
        uint32_t member_len;
        fread(&member_len, sizeof(uint32_t), 1, fp);

        char *member = (char *)malloc(member_len + 1);
        fread(member, member_len, 1, fp);
        member[member_len] = '\0';

        set_add((Set *)o->data, create_bytes_object(member, member_len));
        free(member);
      }

      hash_table_set(db, create_bytes_object(key, key_len), o);
    } else if (type == RDB_TYPE_HASH) {
      uint64_t count;
      if (fread(&count, sizeof(uint64_t), 1, fp) != 1)
        break;

      r_obj *o = create_hash_object();

      for (uint64_t i = 0; i < count; i++) {
        uint32_t field_len;
        fread(&field_len, sizeof(uint32_t), 1, fp);

        char *field = (char *)malloc(field_len + 1);
        fread(field, field_len, 1, fp);
        field[field_len] = '\0';

        uint32_t value_len;
        fread(&value_len, sizeof(uint32_t), 1, fp);

        char *value = (char *)malloc(value_len + 1);
        fread(value, value_len, 1, fp);
        value[value_len] = '\0';

        hash_table_set((HashTable *)o->data,
                       create_bytes_object(field, field_len),
                       create_string_object(value, value_len));
        free(field);
        free(value);
      }
      hash_table_set(db, create_bytes_object(key, key_len), o);
    } else if (type == RDB_TYPE_LIST) {
      uint64_t size;
      if (fread(&size, sizeof(uint64_t), 1, fp) != 1)
        break;

      r_obj *o = create_list_object();
      List *list = (List *)o->data;

      for (uint64_t i = 0; i < size; i++) {
        uint32_t val_len;
        fread(&val_len, sizeof(uint32_t), 1, fp);

        char *val_str = malloc(val_len + 1);
        fread(val_str, val_len, 1, fp);

        val_str[val_len] = '\0';

        list_ins_node_tail(list, create_string_object(val_str, val_len));
      }
      hash_table_set(db, create_bytes_object(key, key_len), o);
    } else if (type == RDB_TYPE_ZSET) {
      uint64_t length;
      if (fread(&length, sizeof(uint64_t), 1, fp) != 1)
        break;

      r_obj *o = create_zset_object();
      ZSet *zs = (ZSet *)o->data;

      for (uint64_t i = 0; i < length; i++) {
        uint32_t mem_len;
        fread(&mem_len, sizeof(uint32_t), 1, fp);
        char *member = malloc(mem_len + 1);
        fread(member, mem_len, 1, fp);
        member[mem_len] = '\0';

        double score;
        fread(&score, sizeof(double), 1, fp);

        zset_add(zs, create_bytes_object(member, mem_len), score);
        free(member);
      }
      hash_table_set(db, create_bytes_object(key, key_len), o);
    }
    if (expire_time > 0) {
      time_t now = time(NULL);
      uint64_t current_ms = (uint64_t)now * 1000;

      if (expire_time < current_ms) {
        hash_table_del(db, create_bytes_object(key, key_len));
      } else {
        hash_table_set(expires, create_bytes_object(key, key_len),
                       create_int_object(expire_time));
      }
    }

    free(key);
  }

  fclose(fp);
  printf("RDB data loaded successfully\n");
}
