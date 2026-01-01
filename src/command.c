#include "../include/command.h"
#include "../include/list.h"
#include "../include/persistance.h"
#include "../include/set.h"
#include "../include/zset.h"
#include <ctype.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <strings.h>
#include <time.h>
#include <unistd.h>

Command CommandTable[] = {
    {"SET", set_command, -3},      {"GET", get_command, 2},
    {"DEL", del_command, -2},      {"TTL", ttl_command, 2},
    {"LPUSH", lpush_command, -3},  {"RPUSH", rpush_command, -3},
    {"RPOP", rpop_command, -2},    {"LPOP", lpop_command, -2},
    {"LLEN", llen_command, 2},     {"LINDEX", lindex_command, 3},
    {"LRANGE", lrange_command, 4}, {"LMOVE", lmove_command, 5},
    {"SADD", sadd_command, -3},    {"SISMEMBER", sismember_command, 3},
    {"ZADD", zadd_command, -4},    {"ZRANGE", zrange_command, -4},
    {"ZSCORE", zscore_command, 3}, {"ZRANK", zrank_command, 3},
    {"SAVE", save_command, 1},     {NULL, NULL, 0}};

r_obj *create_command_object(Command *cmd) {
  r_obj *o;
  if ((o = (r_obj *)malloc(sizeof(r_obj))) == NULL)
    return NULL;
  o->type = COMMAND;
  o->data = cmd;
  return o;
}

void populate_command_table(HashTable *registry) {
  for (int i = 0; CommandTable[i].name != NULL; i++) {
    r_obj *o = create_command_object(&CommandTable[i]);

    hash_table_set(registry, CommandTable[i].name, o);
  }
}

int is_valid_alpha_string(const char *str) {
  if (str == NULL || *str == '\0')
    return 0; // empty or NULL is invalid

  for (; *str; str++) {
    if (!isalpha((unsigned char)*str) && (unsigned char)*str != '[' &&
        (unsigned char)*str != '(')
      return 0;
  }
  return 1;
}

long long get_time_ms() { return (long long)time(NULL) * 1000; }

void set_command(Client *client, HashTable *db, HashTable *expires,
                 OutputBuffer *ob) {
  char **arg_values = client->arg_values;
  int arg_count = client->arg_count;

  if (arg_count < 3) {
    append_to_output_buffer(ob, "-ERR args\r\n", 11);
    return;
  }

  int flags = OBJ_SET_NO_FLAGS;
  long long expire_at = -1;

  char *ifeq_val = NULL;
  char *ifne_val = NULL;

  int j;
  for (j = 3; j < arg_count; j++) {
    char *option = arg_values[j];
    if (strcasecmp(option, "NX") == 0) {
      flags |= OBJ_SET_NX;
    } else if (strcasecmp(option, "XX") == 0) {
      flags |= OBJ_SET_XX;
    } else if (strcasecmp(option, "GET") == 0) {
      flags |= OBJ_SET_GET;
    } else if (strcasecmp(option, "KEEPTTL") == 0) {
      flags |= OBJ_SET_KEEPTTL;
    } else if (strcasecmp(option, "EX") == 0 && j + 1 < arg_count) {
      long long value = atoll(arg_values[++j]);
      if (value <= 0) {
        append_to_output_buffer(ob, "-ERR invalid expire time\r\n", 26);
        return;
      }
      expire_at = get_time_ms() + (value * 1000);
    } else if (strcasecmp(option, "PX") == 0 && j + 1 < arg_count) {
      long long value = atoll(arg_values[++j]);
      if (value <= 0) {
        append_to_output_buffer(ob, "-ERR invalid expire time\r\n", 26);
        return;
      }
      expire_at = get_time_ms() + value;
    } else if (strcasecmp(option, "EXAT") == 0 && j + 1 < arg_count) {
      long long value = atoll(arg_values[++j]);
      if (value <= 0) {
        append_to_output_buffer(ob, "-ERR invalid expire time\r\n", 26);
        return;
      }
      expire_at = value * 1000;
    } else if (strcasecmp(option, "PXAT") == 0 && j + 1 < arg_count) {
      long long value = atoll(arg_values[++j]);
      if (value <= 0) {
        append_to_output_buffer(ob, "-ERR invalid expire time\r\n", 26);
        return;
      }
      expire_at = value;
    } else if (strcasecmp(option, "IFEQ") == 0 && j + 1 < arg_count) {
      flags |= OBJ_SET_IFEQ;
      ifeq_val = arg_values[++j];
    } else if (strcasecmp(option, "IFNE") == 0 && j + 1 < arg_count) {
      flags |= OBJ_SET_IFNE;
      ifne_val = arg_values[++j];
    } else {
      append_to_output_buffer(ob, "-ERR syntax error\r\n", 19);
      return;
    }
  }

  int nx = flags & OBJ_SET_NX;
  int xx = flags & OBJ_SET_XX;
  int ifeq = flags & OBJ_SET_IFEQ;
  int ifne = flags & OBJ_SET_IFNE;

  if (nx && xx || ifeq && ifne) {
    append_to_output_buffer(ob, "-ERR syntax error\r\n", 19);
    return;
  }

  r_obj *o = hash_table_get(db, arg_values[1]);

  if (nx && o != NULL) {
    append_to_output_buffer(ob, "-ERR syntax error\r\n", 19);
    return;
  }

  if (xx && o == NULL) {
    append_to_output_buffer(ob, "-ERR syntax error\r\n", 19);
    return;
  }

  if (ifeq) {
    if (o == NULL) {
      append_to_output_buffer(ob, "_\r\n", 3);
      return;
    }

    if (o->type != STRING) {
      append_to_output_buffer(ob, "_\r\n", 3);
      return;
    }

    if (strcmp((char *)o->data, ifeq_val) != 0) {
      append_to_output_buffer(ob, "_\r\n", 3);
      return;
    }
  }

  if (ifne) {
    if (o != NULL) {
      if (o->type == STRING && strcmp((char *)o->data, ifne_val) == 0) {
        append_to_output_buffer(ob, "_\r\n", 3);
        return;
      }
    }
  }

  char resp_buf[256];
  int resp_len = 0;

  if (flags & OBJ_SET_GET) {
    if (o == NULL) {
      resp_len = snprintf(resp_buf, sizeof(resp_buf), "_\r\n");
    } else if (o->type != STRING) {
      append_to_output_buffer(ob,
                              "-WRONGTYPE Operation against a key holding the "
                              "wrong kind of value\r\n",
                              68);
      return;
    } else {
      char *old = (char *)o->data;
      snprintf(resp_buf, sizeof(resp_buf), "$%lu\r\n%s\r\n", strlen(old), old);
    }
  }

  resp_len = strlen(resp_buf);

  if ((flags & OBJ_SET_KEEPTTL) && o != NULL && expire_at == -1) {
    r_obj *ttl = hash_table_get(expires, arg_values[1]);
    if (ttl != NULL) {
      expire_at = *(long long *)ttl->data;
    }
  }

  r_obj *new_obj = create_string_object(arg_values[2]);
  hash_table_set(db, arg_values[1], new_obj);

  if (expire_at != -1) {
    hash_table_set(expires, arg_values[1], create_int_object(expire_at));
  } else {
    hash_table_del(expires, arg_values[1]);
  }

  if (!(flags & OBJ_SET_GET)) {
    append_to_output_buffer(ob, "+OK\r\n", 5);
  } else {
    append_to_output_buffer(ob, resp_buf, resp_len);
  }
  return;
}

void get_command(Client *client, HashTable *db, HashTable *expires,
                 OutputBuffer *ob) {
  char **arg_values = client->arg_values;
  int arg_count = client->arg_count;

  if (arg_count < 2) {
    append_to_output_buffer(ob, "-ERR args\r\n", 11);
    return;
  }

  r_obj *exp_obj = hash_table_get(expires, arg_values[1]);

  if (exp_obj != NULL) {
    long long *kill_time = (long long *)exp_obj->data;
    if (get_time_ms() > *kill_time) {
      hash_table_del(db, arg_values[1]);
      hash_table_del(expires, arg_values[1]);

      append_to_output_buffer(ob, "_\r\n", 3);
      return;
    }
  }

  r_obj *o = hash_table_get(db, arg_values[1]);

  if (!o) {
    append_to_output_buffer(ob, "_\r\n", 3);
    return;
  }

  if (o->type != STRING) {
    append_to_output_buffer(ob,
                            "-WRONGTYPE Operation against a key holding the "
                            "wrong kind of value\r\n",
                            68);
    return;
  }

  char *value = (char *)o->data;
  size_t val_len = strlen(value);

  char header[64];
  int head_len = snprintf(header, sizeof(header), "$%zu\r\n", val_len);
  append_to_output_buffer(ob, header, head_len);
  append_to_output_buffer(ob, value, val_len);
  append_to_output_buffer(ob, "\r\n", 2);

  return;
}

void del_command(Client *client, HashTable *db, HashTable *expires,
                 OutputBuffer *ob) {
  char **arg_values = client->arg_values;
  int arg_count = client->arg_count;

  if (arg_count < 2) {
    append_to_output_buffer(ob, "-ERR args\r\n", 11);
    return;
  }

  int deleted_count = 0;

  for (int i = 1; i < arg_count; i++) {
    char *key = arg_values[i];
    r_obj *o = hash_table_get(db, key);

    if (o != NULL) {
      hash_table_del(db, key);
      deleted_count++;
    }
  }

  char resp[64];
  int resp_len = snprintf(resp, sizeof(resp), ":%d\r\n", deleted_count);
  append_to_output_buffer(ob, resp, resp_len);
  return;
}

void ttl_command(Client *client, HashTable *db, HashTable *expires,
                 OutputBuffer *ob) {
  char **arg_values = client->arg_values;
  int arg_count = client->arg_count;

  if (arg_count < 2) {
    append_to_output_buffer(ob, "-ERR args\r\n", 11);
    return;
  }

  r_obj *ttl = hash_table_get(expires, arg_values[1]);
  if (ttl == NULL) {
    append_to_output_buffer(ob, ":-1\r\n", 5);
    return;
  }

  long long seconds = (*(long long *)ttl->data - get_time_ms()) / 1000;
  char resp[64];
  int resp_len = snprintf(resp, sizeof(resp), ":%lld\r\n", seconds);
  append_to_output_buffer(ob, resp, resp_len);
  return;
}

void lpush_command(Client *client, HashTable *db, HashTable *expires,
                   OutputBuffer *ob) {

  char **arg_values = client->arg_values;
  int arg_count = client->arg_count;

  if (arg_count < 3) {
    append_to_output_buffer(ob, "-ERR args\r\n", 11);
    return;
  }

  r_obj *o = hash_table_get(db, arg_values[1]);

  if (o != NULL && o->type != LIST) {
    char *msg = "-WRONGTYPE Operation against a key holding "
                "the wrong kind of value\r\n";
    append_to_output_buffer(ob, msg, strlen(msg));
    return;
  }

  if (!o) {
    o = create_list_object();
    hash_table_set(db, arg_values[1], o);
  }

  int j;
  List *list = (List *)o->data;

  for (j = 2; j < arg_count; j++) {
    list_ins_node_head(list, strdup(arg_values[j]));
  }

  char resp[64];
  int resp_len = snprintf(resp, sizeof(resp), ":%lu\r\n", list->size);
  append_to_output_buffer(ob, resp, resp_len);
  return;
}

void llen_command(Client *client, HashTable *db, HashTable *expires,
                  OutputBuffer *ob) {
  char **arg_values = client->arg_values;
  int arg_count = client->arg_count;

  if (arg_count != 2) {
    append_to_output_buffer(ob, "-ERR args\r\n", 11);
    return;
  }

  r_obj *o = hash_table_get(db, arg_values[1]);

  if (!o) {
    append_to_output_buffer(ob, ":0\r\n", 4);
    return;
  }
  if (o->type != LIST) {
    char *msg = "-WRONGTYPE Operation against a key holding "
                "the wrong kind of value\r\n";
    append_to_output_buffer(ob, msg, strlen(msg));
    return;
  }

  List *list = (List *)o->data;
  char resp[64];
  int resp_len = snprintf(resp, sizeof(resp), ":%lu\r\n", list->size);
  append_to_output_buffer(ob, resp, resp_len);
  return;
}

void lindex_command(Client *client, HashTable *db, HashTable *expires,
                    OutputBuffer *ob) {
  char **arg_values = client->arg_values;
  int arg_count = client->arg_count;

  if (arg_count != 3) {
    append_to_output_buffer(ob, "-ERR args\r\n", 11);
    return;
  }

  r_obj *o = hash_table_get(db, arg_values[1]);

  if (!o) {
    append_to_output_buffer(ob, "_\r\n", 3);
    return;
  }

  if (o->type != LIST) {
    char *msg = "-WRONGTYPE Operation against a key holding "
                "the wrong kind of value\r\n";
    append_to_output_buffer(ob, msg, strlen(msg));
    return;
  }

  List *list = (List *)o->data;
  int index = atoi(arg_values[2]);

  if (index < 0 || index >= list->size) {
    append_to_output_buffer(ob, "_\r\n", 3);
    return;
  }

  ListNode *member = list->head;
  printf("list->head %s", (char *)member->value);

  while (index > 0) {
    member = member->next;
    index--;
  }

  char *value = (char *)member->value;
  size_t val_len = strlen(value);

  char header[64];
  int head_len = snprintf(header, sizeof(header), "$%zu\r\n", val_len);
  append_to_output_buffer(ob, header, head_len);
  append_to_output_buffer(ob, value, val_len);
  append_to_output_buffer(ob, "\r\n", 2);
  return;
}

void lrange_command(Client *client, HashTable *db, HashTable *expires,
                    OutputBuffer *ob) {
  char **arg_values = client->arg_values;
  int arg_count = client->arg_count;

  if (arg_count != 4) {
    append_to_output_buffer(ob, "-ERR args\r\n", 11);
    return;
  }

  r_obj *o = hash_table_get(db, arg_values[1]);

  if (!o) {
    append_to_output_buffer(ob, "*0\r\n", 4);
    return;
  }

  if (o->type != LIST) {
    char *msg = "-WRONGTYPE Operation against a key holding "
                "the wrong kind of value\r\n";
    append_to_output_buffer(ob, msg, strlen(msg));
    return;
  }

  List *list = (List *)o->data;

  int start = atoi(arg_values[2]);
  int stop = atoi(arg_values[3]);

  long llen = list->size;

  if (start < 0)
    start = llen + start;
  if (stop < 0)
    stop = llen + stop;
  if (start < 0)
    start = 0;

  if (start > stop || start >= llen) {
    append_to_output_buffer(ob, "*0\r\n", 4);
    return;
  }

  int forward = (start <= llen / 2) ? 1 : 0;

  if (stop >= llen)
    stop = llen - 1;
  long range_len = stop - start + 1;

  ListNode *node;

  if (forward)
    node = list_get_node_at(list, start, 0);
  else
    node = list_get_node_at(list, start, 1);

  char header[64];
  int header_len = snprintf(header, sizeof(header), "*%ld\r\n", range_len);
  append_to_output_buffer(ob, header, header_len);

  while (node && range_len > 0) {
    char *value = (char *)node->value;
    size_t val_len = strlen(value);

    char bulk_header[64];
    int bh_len =
        snprintf(bulk_header, sizeof(bulk_header), "$%zu\r\n", val_len);

    append_to_output_buffer(ob, bulk_header, bh_len);
    append_to_output_buffer(ob, value, val_len);
    append_to_output_buffer(ob, "\r\n", 2);
    node = forward ? node->next : node->prev;
    range_len--;
  }

  return;
}

void lmove_command(Client *client, HashTable *db, HashTable *expires,
                   OutputBuffer *ob) {
  char **arg_values = client->arg_values;
  int arg_count = client->arg_count;

  if (arg_count != 5) {
    append_to_output_buffer(ob, "-ERR args\r\n", 11);
    return;
  }

  int flags = 0;

  char *src_option = arg_values[3];
  if (strcasecmp(src_option, "LEFT") == 0)
    flags |= LMOVE_SRC_LEFT;
  else if (strcasecmp(src_option, "RIGHT") == 0)
    flags |= LMOVE_SRC_RIGHT;
  else {
    append_to_output_buffer(ob, "-ERR syntax error\r\n", 19);
    return;
  }

  char *dest_option = arg_values[4];
  if (strcasecmp(dest_option, "LEFT") == 0)
    flags |= LMOVE_DEST_LEFT;
  else if (strcasecmp(dest_option, "RIGHT") == 0)
    flags |= LMOVE_DEST_RIGHT;
  else {
    append_to_output_buffer(ob, "-ERR syntax error\r\n", 19);
    return;
  }

  r_obj *src_o = hash_table_get(db, arg_values[1]);
  r_obj *dest_o = hash_table_get(db, arg_values[2]);

  if (!src_o) {
    append_to_output_buffer(ob, "$-1\r\n", 5);
    return;
  }

  if (src_o->type != LIST) {
    char *msg = "-WRONGTYPE Operation against a key holding "
                "the wrong kind of value\r\n";
    append_to_output_buffer(ob, msg, strlen(msg));
    return;
  }

  if (!dest_o) {
    dest_o = create_list_object();
    hash_table_set(db, arg_values[2], dest_o);
  } else if (dest_o != NULL && dest_o->type != LIST) {
    char *msg = "-WRONGTYPE Operation against a key holding "
                "the wrong kind of value\r\n";
    append_to_output_buffer(ob, msg, strlen(msg));
    return;
  }

  List *src_list = (List *)src_o->data;
  List *dest_list = (List *)dest_o->data;

  char *value;

  if (flags & LMOVE_SRC_LEFT)
    value = (char *)list_pop_head(src_list);
  else if (flags & LMOVE_SRC_RIGHT)
    value = (char *)list_pop_tail(src_list);

  if (!value) {
    append_to_output_buffer(ob, "$-1\r\n", 5);
    return;
  }

  if (flags & LMOVE_DEST_LEFT)
    list_ins_node_head(dest_list, (const char *)value);
  else if (flags & LMOVE_DEST_RIGHT)
    list_ins_node_tail(dest_list, (const char *)value);

  unsigned long val_len = strlen(value);
  char bulk_header[64];
  int bh_len = snprintf(bulk_header, sizeof(bulk_header), "$%ld\r\n", val_len);
  append_to_output_buffer(ob, bulk_header, bh_len);
  append_to_output_buffer(ob, value, val_len);
  append_to_output_buffer(ob, "\r\n", 2);

  return;
}

void rpush_command(Client *client, HashTable *db, HashTable *expires,
                   OutputBuffer *ob) {

  char **arg_values = client->arg_values;
  int arg_count = client->arg_count;

  if (arg_count < 3) {
    append_to_output_buffer(ob, "-ERR args\r\n", 11);
    return;
  }

  r_obj *o = hash_table_get(db, arg_values[1]);
  if (o != NULL && o->type == LIST) {
    char *msg = "-WRONGTYPE Operation against a key holding "
                "the wrong kind of value\r\n";
    append_to_output_buffer(ob, msg, strlen(msg));
    return;
  }

  if (!o) {
    o = create_list_object();
    hash_table_set(db, arg_values[1], o);
  }

  int j;
  List *list = (List *)o->data;

  for (j = 2; j < arg_count; j++) {
    list_ins_node_tail(list, strdup(arg_values[j]));
  }

  char resp[64];
  int resp_len = snprintf(resp, sizeof(resp), ":%lu\r\n", list->size);
  append_to_output_buffer(ob, resp, resp_len);
  return;
}

void rpop_command(Client *client, HashTable *db, HashTable *expires,
                  OutputBuffer *ob) {
  char **arg_values = client->arg_values;
  int arg_count = client->arg_count;

  if (arg_count < 2) {
    append_to_output_buffer(ob, "-ERR args\r\n", 11);
    return;
  }

  r_obj *o = hash_table_get(db, arg_values[1]);

  if (!o) {
    append_to_output_buffer(ob, "_\r\n", 3);
    return;
  } else if (o->type != LIST) {
    char *msg = "-WRONGTYPE Operation against a key holding "
                "the wrong kind of value\r\n";
    append_to_output_buffer(ob, msg, strlen(msg));
    return;
  }

  List *list = (List *)o->data;

  if (arg_count == 3) {
    int count = atoi(arg_values[2]);

    if (count > list->size) {
      count = list->size;
    }

    char header[64];
    int header_len = snprintf(header, sizeof(header), "*%d\r\n", count);
    append_to_output_buffer(ob, header, header_len);

    for (int i = 0; i < count; i++) {
      char *value = (char *)list_pop_tail(list);
      unsigned long value_len = strlen(value);
      char bulk_header[64];
      int bh_len =
          snprintf(bulk_header, sizeof(bulk_header), "$%lu\r\n", value_len);
      append_to_output_buffer(ob, bulk_header, bh_len);
      append_to_output_buffer(ob, value, value_len);
      append_to_output_buffer(ob, "\r\n", 2);

      free(value);
    }
  } else {
    char *value = (char *)list_pop_tail(list);

    if (value) {
      unsigned long value_len = strlen(value);

      char bulk_header[64];
      int bh_len =
          snprintf(bulk_header, sizeof(bulk_header), "$%lu\r\n", value_len);

      append_to_output_buffer(ob, bulk_header, bh_len);
      append_to_output_buffer(ob, value, value_len);
      append_to_output_buffer(ob, "\r\n", 2);
    } else {
      append_to_output_buffer(ob, "$-1\r\n", 5);
      return;
    }
  }
  if (list->size == 0) {
    hash_table_del(db, arg_values[1]);
  }

  return;
}

void lpop_command(Client *client, HashTable *db, HashTable *expires,
                  OutputBuffer *ob) {
  char **arg_values = client->arg_values;
  int arg_count = client->arg_count;

  if (arg_count < 2) {
    append_to_output_buffer(ob, "-ERR args\r\n", 11);
    return;
  }

  r_obj *o = hash_table_get(db, arg_values[1]);

  if (!o) {
    append_to_output_buffer(ob, "_\r\n", 3);
    return;
  } else if (o->type != LIST) {
    char *msg = "-WRONGTYPE Operation against a key holding "
                "the wrong kind of value\r\n";
    append_to_output_buffer(ob, msg, strlen(msg));
    return;
  }

  List *list = (List *)o->data;

  if (arg_count == 3) {
    int count = atoi(arg_values[2]);

    if (count > list->size) {
      count = list->size;
    }

    char header[64];
    int header_len = snprintf(header, sizeof(header), "*%d\r\n", count);
    append_to_output_buffer(ob, header, header_len);

    for (int i = 0; i < count; i++) {
      char *value = (char *)list_pop_head(list);
      unsigned long value_len = strlen(value);
      char bulk_header[64];
      int bh_len =
          snprintf(bulk_header, sizeof(bulk_header), "$%lu\r\n", value_len);
      append_to_output_buffer(ob, bulk_header, bh_len);
      append_to_output_buffer(ob, value, value_len);
      append_to_output_buffer(ob, "\r\n", 2);

      free(value);
    }
  } else {
    char *value = (char *)list_pop_head(list);

    if (value) {
      unsigned long value_len = strlen(value);

      char bulk_header[64];
      int bh_len =
          snprintf(bulk_header, sizeof(bulk_header), "$%lu\r\n", value_len);

      append_to_output_buffer(ob, bulk_header, bh_len);
      append_to_output_buffer(ob, value, value_len);
      append_to_output_buffer(ob, "\r\n", 2);
    } else {
      append_to_output_buffer(ob, "$-1\r\n", 5);
      return;
    }
  }
  if (list->size == 0) {
    hash_table_del(db, arg_values[1]);
  }

  return;
}

void sadd_command(Client *client, HashTable *db, HashTable *expires,
                  OutputBuffer *ob) {
  char **arg_values = client->arg_values;
  int arg_count = client->arg_count;

  if (arg_count < 3) {
    append_to_output_buffer(ob, "-ERR args\r\n", 11);
    return;
  }
  r_obj *o = hash_table_get(db, arg_values[1]);

  if (o != NULL && o->type != SET) {
    char *msg = "-WRONGTYPE Operation against a key holding "
                "the wrong kind of value\r\n";
    append_to_output_buffer(ob, msg, strlen(msg));
    return;
  }

  if (o == NULL) {
    o = create_set_object();
    hash_table_set(db, arg_values[1], o);
  }

  int j;
  int count = 0;

  Set *set = (Set *)o->data;

  for (j = 2; j < arg_count; j++) {
    char *member = strdup(arg_values[j]);
    if (set_add(set, member) == 1) {
      count++;
    } else {
      free(member);
    }
  }

  char resp[64];
  int resp_len = snprintf(resp, sizeof(resp), ":%d\r\n", count);
  append_to_output_buffer(ob, resp, resp_len);
  return;
}

void sismember_command(Client *client, HashTable *db, HashTable *expires,
                       OutputBuffer *ob) {
  char **arg_values = client->arg_values;
  int arg_count = client->arg_count;

  if (arg_count < 3) {
    append_to_output_buffer(ob, "-ERR args\r\n", 11);
    return;
  }

  r_obj *o = hash_table_get(db, arg_values[1]);

  if (!o) {
    append_to_output_buffer(ob, ":0\r\n", 4);
    return;
  }

  if (o != NULL && o->type != SET) {
    char *msg = "-WRONGTYPE Operation against a key holding "
                "the wrong kind of value\r\n";
    append_to_output_buffer(ob, msg, strlen(msg));
    return;
  }

  int is_member = set_is_member((Set *)o->data, arg_values[2]);
  char resp[64];
  int resp_len = snprintf(resp, sizeof(resp), ":%d\r\n", is_member);
  append_to_output_buffer(ob, resp, resp_len);
  return;
}

void zadd_command(Client *client, HashTable *db, HashTable *expires,
                  OutputBuffer *ob) {

  char **arg_values = client->arg_values;
  int arg_count = client->arg_count;

  if (arg_count < 4) {
    append_to_output_buffer(ob, "-ERR args\r\n", 11);
    return;
  }

  int flags = ZADD_SET_NO_FLAGS;
  int flags_count = 0;

  int j;
  for (j = 2; j < arg_count; j++) {
    char *option = arg_values[j];

    if (strcasecmp(option, "NX") == 0) {
      flags |= ZADD_SET_NX;
      flags_count++;
    } else if (strcasecmp(option, "XX") == 0) {
      flags |= ZADD_SET_XX;
      flags_count++;
    } else if (strcasecmp(option, "GT") == 0) {
      flags |= ZADD_SET_GT;
      flags_count++;
    } else if (strcasecmp(option, "LT") == 0) {
      flags |= ZADD_SET_LT;
      flags_count++;
    } else if (strcasecmp(option, "CH") == 0) {
      flags |= ZADD_SET_CH;
      flags_count++;
    } else if (strcasecmp(option, "INCR") == 0) {
      flags |= ZADD_SET_INCR;
      flags_count++;
    } else
      break;
  }

  int nx = flags & ZADD_SET_NX;
  int xx = flags & ZADD_SET_XX;
  int gt = flags & ZADD_SET_GT;
  int lt = flags & ZADD_SET_LT;

  if ((nx && xx) || (gt && lt) || (gt && nx) || (lt && nx)) {
    append_to_output_buffer(ob, "-ERR syntax error\r\n", 19);
    return;
  }

  int remaining_args = arg_count - j;
  if (remaining_args == 0 || remaining_args % 2 != 0) {
    append_to_output_buffer(ob, "-ERR syntax error\r\n", 19);
    return;
  }

  if ((flags & ZADD_SET_INCR) && remaining_args != 2) {
    char *err = "-ERR INCR option supports a single "
                "increment-element pair\r\n";
    append_to_output_buffer(ob, err, strlen(err));
    return;
  }

  r_obj *o = hash_table_get(db, arg_values[1]);

  if (o != NULL && o->type != ZSET) {
    char *msg = "-WRONGTYPE Operation against a key holding "
                "the wrong kind of value\r\n";
    append_to_output_buffer(ob, msg, strlen(msg));
    return;
  }

  if (!o) {
    if (xx) {
      if (flags & ZADD_SET_INCR) {
        append_to_output_buffer(ob, "$-1\r\n", 5);
      } else {
        append_to_output_buffer(ob, ":0\r\n", 4);
      }
      return;
    }

    o = create_zset_object();
    hash_table_set(db, arg_values[1], o);
  }

  ZSet *zs = (ZSet *)o->data;

  int added = 0;
  int changed = 0;
  int processed = 0;

  for (int i = j; i < arg_count; i++) {
    double score = atof(arg_values[i]);
    char *member = arg_values[++i];

    if (flags & ZADD_SET_INCR) {
      r_obj *score_o = hash_table_get(zs->dict, member);
      double current_score = 0.0;

      if (score_o != NULL) {
        current_score = *(double *)o->data;
      } else {
        if (xx) {
          append_to_output_buffer(ob, "_\r\n", 3);
          return;
        }
      }

      double new_score = current_score + score;

      zset_add(zs, member, new_score);
      char num_str[64];

      int len = snprintf(num_str, sizeof(num_str), "%.17g", new_score);

      char bulk_header[64];
      int bh_len = snprintf(bulk_header, sizeof(bulk_header), "$%d\r\n", len);

      append_to_output_buffer(ob, bulk_header, bh_len);
      append_to_output_buffer(ob, num_str, len);
      append_to_output_buffer(ob, "\r\n", 2);
      processed++;
      return;
    }

    r_obj *member_o = hash_table_get(zs->dict, member);

    if (nx && member_o != NULL)
      continue;
    if (xx && member_o == NULL)
      continue;

    if (member_o != NULL) {
      double current_score = *(double *)member_o->data;

      if (gt && score <= current_score)
        continue;
      if (lt && score >= current_score)
        continue;

      zset_add(zs, member, score);
      changed++;
    } else {
      zset_add(zs, member, score);
      added++;
    }
  }

  if (!(flags & ZADD_SET_INCR)) {
    int ret_val = added;
    if (flags & ZADD_SET_CH) {
      ret_val = added + changed;
    }

    char resp[64];
    int resp_len = snprintf(resp, sizeof(resp), ":%d\r\n", ret_val);
    append_to_output_buffer(ob, resp, resp_len);
  }
  return;
}

void zrange_command(Client *client, HashTable *db, HashTable *expires,
                    OutputBuffer *ob) {
  char **arg_values = client->arg_values;
  int arg_count = client->arg_count;

  if (arg_count < 4) {
    append_to_output_buffer(ob, "-ERR args\r\n", 11);
    return;
  }

  int flags = ZRANGE_SET_NO_FLAGS;
  int limit_offset = 0;
  int limit_count = -1; // -1 implies infinite
  int with_scores = 0;
  int reverse = 0;

  int j;
  for (j = 4; j < arg_count; j++) {
    if (!strcasecmp(arg_values[j], "BYSCORE"))
      flags |= ZRANGE_SET_BYSCORE;
    else if (!strcasecmp(arg_values[j], "BYLEX"))
      flags |= ZRANGE_SET_BYLEX;
    else if (!strcasecmp(arg_values[j], "REV")) {
      flags |= ZRANGE_SET_REV;
      reverse = 1;
    } else if (!strcasecmp(arg_values[j], "WITHSCORES"))
      with_scores = 1;
    else if (!strcasecmp(arg_values[j], "LIMIT")) {
      if ((j + 2) >= arg_count) {
        append_to_output_buffer(ob, "-ERR syntax error\r\n", 19);
        return;
      }
      flags |= ZRANGE_SET_LIMIT;
      limit_offset = atoi(arg_values[j + 1]);
      limit_count = atoi(arg_values[j + 2]);
      j += 2;
    } else {
      append_to_output_buffer(ob, "-ERR syntax error\r\n", 19);
      return;
    }
  }

  if ((flags & ZRANGE_SET_BYLEX) && (flags & ZRANGE_SET_BYSCORE)) {
    append_to_output_buffer(ob, "-ERR syntax error\r\n", 19);
    return;
  }
  if ((flags & ZRANGE_SET_BYLEX) && with_scores) {
    char *msg = "-ERR syntax error, WITHSCORES not supported with BYLEX\r\n";
    append_to_output_buffer(ob, msg, strlen(msg));
    return;
  }
  if ((flags & ZRANGE_SET_LIMIT) && !(flags & ZRANGE_SET_BYSCORE) &&
      !(flags & ZRANGE_SET_BYLEX)) {
    char *msg =
        "-ERR syntax error, LIMIT only supported with BYSCORE or BYLEX\r\n";
    append_to_output_buffer(ob, msg, strlen(msg));
    return;
  }

  r_obj *o = hash_table_get(db, arg_values[1]);

  if (!o) {
    append_to_output_buffer(ob, "*0\r\n", 4);
    return;
  }

  if (o->type != ZSET) {
    char *err = "-WRONGTYPE Operation against a key holding the wrong kind of "
                "value\r\n";
    append_to_output_buffer(ob, err, strlen(err));
    return;
  }

  ZSet *zs = (ZSet *)o->data;
  ZSkipList *zsl = zs->zsl;
  ZSkipListNode *node;
  int count = 0;

  if (flags & ZRANGE_SET_BYSCORE) {
    double start = atof(arg_values[2]);
    double stop = atof(arg_values[3]);

    if (reverse)
      node = zsl_last_in_range(zsl, stop);
    else
      node = zsl_first_in_range(zsl, start);

    while (node && limit_offset) {
      if (reverse ? (node->score < start) : (node->score > stop)) {
        node = NULL;
        break;
      }

      node = zsl_next_node(node, reverse);
      limit_offset--;
    }

    while (node && (limit_count != 0)) {
      if (reverse ? (node->score < start) : (node->score > stop)) {
        break;
      }

      zrange_emit_node(ob, node, with_scores);
      if (with_scores)
        count++;
      count++;

      node = zsl_next_node(node, reverse);
      if (limit_count > 0)
        limit_count--;
    }
  } else if (flags & ZRANGE_SET_BYLEX) {
    int min_inclusive = (arg_values[2][0] == '[');
    char *min_str = arg_values[2] + 1;

    int max_inclusive = (arg_values[3][0] == '[');
    char *max_str = arg_values[3] + 1;

    if (reverse)
      node = zsl_last_in_lex_range(zsl, max_str, max_inclusive);
    else
      node = zsl_first_in_lex_range(zsl, min_str, min_inclusive);

    while (node && limit_offset > 0) {
      node = zsl_next_node(node, reverse);
      limit_offset--;
    }

    while (node && (limit_count != 0)) {
      if (reverse) {
        int cmp = strcmp(node->element, min_str);
        if (min_inclusive ? cmp < 0 : cmp <= 0)
          break;
      } else {
        int cmp = strcmp(node->element, max_str);
        if (max_inclusive ? cmp > 0 : cmp >= 0)
          break;
      }

      zrange_emit_node(ob, node, 0);
      count++;

      node = zsl_next_node(node, reverse);
      if (limit_count > 0)
        limit_count--;
    }
  } else {
    long start = atoi(arg_values[2]);
    long stop = atoi(arg_values[3]);
    long llen = zsl->length;

    if (start < 0)
      start = llen + start;
    if (stop < 0)
      stop = llen + stop;
    if (start < 0)
      start = 0;

    if (start > stop || start >= llen) {
      append_to_output_buffer(ob, "*0\r\n", 4);
      return;
    }

    if (stop >= llen)
      stop = llen - 1;
    long range_len = stop - start + 1;

    if (reverse)
      node = zsl_get_element_by_rank(zsl, stop);
    else
      node = zsl_get_element_by_rank(zsl, start);

    while (node && range_len > 0) {
      zrange_emit_node(ob, node, with_scores);
      if (with_scores)
        count++;
      count++;

      node = zsl_next_node(node, reverse);
      range_len--;
    }
  }

  char header[64];
  int header_len = snprintf(header, sizeof(header), "*%d\r\n", count);
  write(ob->fd, header, header_len);
}

void zscore_command(Client *client, HashTable *db, HashTable *expires,
                    OutputBuffer *ob) {
  char **arg_values = client->arg_values;
  int arg_count = client->arg_count;

  if (arg_count < 3) {
    append_to_output_buffer(ob, "-ERR args\r\n", 11);
    return;
  }

  r_obj *o = hash_table_get(db, arg_values[1]);
  if (o == NULL) {
    append_to_output_buffer(ob, "$-1\r\n", 5);
    return;
  } else if (o->type != ZSET) {
    char *err = "-WRONGTYPE Operation against a key holding the "
                "wrong kind of value\r\n";
    append_to_output_buffer(ob, err, strlen(err));
    return;
  }

  ZSet *zs = (ZSet *)o->data;
  r_obj *score_o = hash_table_get(zs->dict, arg_values[2]);
  if (score_o == NULL) {
    append_to_output_buffer(ob, "$-1\r\n", 5);
    return;
  }

  double score = *(double *)score_o->data;

  char resp[128];
  int resp_len = snprintf(resp, sizeof(resp), ",%.17g\r\n", score);
  append_to_output_buffer(ob, resp, resp_len);
  return;
}

void zrank_command(Client *client, HashTable *db, HashTable *expires,
                   OutputBuffer *ob) {
  char **arg_values = client->arg_values;
  int arg_count = client->arg_count;

  if (arg_count < 3) {
    append_to_output_buffer(ob, "-ERR args\r\n", 11);
    return;
  }

  r_obj *o = hash_table_get(db, arg_values[1]);
  if (!o) {
    append_to_output_buffer(ob, "$-1\r\n", 5);
    return;
  } else if (o->type != ZSET) {
    char *err = "-WRONGTYPE Operation against a key holding the "
                "wrong kind of value\r\n";
    append_to_output_buffer(ob, err, strlen(err));
    return;
  }

  ZSet *zs = (ZSet *)o->data;

  r_obj *score_o = hash_table_get(zs->dict, arg_values[2]);
  if (score_o == NULL) {
    append_to_output_buffer(ob, "$-1\r\n", 5);
    return;
  }

  double score = *(double *)score_o->data;
  unsigned long rank = zsl_get_rank(zs->zsl, score, arg_values[2]);
  if (rank > 0) {
    rank--;
    char resp[64];
    int resp_len = snprintf(resp, sizeof(resp), ":%lu\r\n", rank);
    append_to_output_buffer(ob, resp, resp_len);
  } else {
    append_to_output_buffer(ob, "$-1\r\n", 5);
  }

  return;
}

void save_command(Client *client, HashTable *db, HashTable *expires,
                  OutputBuffer *ob) {
  rdb_save(db, expires, "dump.rdb");
  append_to_output_buffer(ob, "+OK\r\n", 5);
  return;
}

void ping_command(Client *client, HashTable *db, HashTable *expires,
                  OutputBuffer *ob) {
  append_to_output_buffer(ob, "+PONG\r\n", 7);
  return;
}
