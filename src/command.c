#include "../include/command.h"
#include "../include/list.h"
#include "../include/persistance.h"
#include "../include/recis.h"
#include "../include/set.h"
#include "../include/zset.h"
#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <time.h>
#include <unistd.h>

Command CommandTable[] = {
    {"SET", set_command, -3},           {"GET", get_command, 2},
    {"DEL", del_command, -2},           {"TTL", ttl_command, 2},
    {"INCR", incr_command, 2},          {"INCRBY", incrby_command, 3},
    {"LPUSH", lpush_command, -3},       {"RPUSH", rpush_command, -3},
    {"RPOP", rpop_command, -2},         {"LPOP", lpop_command, -2},
    {"LLEN", llen_command, 2},          {"LINDEX", lindex_command, 3},
    {"LRANGE", lrange_command, 4},      {"LMOVE", lmove_command, 5},
    {"LTRIM", ltrim_command, 4},        {"SADD", sadd_command, -3},
    {"SREM", srem_command, -3},         {"SCARD", scard_command, 2},
    {"SINTER", sinter_command, -2},     {"SISMEMBER", sismember_command, 3},
    {"SMEMEBERS", smembers_command, 2}, {"HSET", hset_command, -4},
    {"HGET", hget_command, 3},          {"HMGET", hmget_command, -3},
    {"HINCRBY", hincrby_command, 4},    {"ZADD", zadd_command, -4},
    {"ZREM", zrem_command, -3}, {"ZCARD", zcard_command, 2},
    {"ZRANGE", zrange_command, -4},     {"ZSCORE", zscore_command, 3},
    {"ZRANK", zrank_command, 3},        {"SAVE", save_command, 1},
    {"PING", ping_command, 1},          {NULL, NULL, 0}};

r_obj *create_command_object(Command *cmd) {
  r_obj *o;
  if ((o = (r_obj *)malloc(sizeof(r_obj))) == NULL)
    return NULL;
  o->type = COMMAND;
  o->data = cmd;
  return o;
}

Command *command_lookup(char *name, int len) {
  for (int i = 0; CommandTable[i].name != NULL; i++) {
    char *cmd_name = CommandTable[i].name;

    if (strlen(cmd_name) != len)
      continue;

    if (strncasecmp(name, cmd_name, len) == 0) {
      return &CommandTable[i];
    }
  }

  return NULL;
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

int try_parse_int64(const char *s, int64_t *out) {
  if (*s == '\0')
    return 0;

  char *end;
  errno = 0;
  long long val = strtoll(s, &end, 10);

  if (errno != 0)
    return 0;
  if (*end != '\0')
    return 0;

  *out = val;
  return 1;
}

long long get_time_ms() { return (long long)time(NULL) * 1000; }

void set_command(Client *client, HashTable *db, HashTable *expires,
                 OutputBuffer *ob) {
  Bytes **arg_values = client->arg_values;
  int arg_count = client->arg_count;

  if (arg_count < 3) {
    append_to_output_buffer(ob, "-ERR args\r\n", 11);
    return;
  }

  int flags = OBJ_SET_NO_FLAGS;
  long long expire_at = -1;

  Bytes *ifeq_val = NULL;
  Bytes *ifne_val = NULL;

  int j;
  for (j = 3; j < arg_count; j++) {
    Bytes *option_o = arg_values[j];
    char *option = option_o->data;

    if (strcasecmp(option, "NX") == 0) {
      flags |= OBJ_SET_NX;
    } else if (strcasecmp(option, "XX") == 0) {
      flags |= OBJ_SET_XX;
    } else if (strcasecmp(option, "GET") == 0) {
      flags |= OBJ_SET_GET;
    } else if (strcasecmp(option, "KEEPTTL") == 0) {
      flags |= OBJ_SET_KEEPTTL;
    } else if (strcasecmp(option, "EX") == 0 && j + 1 < arg_count) {
      long long value = atoll(arg_values[++j]->data);
      if (value <= 0) {
        append_to_output_buffer(ob, "-ERR invalid expire time\r\n", 26);
        return;
      }
      expire_at = get_time_ms() + (value * 1000);
    } else if (strcasecmp(option, "PX") == 0 && j + 1 < arg_count) {
      long long value = atoll(arg_values[++j]->data);
      if (value <= 0) {
        append_to_output_buffer(ob, "-ERR invalid expire time\r\n", 26);
        return;
      }
      expire_at = get_time_ms() + value;
    } else if (strcasecmp(option, "EXAT") == 0 && j + 1 < arg_count) {
      long long value = atoll(arg_values[++j]->data);
      if (value <= 0) {
        append_to_output_buffer(ob, "-ERR invalid expire time\r\n", 26);
        return;
      }
      expire_at = value * 1000;
    } else if (strcasecmp(option, "PXAT") == 0 && j + 1 < arg_count) {
      long long value = atoll(arg_values[++j]->data);
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

  if ((nx && xx) || (ifeq && ifne)) {
    append_to_output_buffer(ob, "-ERR syntax error\r\n", 19);
    return;
  }

  r_obj *o = hash_table_get(db, arg_values[1]);

  if (nx && o != NULL) {
    append_to_output_buffer(ob, "_\r\n", 3);
    return;
  }

  if (xx && o == NULL) {
    append_to_output_buffer(ob, "_\r\n", 3);
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

    if (bytes_equal((Bytes *)o->data, ifeq_val) == 0) {
      append_to_output_buffer(ob, "_\r\n", 3);
      return;
    }
  }

  if (ifne) {
    if (o != NULL) {
      if (o->type == STRING && bytes_equal((Bytes *)o->data, ifne_val) != 0) {
        append_to_output_buffer(ob, "_\r\n", 3);
        return;
      }
    }
  }

  if ((flags & OBJ_SET_KEEPTTL) && expire_at != -1) {
    append_to_output_buffer(ob, "-ERR syntax error\r\n", 19);
    return;
  }

  if (flags & OBJ_SET_GET) {
    if (o == NULL) {
      append_to_output_buffer(ob, "_\r\n", 3);
    } else if (o->type != STRING) {
      append_to_output_buffer(ob,
                              "-WRONGTYPE Operation against a key holding the "
                              "wrong kind of value\r\n",
                              68);
      return;
    } else {
      Bytes *old = (Bytes *)o->data;
      char header[64];
      int head_len =
          snprintf(header, sizeof(header), "$%" PRIu32 "\r\n", old->length);

      append_to_output_buffer(ob, header, head_len);
      append_to_output_buffer(ob, old->data, old->length);
      append_to_output_buffer(ob, "\r\n", 2);
    }
  }

  if ((flags & OBJ_SET_KEEPTTL) && o != NULL && expire_at == -1) {
    r_obj *ttl = hash_table_get(expires, arg_values[1]);
    if (ttl != NULL) {
      expire_at = *(long long *)ttl->data;
    }
  }

  Bytes *val = arg_values[2];

  r_obj *new_obj = create_string_object(val->data, val->length);
  hash_table_set(db, arg_values[1], new_obj);

  if (expire_at != -1) {
    hash_table_set(expires, arg_values[1], create_int_object(expire_at));
  } else {
    hash_table_del(expires, arg_values[1]);
  }

  if (!(flags & OBJ_SET_GET)) {
    append_to_output_buffer(ob, "+OK\r\n", 5);
  }
  return;
}

void get_command(Client *client, HashTable *db, HashTable *expires,
                 OutputBuffer *ob) {
  Bytes **arg_values = client->arg_values;
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

      /* append_to_output_buffer(ob, "_\r\n", 3); */
      append_to_output_buffer(ob, "$-1\r\n", 5);
      return;
    }
  }

  r_obj *o = hash_table_get(db, arg_values[1]);

  if (!o) {
    /* append_to_output_buffer(ob, "_\r\n", 3); */
    append_to_output_buffer(ob, "$-1\r\n", 5);
    return;
  }

  if (o->type != STRING) {
    append_to_output_buffer(ob,
                            "-WRONGTYPE Operation against a key holding the "
                            "wrong kind of value\r\n",
                            68);
    return;
  }

  Bytes *value = (Bytes *)o->data;

  char header[64];
  int head_len =
      snprintf(header, sizeof(header), "$%" PRIu32 "\r\n", value->length);
  append_to_output_buffer(ob, header, head_len);
  append_to_output_buffer(ob, value->data, value->length);
  append_to_output_buffer(ob, "\r\n", 2);

  return;
}

void del_command(Client *client, HashTable *db, HashTable *expires,
                 OutputBuffer *ob) {
  Bytes **arg_values = client->arg_values;
  int arg_count = client->arg_count;

  if (arg_count < 2) {
    append_to_output_buffer(ob, "-ERR args\r\n", 11);
    return;
  }

  int deleted_count = 0;

  for (int i = 1; i < arg_count; i++) {
    Bytes *key = arg_values[i];
    if (hash_table_del(db, key) == 1) {
      hash_table_del(expires, key);
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
  Bytes **arg_values = client->arg_values;
  int arg_count = client->arg_count;

  if (arg_count < 2) {
    append_to_output_buffer(ob, "-ERR args\r\n", 11);
    return;
  }

  r_obj *o = hash_table_get(db, arg_values[1]);
  if (o == NULL) {
    append_to_output_buffer(ob, ":-2\r\n", 5);
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

void incr_command(Client *client, HashTable *db, HashTable *expires,
                  OutputBuffer *ob) {
  Bytes **arg_values = client->arg_values;
  int arg_count = client->arg_count;

  if (arg_count != 2) {
    append_to_output_buffer(ob, "-ERR args\r\n", 11);
    return;
  }

  r_obj *o = hash_table_get(db, arg_values[1]);
  int64_t value = 0;

  if (o != NULL) {
    if (o->type != STRING) {
      append_to_output_buffer(ob,
                              "-WRONGTYPE Operation against a key holding the "
                              "wrong kind of value\r\n",
                              68);
      return;
    }

    Bytes *o_bytes = (Bytes *)o->data;
    if (try_parse_int64(o_bytes->data, &value) == 0) {
      append_to_output_buffer(
          ob, "-value is not an integer or out of range\r\n", 42);
      return;
    }
  }

  if (value == INT64_MAX) {
    append_to_output_buffer(
        ob, "-ERR increment or decrement would overflow\r\n", 44);
    return;
  }

  value++;

  char num_str[64];
  int num_len = snprintf(num_str, sizeof(num_str), "%" PRId64, value);
  hash_table_set(db, client->arg_values[1],
                 create_string_object(num_str, num_len));
  char resp[64];
  int resp_len = snprintf(resp, sizeof(resp), ":%s\r\n", num_str);
  append_to_output_buffer(ob, resp, resp_len);
  return;
}

void incrby_command(Client *client, HashTable *db, HashTable *expires,
                    OutputBuffer *ob) {
  Bytes **arg_values = client->arg_values;
  int arg_count = client->arg_count;

  if (arg_count != 3) {
    append_to_output_buffer(ob, "-ERR args\r\n", 11);
    return;
  }

  int64_t increment = 0;
  if (try_parse_int64(arg_values[2]->data, &increment) == 0) {
    append_to_output_buffer(ob, "-value is not an integer or out of range\r\n",
                            42);
    return;
  }

  r_obj *o = hash_table_get(db, arg_values[1]);

  int64_t value = 0;

  if (o != NULL) {
    if (o->type != STRING) {
      append_to_output_buffer(ob,
                              "-WRONGTYPE Operation against a key holding the "
                              "wrong kind of value\r\n",
                              68);
      return;
    }

    Bytes *o_bytes = (Bytes *)o->data;
    if (try_parse_int64(o_bytes->data, &value) == 0) {
      append_to_output_buffer(
          ob, "-value is not an integer or out of range\r\n", 42);
      return;
    }
  }

  if ((increment > 0 && value > INT64_MAX - increment) ||
      (increment < 0 && value < INT64_MIN - increment)) {
    append_to_output_buffer(
        ob, "-ERR increment or decrement would overflow\r\n", 44);
    return;
  }

  value += increment;

  char num_str[64];
  int num_len = snprintf(num_str, sizeof(num_str), "%" PRId64, value);
  hash_table_set(db, arg_values[1], create_string_object(num_str, num_len));

  char resp[64];
  int resp_len = snprintf(resp, sizeof(resp), ":%s\r\n", num_str);
  append_to_output_buffer(ob, resp, resp_len);
  return;
}

void decr_command(Client *client, HashTable *db, HashTable *expires,
                  OutputBuffer *ob) {
  Bytes **arg_values = client->arg_values;
  int arg_count = client->arg_count;

  if (arg_count != 2) {
    append_to_output_buffer(ob, "-ERR args\r\n", 11);
    return;
  }

  r_obj *o = hash_table_get(db, arg_values[1]);
  int64_t value = 0;

  if (o != NULL) {
    if (o->type != STRING) {
      append_to_output_buffer(ob,
                              "-WRONGTYPE Operation against a key holding the "
                              "wrong kind of value\r\n",
                              68);
      return;
    }

    Bytes *o_bytes = (Bytes *)o->data;
    if (try_parse_int64(o_bytes->data, &value) == 0) {
      append_to_output_buffer(
          ob, "-value is not an integer or out of range\r\n", 42);
      return;
    }
  }

  if (value == INT64_MIN) {
    append_to_output_buffer(
        ob, "-ERR increment or decrement would overflow\r\n", 44);
    return;
  }

  value--;

  char num_str[64];
  int num_len = snprintf(num_str, sizeof(num_str), "%" PRId64, value);
  hash_table_set(db, client->arg_values[1],
                 create_string_object(num_str, num_len));
  char resp[64];
  int resp_len = snprintf(resp, sizeof(resp), ":%s\r\n", num_str);
  append_to_output_buffer(ob, resp, resp_len);
  return;
}

void decrby_command(Client *client, HashTable *db, HashTable *expires,
                    OutputBuffer *ob) {
  Bytes **arg_values = client->arg_values;
  int arg_count = client->arg_count;

  if (arg_count != 3) {
    append_to_output_buffer(ob, "-ERR args\r\n", 11);
    return;
  }

  int64_t decrement = 0;
  if (try_parse_int64(arg_values[2]->data, &decrement) == 0) {
    append_to_output_buffer(ob, "-value is not an integer or out of range\r\n",
                            42);
    return;
  }

  r_obj *o = hash_table_get(db, arg_values[1]);

  int64_t value = 0;

  if (o != NULL) {
    if (o->type != STRING) {
      append_to_output_buffer(ob,
                              "-WRONGTYPE Operation against a key holding the "
                              "wrong kind of value\r\n",
                              68);
      return;
    }

    Bytes *o_bytes = (Bytes *)o->data;
    if (try_parse_int64(o_bytes->data, &value) == 0) {
      append_to_output_buffer(
          ob, "-value is not an integer or out of range\r\n", 42);
      return;
    }
  }

  if ((decrement > 0 && value < INT64_MIN + decrement) ||
      (decrement < 0 && value > INT64_MAX + decrement)) {
    append_to_output_buffer(
        ob, "-ERR increment or decrement would overflow\r\n", 44);
    return;
  }

  value -= decrement;

  char num_str[64];
  int num_len = snprintf(num_str, sizeof(num_str), "%" PRId64, value);
  hash_table_set(db, arg_values[1], create_string_object(num_str, num_len));

  char resp[64];
  int resp_len = snprintf(resp, sizeof(resp), ":%s\r\n", num_str);
  append_to_output_buffer(ob, resp, resp_len);
  return;
}

void lpush_command(Client *client, HashTable *db, HashTable *expires,
                   OutputBuffer *ob) {

  Bytes **arg_values = client->arg_values;
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
    list_ins_node_head(
        list, create_string_object(arg_values[j]->data, arg_values[j]->length));
  }

  char resp[64];
  int resp_len = snprintf(resp, sizeof(resp), ":%zu\r\n", list->size);
  append_to_output_buffer(ob, resp, resp_len);
  return;
}

void llen_command(Client *client, HashTable *db, HashTable *expires,
                  OutputBuffer *ob) {
  Bytes **arg_values = client->arg_values;
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
  int resp_len = snprintf(resp, sizeof(resp), ":%zu\r\n", list->size);
  append_to_output_buffer(ob, resp, resp_len);
  return;
}

void lindex_command(Client *client, HashTable *db, HashTable *expires,
                    OutputBuffer *ob) {
  Bytes **arg_values = client->arg_values;
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
  int index = atoi(arg_values[2]->data);

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

  r_obj *member_o = (r_obj *)member->value;
  Bytes *member_bytes = (Bytes *)member_o->data;

  char *value = member_bytes->data;
  uint32_t val_len = member_bytes->length;

  char header[64];
  int head_len = snprintf(header, sizeof(header), "$%" PRIu32 "\r\n", val_len);
  append_to_output_buffer(ob, header, head_len);
  append_to_output_buffer(ob, value, val_len);
  append_to_output_buffer(ob, "\r\n", 2);
  return;
}

void lrange_command(Client *client, HashTable *db, HashTable *expires,
                    OutputBuffer *ob) {
  Bytes **arg_values = client->arg_values;
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

  int64_t start;
  int64_t stop;

  if (try_parse_int64(arg_values[2]->data, &start) != 1 ||
      try_parse_int64(arg_values[3]->data, &stop) != 1) {
    append_to_output_buffer(ob, "-value is not an integer or out of range\r\n",
                            42);
    return;
  }

  size_t llen = list->size;

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
  size_t range_len = stop - start + 1;

  ListNode *node;

  if (forward)
    node = list_get_node_at(list, start, 0);
  else
    node = list_get_node_at(list, start, 1);

  char header[64];
  int header_len = snprintf(header, sizeof(header), "*%ld\r\n", range_len);
  append_to_output_buffer(ob, header, header_len);

  while (node && range_len > 0) {
    r_obj *member_o = (r_obj *)node->value;
    Bytes *member_bytes = (Bytes *)member_o->data;

    char *value = member_bytes->data;
    uint32_t val_len = member_bytes->length;

    char bulk_header[64];
    int bh_len =
        snprintf(bulk_header, sizeof(bulk_header), "$%" PRIu32 "\r\n", val_len);

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
  Bytes **arg_values = client->arg_values;
  int arg_count = client->arg_count;

  if (arg_count != 5) {
    append_to_output_buffer(ob, "-ERR args\r\n", 11);
    return;
  }

  int flags = 0;

  Bytes *src = arg_values[3];
  char *src_option = src->data;

  if (strcasecmp(src_option, "LEFT") == 0)
    flags |= LMOVE_SRC_LEFT;
  else if (strcasecmp(src_option, "RIGHT") == 0)
    flags |= LMOVE_SRC_RIGHT;
  else {
    append_to_output_buffer(ob, "-ERR syntax error\r\n", 19);
    return;
  }

  Bytes *dest = arg_values[4];
  char *dest_option = dest->data;

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

  r_obj *value;

  if (flags & LMOVE_SRC_LEFT)
    value = list_pop_head(src_list);
  else if (flags & LMOVE_SRC_RIGHT)
    value = list_pop_tail(src_list);

  if (!value) {
    append_to_output_buffer(ob, "$-1\r\n", 5);
    return;
  }

  if (flags & LMOVE_DEST_LEFT)
    list_ins_node_head(dest_list, value);
  else if (flags & LMOVE_DEST_RIGHT)
    list_ins_node_tail(dest_list, value);

  if (src_list->size == 0) {
    hash_table_del(db, arg_values[1]);
    hash_table_del(expires, arg_values[1]);
  }

  Bytes *value_bytes = (Bytes *)value->data;

  uint32_t val_len = value_bytes->length;
  char bulk_header[64];
  int bh_len =
      snprintf(bulk_header, sizeof(bulk_header), "$%" PRIu32 "\r\n", val_len);
  append_to_output_buffer(ob, bulk_header, bh_len);
  append_to_output_buffer(ob, value_bytes->data, val_len);
  append_to_output_buffer(ob, "\r\n", 2);

  return;
}

void ltrim_command(Client *client, HashTable *db, HashTable *expires,
                   OutputBuffer *ob) {
  Bytes **arg_values = client->arg_values;
  int arg_count = client->arg_count;

  if (arg_count != 4) {
    append_to_output_buffer(ob, "-ERR args\r\n", 11);
    return;
  }

  r_obj *o = hash_table_get(db, arg_values[1]);

  if (!o) {
    append_to_output_buffer(ob, "+OK\r\n", 5);
    return;
  }

  if (o->type != LIST) {
    char *msg = "-WRONGTYPE Operation against a key holding "
                "the wrong kind of value\r\n";
    append_to_output_buffer(ob, msg, strlen(msg));
    return;
  }

  List *list = (List *)o->data;

  int64_t start;
  int64_t stop;

  if (try_parse_int64(arg_values[2]->data, &start) != 1 ||
      try_parse_int64(arg_values[3]->data, &stop) != 1) {
    append_to_output_buffer(ob, "-value is not an integer or out of range\r\n",
                            42);
    return;
  }

  size_t llen = list->size;

  if (start < 0)
    start = llen + start;
  if (stop < 0)
    stop = llen + stop;
  if (start < 0)
    start = 0;

  if (start > stop || start >= llen) {
    hash_table_del(db, arg_values[1]);
    append_to_output_buffer(ob, "+OK\r\n", 5);
    return;
  }

  if (stop >= llen)
    stop = llen - 1;

  for (int64_t i = 0; i < start; i++) {
    r_obj *value = list_pop_head(list);
    free_object(value);
  }

  stop = stop - start;

  while (list->size > stop + 1) {
    r_obj *value = list_pop_tail(list);
    free_object(value);
  }

  append_to_output_buffer(ob, "+OK\r\n", 5);
  return;
}

void rpush_command(Client *client, HashTable *db, HashTable *expires,
                   OutputBuffer *ob) {

  Bytes **arg_values = client->arg_values;
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
    list_ins_node_tail(
        list, create_string_object(arg_values[j]->data, arg_values[j]->length));
  }

  char resp[64];
  int resp_len = snprintf(resp, sizeof(resp), ":%zu\r\n", list->size);
  append_to_output_buffer(ob, resp, resp_len);
  return;
}

void rpop_command(Client *client, HashTable *db, HashTable *expires,
                  OutputBuffer *ob) {
  Bytes **arg_values = client->arg_values;
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
    int64_t count;
    if (try_parse_int64(arg_values[2]->data, &count) == 0) {
      append_to_output_buffer(
          ob, "-value is out of range, must be positive\r\n", 42);
      return;
    }

    if (count > list->size) {
      count = list->size;
    }

    char header[64];
    int header_len =
        snprintf(header, sizeof(header), "*%" PRId64 "\r\n", count);
    append_to_output_buffer(ob, header, header_len);

    for (int64_t i = 0; i < count; i++) {
      r_obj *value = list_pop_tail(list);
      Bytes *value_bytes = (Bytes *)value->data;
      uint32_t val_len = value_bytes->length;

      char bulk_header[64];
      int bh_len = snprintf(bulk_header, sizeof(bulk_header),
                            "$%" PRIu32 "\r\n", val_len);
      append_to_output_buffer(ob, bulk_header, bh_len);
      append_to_output_buffer(ob, value_bytes->data, val_len);
      append_to_output_buffer(ob, "\r\n", 2);

      free_object(value);
    }
  } else {
    r_obj *value = list_pop_tail(list);

    if (value) {
      Bytes *value_bytes = (Bytes *)value->data;
      uint32_t val_len = value_bytes->length;

      char bulk_header[64];
      int bh_len = snprintf(bulk_header, sizeof(bulk_header),
                            "$%" PRIu32 "\r\n", val_len);

      append_to_output_buffer(ob, bulk_header, bh_len);
      append_to_output_buffer(ob, value_bytes->data, val_len);
      append_to_output_buffer(ob, "\r\n", 2);

      free_object(value);
    } else {
      append_to_output_buffer(ob, "$-1\r\n", 5);
      return;
    }
  }
  if (list->size == 0) {
    hash_table_del(db, arg_values[1]);
    hash_table_del(expires, arg_values[1]);
  }

  return;
}

void lpop_command(Client *client, HashTable *db, HashTable *expires,
                  OutputBuffer *ob) {
  Bytes **arg_values = client->arg_values;
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
    int64_t count;
    if (try_parse_int64(arg_values[2]->data, &count) == 0) {
      append_to_output_buffer(
          ob, "-value is out of range, must be positive\r\n", 42);
      return;
    }

    if (count > list->size) {
      count = list->size;
    }

    char header[64];
    int header_len =
        snprintf(header, sizeof(header), "*%" PRId64 "\r\n", count);
    append_to_output_buffer(ob, header, header_len);

    for (int64_t i = 0; i < count; i++) {
      r_obj *value = list_pop_head(list);
      Bytes *value_bytes = (Bytes *)value->data;
      uint32_t val_len = value_bytes->length;

      char bulk_header[64];
      int bh_len = snprintf(bulk_header, sizeof(bulk_header),
                            "$%" PRIu32 "\r\n", val_len);
      append_to_output_buffer(ob, bulk_header, bh_len);
      append_to_output_buffer(ob, value_bytes->data, val_len);
      append_to_output_buffer(ob, "\r\n", 2);

      free_object(value);
    }
  } else {
    r_obj *value = list_pop_head(list);

    if (value) {
      Bytes *value_bytes = (Bytes *)value->data;
      uint32_t val_len = value_bytes->length;

      char bulk_header[64];
      int bh_len = snprintf(bulk_header, sizeof(bulk_header),
                            "$%" PRIu32 "\r\n", val_len);

      append_to_output_buffer(ob, bulk_header, bh_len);
      append_to_output_buffer(ob, value_bytes->data, val_len);
      append_to_output_buffer(ob, "\r\n", 2);

      free_object(value);
    } else {
      append_to_output_buffer(ob, "$-1\r\n", 5);
      return;
    }
  }
  if (list->size == 0) {
    hash_table_del(db, arg_values[1]);
    hash_table_del(expires, arg_values[1]);
  }

  return;
}

void scard_command(Client *client, HashTable *db, HashTable *expires,
                   OutputBuffer *ob) {
  Bytes **arg_values = client->arg_values;
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

  if (o->type != SET) {
    char *msg = "-WRONGTYPE Operation against a key holding "
                "the wrong kind of value\r\n";
    append_to_output_buffer(ob, msg, strlen(msg));
    return;
  }

  Set *set = (Set *)o->data;

  char resp[64];
  int resp_len = snprintf(resp, sizeof(resp), ":%zu\r\n", set->count);
  append_to_output_buffer(ob, resp, resp_len);
  return;
}

void sadd_command(Client *client, HashTable *db, HashTable *expires,
                  OutputBuffer *ob) {
  Bytes **arg_values = client->arg_values;
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
    if (set_add(set, arg_values[j]) == 1) {
      count++;
    }
  }

  char resp[64];
  int resp_len = snprintf(resp, sizeof(resp), ":%d\r\n", count);
  append_to_output_buffer(ob, resp, resp_len);
  return;
}

void srem_command(Client *client, HashTable *db, HashTable *expires,
                  OutputBuffer *ob) {
  Bytes **arg_values = client->arg_values;
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

  if (o->type != SET) {
    char *msg = "-WRONGTYPE Operation against a key holding "
                "the wrong kind of value\r\n";
    append_to_output_buffer(ob, msg, strlen(msg));
    return;
  }

  Set *set = (Set *)o->data;

  int j;
  int count = 0;
  for (j = 2; j < arg_count; j++) {
    if (set_rem(set, arg_values[j]) == 1)
      count++;
  }

  if (set->count == 0) {
    hash_table_del(db, arg_values[1]);
    hash_table_del(expires, arg_values[1]);
  }

  char resp[64];
  int resp_len = snprintf(resp, sizeof(resp), ":%d\r\n", count);
  append_to_output_buffer(ob, resp, resp_len);
  return;
}

void sinter_command(Client *client, HashTable *db, HashTable *expires,
                    OutputBuffer *ob) {
  Bytes **arg_values = client->arg_values;
  int arg_count = client->arg_count;

  if (arg_count < 2) {
    append_to_output_buffer(ob, "-ERR args\r\n", 11);
    return;
  }

  Set **sets;
  if ((sets = (Set **)malloc((arg_count - 1) * sizeof(Set *))) == NULL) {
    append_to_output_buffer(ob, "-ERR out of memory\r\n", 20);
    return;
  }

  int j;
  int card = 0;
  Set *small = NULL;

  for (j = 1; j < arg_count; j++) {
    r_obj *o = hash_table_get(db, arg_values[j]);

    if (!o) {
      free(sets);
      append_to_output_buffer(ob, "~0\r\n", 4);
      return;
    }

    if (o->type != SET) {
      free(sets);
      char *msg = "-WRONGTYPE Operation against a key holding "
                  "the wrong kind of value\r\n";
      append_to_output_buffer(ob, msg, strlen(msg));
      return;
    }

    Set *set = (Set *)o->data;
    if (set->size == 0) {
      free(sets);
      append_to_output_buffer(ob, "~0\r\n", 4);
      return;
    }

    sets[j - 1] = set;

    if (small == NULL || set->size < card) {
      small = set;
      card = set->size;
    }
  }

  size_t count = 0;

  for (size_t i = 0; i <= small->size; i++) {
    Node *entry = small->buckets[i];
    while (entry) {
      int in_all = 1;

      for (int j = 0; j < arg_count - 1; j++) {
        if (sets[j] == small)
          continue;

        if (!set_is_member(sets[j], entry->key)) {
          in_all = 0;
          break;
        }
      }

      if (in_all) {
        Bytes *val_bytes = entry->key;
        char *val = val_bytes->data;
        uint32_t val_len = val_bytes->length;

        char bulk_header[64];
        int bh_len = snprintf(bulk_header, sizeof(bulk_header),
                              "$%" PRIu32 "\r\n", val_len);

        append_to_output_buffer(ob, bulk_header, bh_len);
        append_to_output_buffer(ob, val, val_len);
        append_to_output_buffer(ob, "\r\n", 2);

        count++;
      }

      entry = entry->next;
    }
  }

  free(sets);

  char header[64];
  int header_len = snprintf(header, sizeof(header), "~%zu\r\n", count);
  write(ob->fd, header, header_len);
  return;
}

void sismember_command(Client *client, HashTable *db, HashTable *expires,
                       OutputBuffer *ob) {
  Bytes **arg_values = client->arg_values;
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

void smembers_command(Client *client, HashTable *db, HashTable *expires,
                      OutputBuffer *ob) {
  Bytes **arg_values = client->arg_values;
  int arg_count = client->arg_count;

  if (arg_count != 2) {
    append_to_output_buffer(ob, "-ERR args\r\n", 11);
    return;
  }

  r_obj *o = hash_table_get(db, arg_values[1]);

  if (!o) {
    append_to_output_buffer(ob, "*0\r\n", 4);
    return;
  }

  if (o->type != SET) {
    char *msg = "-WRONGTYPE Operation against a key holding "
                "the wrong kind of value\r\n";
    append_to_output_buffer(ob, msg, strlen(msg));
    return;
  }

  Set *set = (Set *)o->data;
  size_t count = set->count;

  char header[64];
  int header_len = snprintf(header, sizeof(header), "*%zu\r\n", count);
  append_to_output_buffer(ob, header, header_len);

  int j;
  for (j = 0; j < set->size; j++) {
    Node *entry = set->buckets[j];
    while (entry) {
      Bytes *value_bytes = entry->key;

      char *val = value_bytes->data;
      uint32_t val_len = value_bytes->length;

      char bulk_header[64];
      int bh_len = snprintf(bulk_header, sizeof(bulk_header),
                            "$%" PRIu32 "\r\n", val_len);

      append_to_output_buffer(ob, bulk_header, bh_len);
      append_to_output_buffer(ob, val, val_len);
      append_to_output_buffer(ob, "\r\n", 2);

      entry = entry->next;
    }
  }

  return;
}

void hset_command(Client *client, HashTable *db, HashTable *expires,
                  OutputBuffer *ob) {

  Bytes **arg_values = client->arg_values;
  int arg_count = client->arg_count;

  if (arg_count < 4 || arg_count % 2 != 0) {
    append_to_output_buffer(ob, "-ERR args\r\n", 11);
    return;
  }

  r_obj *o = hash_table_get(db, arg_values[1]);

  if (o != NULL && o->type != HASH) {
    char *msg = "-WRONGTYPE Operation against a key holding "
                "the wrong kind of value\r\n";
    append_to_output_buffer(ob, msg, strlen(msg));
    return;
  }

  if (o == NULL) {
    o = create_hash_object();
    hash_table_set(db, arg_values[1], o);
  }

  int count = 0;
  int j;
  for (j = 2; j < arg_count; j++) {
    Bytes *field = arg_values[j];
    Bytes *value = arg_values[++j];
    hash_table_set((HashTable *)o->data, field,
                   create_string_object(value->data, value->length));
    count++;
  }

  char resp[64];
  int resp_len = snprintf(resp, sizeof(resp), ":%d\r\n", count);
  append_to_output_buffer(ob, resp, resp_len);
  return;
}

void hget_command(Client *client, HashTable *db, HashTable *expires,
                  OutputBuffer *ob) {

  Bytes **arg_values = client->arg_values;
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

  if (o->type != HASH) {
    char *msg = "-WRONGTYPE Operation against a key holding "
                "the wrong kind of value\r\n";
    append_to_output_buffer(ob, msg, strlen(msg));
    return;
  }

  HashTable *hash = (HashTable *)o->data;
  r_obj *hash_o = hash_table_get(hash, arg_values[2]);
  if (hash_o == NULL) {
    append_to_output_buffer(ob, "-ERR args\r\n", 11);
    return;
  }

  Bytes *value_bytes = (Bytes *)hash_o->data;
  uint32_t val_len = value_bytes->length;
  char *val = value_bytes->data;

  char bulk_header[64];
  int bh_len =
      snprintf(bulk_header, sizeof(bulk_header), "$%" PRIu32 "\r\n", val_len);

  append_to_output_buffer(ob, bulk_header, bh_len);
  append_to_output_buffer(ob, val, val_len);
  append_to_output_buffer(ob, "\r\n", 2);

  return;
}

void hmget_command(Client *client, HashTable *db, HashTable *expires,
                   OutputBuffer *ob) {

  Bytes **arg_values = client->arg_values;
  int arg_count = client->arg_count;

  if (arg_count < 3) {
    append_to_output_buffer(ob, "-ERR args\r\n", 11);
    return;
  }

  r_obj *o = hash_table_get(db, arg_values[1]);
  if (!o) {
    append_to_output_buffer(ob, "*0\r\n", 4);
    return;
  }

  if (o->type != HASH) {
    char *msg = "-WRONGTYPE Operation against a key holding "
                "the wrong kind of value\r\n";
    append_to_output_buffer(ob, msg, strlen(msg));
    return;
  }

  HashTable *hash = (HashTable *)o->data;

  char header[64];
  int header_len = snprintf(header, sizeof(header), "*%d\r\n", arg_count - 2);
  append_to_output_buffer(ob, header, header_len);

  for (int j = 2; j < arg_count; j++) {
    Bytes *field = arg_values[j];

    r_obj *field_o = hash_table_get(hash, field);

    if (field_o == NULL) {
      append_to_output_buffer(ob, "_\r\n", 3);
      continue;
    }

    Bytes *value_bytes = (Bytes *)field_o->data;

    uint32_t val_len = value_bytes->length;
    char *val = value_bytes->data;

    char bulk_header[64];
    int bh_len =
        snprintf(bulk_header, sizeof(bulk_header), "$%" PRIu32 "\r\n", val_len);

    append_to_output_buffer(ob, bulk_header, bh_len);
    append_to_output_buffer(ob, val, val_len);
    append_to_output_buffer(ob, "\r\n", 2);
  }

  return;
}

void hincrby_command(Client *client, HashTable *db, HashTable *expires,
                     OutputBuffer *ob) {
  Bytes **arg_values = client->arg_values;
  int arg_count = client->arg_count;

  if (arg_count != 4) {
    append_to_output_buffer(ob, "-ERR args\r\n", 11);
    return;
  }

  int64_t increment = 0;
  if (try_parse_int64(arg_values[3]->data, &increment) == 0) {
    append_to_output_buffer(
        ob, "-ERR value is not an integer or out of range\r\n", 44);
    return;
  }

  r_obj *o = hash_table_get(db, arg_values[1]);

  if (o != NULL && o->type != HASH) {
    char *msg = "-WRONGTYPE Operation against a key holding "
                "the wrong kind of value\r\n";
    append_to_output_buffer(ob, msg, strlen(msg));
    return;
  }

  if (o == NULL) {
    o = create_hash_object();
    hash_table_set(db, arg_values[1], o);
  }

  HashTable *hash = (HashTable *)o->data;

  r_obj *field_o = hash_table_get(hash, arg_values[2]);
  int64_t current_val = 0;

  if (field_o != NULL) {
    Bytes *bytes = (Bytes *)field_o->data;

    if (try_parse_int64(bytes->data, &current_val) == 0) {
      append_to_output_buffer(ob, "-ERR hash value is not an integer\r\n", 35);
      return;
    }
  }

  if ((increment > 0 && current_val > INT64_MAX - increment) ||
      (increment < 0 && current_val < INT64_MIN - increment)) {
    append_to_output_buffer(
        ob, "-ERR increment or decrement would overflow\r\n", 44);
    return;
  }

  int64_t new_val = current_val + increment;

  char num_str[64];
  int len = snprintf(num_str, sizeof(num_str), "%" PRId64, new_val);
  r_obj *new_obj = create_string_object(num_str, len);
  hash_table_set(hash, arg_values[2], new_obj);

  char resp[64];
  int resp_len = snprintf(resp, sizeof(resp), ":%" PRId64 "\r\n", new_val);
  append_to_output_buffer(ob, resp, resp_len);
  return;
}

void zadd_command(Client *client, HashTable *db, HashTable *expires,
                  OutputBuffer *ob) {

  Bytes **arg_values = client->arg_values;
  int arg_count = client->arg_count;

  if (arg_count < 4) {
    append_to_output_buffer(ob, "-ERR args\r\n", 11);
    return;
  }

  int flags = ZADD_SET_NO_FLAGS;
  int flags_count = 0;

  int j;
  for (j = 2; j < arg_count; j++) {
    Bytes *option_bytes = arg_values[j];
    char *option = option_bytes->data;

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
    double score = atof(arg_values[i]->data);

    Bytes *member = arg_values[++i];

    if (flags & ZADD_SET_INCR) {
      r_obj *score_o = hash_table_get(zs->dict, member);
      double current_score = 0.0;

      if (score_o != NULL) {
        current_score = *(double *)score_o->data;
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

void zrem_command(Client *client, HashTable *db, HashTable *expires,
                  OutputBuffer *ob) {
  Bytes **arg_values = client->arg_values;
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

  if (o->type != ZSET) {
    append_to_output_buffer(ob,
                            "-WRONGTYPE Operation against a key holding the "
                            "wrong kind of value\r\n",
                            68);
    return;
  }

  ZSet *zs = (ZSet *)o->data;

  int j;
  int deleted_count = 0;
  for (j = 2; j < arg_count; j++) {
    Bytes *member = arg_values[j];

    r_obj *score_o = hash_table_get(zs->dict, member);

    if (score_o == NULL) {
      continue;
    }

    double score = *(double *)score_o->data;

    if (zsl_remove(zs->zsl, score, member) == 1 &&
        hash_table_del(zs->dict, member) == 1) {
      deleted_count++;
    }
  }

  if (zs->dict->count == 0) {
    hash_table_del(db, arg_values[1]);
    hash_table_del(expires, arg_values[1]);
  }

  char num_str[64];
  int num_len = snprintf(num_str, sizeof(num_str), ":%d\r\n", deleted_count);
  append_to_output_buffer(ob, num_str, num_len);
  return;
}

void zcard_command(Client *client, HashTable *db, HashTable *expires,
                   OutputBuffer *ob) {
  Bytes **arg_values = client->arg_values;
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

  if (o->type != ZSET) {
    append_to_output_buffer(ob,
                            "-WRONGTYPE Operation against a key holding the "
                            "wrong kind of value\r\n",
                            68);
    return;
  }

  ZSet *zs = (ZSet *)o->data;

  char num_str[64];
  int num_len = snprintf(num_str, sizeof(num_str), ":%zu\r\n", zs->dict->count);
  append_to_output_buffer(ob, num_str, num_len);
  return;
}

void zrange_command(Client *client, HashTable *db, HashTable *expires,
                    OutputBuffer *ob) {
  Bytes **arg_values = client->arg_values;
  int arg_count = client->arg_count;

  if (arg_count < 4) {
    append_to_output_buffer(ob, "-ERR args\r\n", 11);
    return;
  }

  int flags = ZRANGE_SET_NO_FLAGS;
  int64_t limit_offset = 0;
  int64_t limit_count = -1; // -1 implies infinite
  int with_scores = 0;
  int reverse = 0;

  int j;
  for (j = 4; j < arg_count; j++) {
    char *option = arg_values[j]->data;

    if (strcasecmp(option, "BYSCORE") == 0)
      flags |= ZRANGE_SET_BYSCORE;
    else if (strcasecmp(option, "BYLEX") == 0)
      flags |= ZRANGE_SET_BYLEX;
    else if (strcasecmp(option, "REV") == 0) {
      flags |= ZRANGE_SET_REV;
      reverse = 1;
    } else if (strcasecmp(option, "WITHSCORES") == 0)
      with_scores = 1;
    else if (strcasecmp(option, "LIMIT") == 0) {
      if ((j + 2) >= arg_count) {
        append_to_output_buffer(ob, "-ERR syntax error\r\n", 19);
        return;
      }
      flags |= ZRANGE_SET_LIMIT;
      if (try_parse_int64(arg_values[j + 1]->data, &limit_offset) == 0 ||
          try_parse_int64(arg_values[j + 2]->data, &limit_count) == 0) {
        append_to_output_buffer(
            ob, "-value is not an integer or out of range\r\n", 42);
        return;
      }
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
    append_to_output_buffer(
        ob, "-ERR syntax error, WITHSCORES not supported with BYLEX\r\n;", 56);
    return;
  }
  if ((flags & ZRANGE_SET_LIMIT) && !(flags & ZRANGE_SET_BYSCORE) &&
      !(flags & ZRANGE_SET_BYLEX)) {
    append_to_output_buffer(
        ob, "-ERR syntax error, LIMIT only supported with BYSCORE or BYLEX\r\n",
        63);
    return;
  }

  r_obj *o = hash_table_get(db, arg_values[1]);

  if (!o) {
    append_to_output_buffer(ob, "*0\r\n", 4);
    return;
  }

  if (o->type != ZSET) {

    append_to_output_buffer(ob,
                            "-WRONGTYPE Operation against a key holding the "
                            "wrong kind of value\r\n",
                            68);
    return;
  }

  ZSet *zs = (ZSet *)o->data;
  ZSkipList *zsl = zs->zsl;
  ZSkipListNode *node;
  int count = 0;

  if (flags & ZRANGE_SET_BYSCORE) {
    double start = atof(arg_values[2]->data);
    double stop = atof(arg_values[3]->data);

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

    Bytes *min_arg = arg_values[2];
    Bytes *max_arg = arg_values[3];

    if (min_arg->length == 1 && min_arg->data[0] == '-') {
      node = zsl->head->level[0].forward;

    } else {
      if (min_arg->length < 1) {
        append_to_output_buffer(
            ob, "-ERR min or max not valid string range item\r\n", 46);
        return;
      }

      int inclusive = (min_arg->data[0] == '[');
      if (!inclusive && min_arg->data[0] != '(') {
        append_to_output_buffer(
            ob, "-ERR min or max not valid string range item\r\n", 46);
        return;
      }

      Bytes min_parsed;

      min_parsed.data = min_arg->data + 1;
      min_parsed.length = min_arg->length - 1;

      if (reverse)
        node = zsl_last_in_lex_range(zsl, &min_parsed, inclusive);
      else
        node = zsl_first_in_lex_range(zsl, &min_parsed, inclusive);
    }

    if (reverse && (max_arg->length == 1 && max_arg->data[0] == '+')) {
      node = zsl->tail;
    }

    while (node && limit_count > 0) {
      if (reverse) {
        if (!(min_arg->length == 1 && min_arg->data[0] == '-')) {
          Bytes min_val;
          min_val.data = min_arg->data + 1;
          min_val.length = min_arg->length - 1;
          int inclusive = (min_arg->data[0] == '[');

          int cmp = bytes_compare(node->element, &min_val);

          if (inclusive ? (cmp < 0) : (cmp <= 0))
            break;
        }
      } else {
        if (!(max_arg->length == 1 && max_arg->data[0] == '+')) {
          Bytes max_val;
          max_val.data = max_arg->data + 1;
          max_val.length = max_arg->length - 1;
          int inclusive = (max_arg->data[0] == '[');

          int cmp = bytes_compare(node->element, &max_val);

          if (inclusive ? (cmp > 0) : (cmp >= 0))
            break;
        }
      }

      zrange_emit_node(ob, node, 0);
      node = zsl_next_node(node, reverse);
      limit_count--;
    }

  } else {

    int64_t start;
    int64_t stop;

    if (try_parse_int64(arg_values[2]->data, &start) == 0 ||
        try_parse_int64(arg_values[3]->data, &stop) == 0) {
      append_to_output_buffer(
          ob, "-value is not an integer or out of range\r\n", 42);
    }

    size_t llen = zsl->length;

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
  Bytes **arg_values = client->arg_values;
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
  Bytes **arg_values = client->arg_values;
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
