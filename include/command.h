#ifndef COMMAND_H
#define COMMAND_H

#include "hash_table.h"
#include "networking.h"
#include "parser.h"

typedef struct CommandContext_ {
  Client *client;
  HashTable *db;
  HashTable *expires;
  HashTable *vector_indices;
  OutputBuffer *ob;
} CommandContext;

typedef void (*commandProc)(CommandContext *ctx);

typedef struct Command_ {
  char *name;
  commandProc proc;
  int arity;
} Command;

r_obj *create_command_object(Command *cmd);

long long get_time_ms();
Command *command_lookup(char *name, int len);

void set_command(CommandContext *ctx);
void get_command(CommandContext *ctx);
void del_command(CommandContext *ctx);
void ttl_command(CommandContext *ctx);
void incr_command(CommandContext *ctx);
void incrby_command(CommandContext *ctx);
void lpush_command(CommandContext *ctx);
void rpush_command(CommandContext *ctx);
void rpop_command(CommandContext *ctx);
void lpop_command(CommandContext *ctx);
void llen_command(CommandContext *ctx);
void lindex_command(CommandContext *ctx);
void lrange_command(CommandContext *ctx);
void lmove_command(CommandContext *ctx);
void ltrim_command(CommandContext *ctx);
void sadd_command(CommandContext *ctx);
void srem_command(CommandContext *ctx);
void sinter_command(CommandContext *ctx);
void smembers_command(CommandContext *ctx);
void scard_command(CommandContext *ctx);
void sismember_command(CommandContext *ctx);
void hset_command(CommandContext *ctx);
void hget_command(CommandContext *ctx);
void hmget_command(CommandContext *ctx);
void hincrby_command(CommandContext *ctx);
void zadd_command(CommandContext *ctx);
void zrem_command(CommandContext *ctx);
void zcard_command(CommandContext *ctx);
void zrange_command(CommandContext *ctx);
void zscore_command(CommandContext *ctx);
void zrank_command(CommandContext *ctx);
void vidx_create_command(CommandContext *ctx);
void vidx_drop_command(CommandContext *ctx);
void vidx_list(CommandContext *ctx);
void vidx_info_command(CommandContext *ctx);
void vadd_command(CommandContext *ctx);
void save_command(CommandContext *ctx);
void ping_command(CommandContext *ctx);

#endif // !COMMAND_H
