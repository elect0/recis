#ifndef COMMAND_H
#define COMMAND_H

#include "hash_table.h"
#include "networking.h"
#include "parser.h"

typedef void (*commandProc)(Client *client, HashTable *db, HashTable *expires,
                            OutputBuffer *ob);

typedef struct Command_ {
  char *name;
  commandProc proc;
  int arity;
} Command;

r_obj *create_command_object(Command *cmd);

long long get_time_ms();
Command *command_lookup(char *name, int len);

void set_command(Client *client, HashTable *db, HashTable *expires,
                 OutputBuffer *ob);
void get_command(Client *client, HashTable *db, HashTable *expires,
                 OutputBuffer *ob);
void del_command(Client *client, HashTable *db, HashTable *expires,
                 OutputBuffer *ob);
void ttl_command(Client *client, HashTable *db, HashTable *expires,
                 OutputBuffer *ob);
void incr_command(Client *client, HashTable *db, HashTable *expires,
                  OutputBuffer *ob);
void incrby_command(Client *client, HashTable *db, HashTable *expires,
                    OutputBuffer *ob);
void lpush_command(Client *client, HashTable *db, HashTable *expires,
                   OutputBuffer *ob);
void rpush_command(Client *client, HashTable *db, HashTable *expires,
                   OutputBuffer *ob);
void rpop_command(Client *client, HashTable *db, HashTable *expires,
                  OutputBuffer *ob);
void lpop_command(Client *client, HashTable *db, HashTable *expires,
                  OutputBuffer *ob);
void llen_command(Client *client, HashTable *db, HashTable *expires,
                  OutputBuffer *ob);
void lindex_command(Client *client, HashTable *db, HashTable *expires,
                    OutputBuffer *ob);
void lrange_command(Client *client, HashTable *db, HashTable *expires,
                    OutputBuffer *ob);
void lmove_command(Client *client, HashTable *db, HashTable *expires,
                   OutputBuffer *ob);
void ltrim_command(Client *client, HashTable *db, HashTable *expires,
                   OutputBuffer *ob);
void sadd_command(Client *client, HashTable *db, HashTable *expires,
                  OutputBuffer *ob);
void srem_command(Client *client, HashTable *db, HashTable *expires,
                  OutputBuffer *ob);
void sinter_command(Client *client, HashTable *db, HashTable *expires,
                    OutputBuffer *ob);
void smembers_command(Client *client, HashTable *db, HashTable *expires,
                      OutputBuffer *ob);
void scard_command(Client *client, HashTable *db, HashTable *expires,
                   OutputBuffer *ob);
void sismember_command(Client *client, HashTable *db, HashTable *expires,
                       OutputBuffer *ob);
void hset_command(Client *client, HashTable *db, HashTable *expires,
                  OutputBuffer *ob);
void hget_command(Client *client, HashTable *db, HashTable *expires,
                  OutputBuffer *ob);
void hmget_command(Client *client, HashTable *db, HashTable *expires,
                   OutputBuffer *ob);
void hincrby_command(Client *client, HashTable *db, HashTable *expires,
                     OutputBuffer *ob);
void zadd_command(Client *client, HashTable *db, HashTable *expires,
                  OutputBuffer *ob);
void zrem_command(Client *client, HashTable *db, HashTable *expires,
                  OutputBuffer *ob);
void zcard_command(Client *client, HashTable *db, HashTable *expires,
                  OutputBuffer *ob);
void zrange_command(Client *client, HashTable *db, HashTable *expires,
                    OutputBuffer *ob);
void zscore_command(Client *client, HashTable *db, HashTable *expires,
                    OutputBuffer *ob);
void zrank_command(Client *client, HashTable *db, HashTable *expires,
                   OutputBuffer *ob);
void vidx_create_command(Client *client, HashTable *db, HashTable *expires,
                   OutputBuffer *ob);
void save_command(Client *client, HashTable *db, HashTable *expires,
                  OutputBuffer *ob);
void ping_command(Client *client, HashTable *db, HashTable *expires,
                  OutputBuffer *ob);
#endif // !COMMAND_H
