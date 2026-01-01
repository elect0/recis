#ifndef COMMAND_H
#define COMMAND_H

#include "networking.h"
#include "parser.h"
#include "redis.h"

typedef void (*commandProc)(Client *client, HashTable *db, HashTable *expires,
                            OutputBuffer *ob);

typedef struct Command_ {
  char *name;
  commandProc proc;
  int arity;
} Command;

r_obj *create_command_object(Command *cmd);

long long get_time_ms();
void populate_command_table(HashTable *registry);

void set_command(Client *client, HashTable *db, HashTable *expires,
                 OutputBuffer *ob);
void get_command(Client *client, HashTable *db, HashTable *expires,
                 OutputBuffer *ob);
void del_command(Client *client, HashTable *db, HashTable *expires,
                 OutputBuffer *ob);
void ttl_command(Client *client, HashTable *db, HashTable *expires,
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
void sadd_command(Client *client, HashTable *db, HashTable *expires,
                  OutputBuffer *ob);
void sismember_command(Client *client, HashTable *db, HashTable *expires,
                       OutputBuffer *ob);
void zadd_command(Client *client, HashTable *db, HashTable *expires,
                  OutputBuffer *ob);
void zrange_command(Client *client, HashTable *db, HashTable *expires,
                    OutputBuffer *ob);
void zscore_command(Client *client, HashTable *db, HashTable *expires,
                    OutputBuffer *ob);
void zrank_command(Client *client, HashTable *db, HashTable *expires,
                   OutputBuffer *ob);
void save_command(Client *client, HashTable *db, HashTable *expires,
                  OutputBuffer *ob);
void ping_command(Client *client, HashTable *db, HashTable *expires,
                  OutputBuffer *ob);
#endif // !COMMAND_H
