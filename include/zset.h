#ifndef ZSET_H
#define ZSET_H

#include "client.h"
#include "hash_table.h"

struct RObj;
typedef struct RObj r_obj;

// Bitmasks for ZADD command
#define ZADD_SET_NO_FLAGS 0
#define ZADD_SET_NX 1 << 0
#define ZADD_SET_XX 1 << 1
#define ZADD_SET_GT 1 << 2
#define ZADD_SET_LT 1 << 3
#define ZADD_SET_CH 1 << 4
#define ZADD_SET_INCR 1 << 5

// Bitmasks for ZRANGE command
#define ZRANGE_SET_NO_FLAGS 0
#define ZRANGE_SET_BYSCORE 1 << 0
#define ZRANGE_SET_BYLEX 1 << 1
#define ZRANGE_SET_REV 1 << 2
#define ZRANGE_SET_LIMIT 1 << 3
#define ZRANGE_SET_WITHSCORES 1 << 4

#define ZSKIPLIST_MAX_LEVEL 32
#define ZSKIPLIST_P 0.25

typedef struct ZSkipListLevel_ {
  struct ZSkipListNode_ *forward;
  unsigned long span;
} ZSkipListLevel;

typedef struct ZSkipListNode_ {
  Bytes *element;
  double score;
  struct ZSkipListNode_ *backward;

  ZSkipListLevel level[];
} ZSkipListNode;

typedef struct ZSkipList_ {
  ZSkipListNode *head;
  ZSkipListNode *tail;

  size_t length;
  int level;
} ZSkipList;

typedef struct ZSet_ {
  HashTable *dict;
  ZSkipList *zsl;
} ZSet;

r_obj *create_zset_object();

ZSet *zset_create();
int zset_add(ZSet *zs, Bytes *element, double score);
void zset_range(ZSet *zs, int min_index, int max_index);
void zset_destroy(ZSet *zs);

void zrange_emit_node(OutputBuffer *ob, ZSkipListNode *node, int with_scores);
ZSkipListNode *zsl_next_node(ZSkipListNode *node, int reverse);
ZSkipListNode *zsl_last_in_range(ZSkipList *zsl, double max);
ZSkipListNode *zsl_last_in_lex_range(ZSkipList *zsl, Bytes *max, int inclusive);
ZSkipListNode *zsl_get_element_by_rank(ZSkipList *zsl, int rank);
ZSkipListNode *zsl_first_in_range(ZSkipList *zsl, double min);
int zsl_remove(ZSkipList *zsl, double score, Bytes *element);
ZSkipListNode *zsl_first_in_lex_range(ZSkipList *zsl, Bytes *min,
                                      int inclusive);
unsigned long zsl_get_rank(ZSkipList *zsl, double score, Bytes *element);

#endif // !ZSET_H
