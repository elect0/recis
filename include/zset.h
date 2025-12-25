#ifndef ZSET_H
#define ZSET_H

#include "redis.h"

#define ZSKIPLIST_MAX_LEVEL 32
#define ZSKIPLIST_P 0.25

typedef struct ZSkipListLevel_ {
  struct ZSkipListNode_ *forward;
  unsigned long span;
} ZSkipListLevel;

typedef struct ZSkipListNode_ {
  char *element;
  double score;
  struct ZSkipListNode_ *backward;

  ZSkipListLevel level[];
} ZSkipListNode;

typedef struct ZSkipList_ {
  ZSkipListNode *head;
  ZSkipListNode *tail;

  unsigned long length;
  int level;
} ZSkipList;

typedef struct ZSet_ {
  HashTable *dict;
  ZSkipList *zsl;
} ZSet;

r_obj *create_zset_object();

ZSet *zset_create();
int zset_add(ZSet *zs, char *element, double score);
void zset_range(ZSet *zs, int min_index, int max_index);
void zset_destroy(ZSet *zs);

ZSkipListNode *zsl_get_node_at_rank(ZSkipList *zsl, int rank);
ZSkipListNode *zsl_get_element_by_rank(ZSkipList *zsl, int rank);
unsigned long zsl_get_rank(ZSkipList *zsl, double score, char *element);

#endif // !ZSET_H
