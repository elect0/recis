#ifndef ZSET_H
#define ZSET_H

#include "redis.h"

#define ZSKIPLIST_MAX_LEVEL 32
#define ZSKIPLIST_P 0.25

typedef struct ZSkipListLevel_ {
  struct ZSkipListNode_ *forward;
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

ZSet *zset_create();
int zset_add(ZSet *zs, char *element, double score);
void zset_range(ZSet *zs, int min_index, int max_index);

#endif // !ZSET_H
