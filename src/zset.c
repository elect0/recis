#include "../include/zset.h"
#include "../include/redis.h"

#include <stdlib.h>
#include <string.h>

ZSkipListNode *zsl_create_node(int level, double score, char *element) {
  ZSkipListNode *node;
  if ((node = (ZSkipListNode *)malloc(sizeof(ZSkipListNode) +
                                      level * sizeof(ZSkipListLevel))) == NULL)
    return NULL;

  node->score = score;
  node->element = element;
  return node;
}
int zsl_random_level() {
  int level = 1;
  while ((random() & 0xFFFF) < (ZSKIPLIST_P * 0xFFFF))
    level += 1;
  return (level < ZSKIPLIST_MAX_LEVEL) ? level : ZSKIPLIST_MAX_LEVEL;
}

ZSkipList *zsl_create() {
  int j;
  ZSkipList *zsl;
  if ((zsl = (ZSkipList *)malloc(sizeof(ZSkipList))) == NULL)
    return NULL;

  zsl->level = 1;
  zsl->length = 0;

  zsl->head = zsl_create_node(ZSKIPLIST_MAX_LEVEL, 0, NULL);

  for (j = 0; j < ZSKIPLIST_MAX_LEVEL; j++) {
    zsl->head->level[j].forward = NULL;
    zsl->head->level[j].span = 0;
  }

  zsl->head->backward = NULL;
  zsl->tail = NULL;
  return zsl;
}

ZSkipListNode *zsl_insert(ZSkipList *zsl, double score, char *element) {
  ZSkipListNode *update[ZSKIPLIST_MAX_LEVEL], *x;
  int i, level;
  unsigned int rank[ZSKIPLIST_MAX_LEVEL];

  x = zsl->head;

  for (i = zsl->level - 1; i >= 0; i--) {

    rank[i] = i == (zsl->level - 1) ? 0 : rank[i + 1];

    while (x->level[i].forward &&
           (x->level[i].forward->score < score ||
            (x->level[i].forward->score == score &&
             strcmp(x->level[i].forward->element, element) < 0))) {

      rank[i] += x->level[i].span;
      x = x->level[i].forward;
    }

    update[i] = x;
  }

  level = zsl_random_level();

  if (level > zsl->level) {
    for (i = zsl->level; i < level; i++) {
      rank[i] = 0;
      update[i] = zsl->head;
      update[i]->level[i].span = zsl->length;
    }
    zsl->level = level;
  }

  x = zsl_create_node(level, score, element);
  for (i = 0; i < level; i++) {
    x->level[i].forward = update[i]->level[i].forward;
    update[i]->level[i].forward = x;

    x->level[i].span = update[i]->level[i].span - (rank[0] - rank[i]);

    update[i]->level[i].span = (rank[0] - rank[i]) + 1;
  }

  for (i = level; i < zsl->level; i++) {
    update[i]->level[i].span++;
  }

  if (update[0] == zsl->head) {
    x->backward = NULL;
  } else {
    x->backward = update[0];
  }

  if (x->level[0].forward) {
    x->level[0].forward->backward = x;
  } else {
    zsl->tail = x;
  }

  zsl->length++;
  return x;
}

ZSet *zset_create() {
  ZSet *zs;
  if ((zs = (ZSet *)malloc(sizeof(ZSet))) == NULL)
    return NULL;

  zs->dict = hash_table_create(16);
  zs->zsl = zsl_create();
  return zs;
}

int zset_add(ZSet *zs, char *element, double score) {

  // TODO: implement update
  if (hash_table_get(zs->dict, element) != NULL)
    return 0;

  char *element_copy = strdup(element);

  zsl_insert(zs->zsl, score, element_copy);

  hash_table_set(zs->dict, element, create_double_object(score));

  return 1;
}

r_obj *create_zset_object() {
  r_obj *o;
  if ((o = (r_obj *)malloc(sizeof(r_obj))) == NULL)
    return NULL;

  o->type = ZSET;
  o->data = zset_create();
  return o;
}

ZSkipListNode *zsl_get_node_at_rank(ZSkipList *zsl, int rank) {
  ZSkipListNode *x = zsl->head;
  int traversed = 0;

  while (x->level[0].forward && traversed < rank) {
    x = x->level[0].forward;
    traversed++;
  }

  return x->level[0].forward;
}

ZSkipListNode *zsl_get_element_by_rank(ZSkipList *zsl, int rank) {
  ZSkipListNode *x = zsl->head->level[0].forward;
  int i = 0;
  while (x && i < rank) {
    x = x->level[0].forward;
    i++;
  }

  return x;
}

unsigned long zsl_get_rank(ZSkipList *zsl, double score, char *element) {
  ZSkipListNode *x = zsl->head;
  unsigned long rank = 0;
  int i;

  for (i = zsl->level; i >= 0; i--) {
    while (x->level[i].forward &&
           (x->level[i].forward->score < score ||
            (x->level[i].forward->score == score &&
             strcmp(x->level[i].forward->element, element) <= 0))) {

      rank += x->level[i].span;
      x = x->level[i].forward;

      if (x->score == score && strcmp(x->element, element) == 0)
        return rank;
    }
  }

  return 0;
}
