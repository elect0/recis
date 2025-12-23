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
  }

  zsl->head->backward = NULL;
  zsl->tail = NULL;
  return zsl;
}

ZSkipListNode *zsl_insert(ZSkipList *zsl, double score, char *element) {
  ZSkipListNode *update[ZSKIPLIST_MAX_LEVEL], *x;
  int i, level;

  x = zsl->head;

  for (i = zsl->level - 1; i >= 0; i--) {
    while (x->level[i].forward &&
           (x->level[i].forward->score < score ||
            (x->level[i].forward->score == score &&
             strcmp(x->level[i].forward->element, element) < 0))) {
      x = x->level[i].forward;
    }

    update[i] = x;
  }

  level = zsl_random_level();

  if (level > zsl->level) {
    for (i = zsl->level; i < level; i++) {
      update[i] = zsl->head;
    }
    zsl->level = level;
  }

  x = zsl_create_node(level, score, element);
  for (i = 0; i < level; i++) {
    x->level[i].forward = update[i]->level[i].forward;
    update[i]->level[i].forward = x;
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
