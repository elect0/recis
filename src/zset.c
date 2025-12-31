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

void zsl_destroy(ZSkipList *zsl) {
  ZSkipListNode *node = zsl->head->level[0].forward;
  ZSkipListNode *next;

  while (node) {
    next = node->level[0].forward;
    free(node->element);
    free(node);
    node = next;
  }

  free(zsl->head);
  free(zsl);
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

void zset_destroy(ZSet *zs) {
  zsl_destroy(zs->zsl);
  hash_table_destroy(zs->dict);

  free(zs);
}

int zsl_remove(ZSkipList *zsl, double score, char *element) {
  ZSkipListNode *x = zsl->head;
  ZSkipListNode *update[ZSKIPLIST_MAX_LEVEL];

  int i;
  for (i = zsl->level - 1; i >= 0; i--) {
    while (x->level[i].forward &&
           (x->level[i].forward->score < score ||
            (x->level[i].forward->score == score &&
             strcmp(x->level[i].forward->element, element) < 0))) {
      x = x->level[i].forward;
    }
    update[i] = x;
  }

  ZSkipListNode *candidate = update[0]->level[0].forward;
  if (candidate != NULL && candidate->score == score &&
      strcasecmp(candidate->element, element) == 0) {
    for (i = zsl->level - 1; i >= 0; i--) {
      if (update[i]->level[i].forward == candidate) {
        update[i]->level[i].span += candidate->level[i].span - 1;
        update[i]->level[i].forward = candidate->level[i].forward;
      } else {
        update[i]->level[i].span -= 1;
      }
    }

    if (candidate->level[0].forward) {
      candidate->level[0].forward->backward = candidate->backward;
    } else {
      zsl->tail = candidate->backward;
    }

    while (zsl->level > 1 && zsl->head->level[zsl->level - 1].forward == NULL) {
      zsl->level--;
    }

    zsl->length--;

    free(candidate->element);
    free(candidate);

    return 1;
  }

  return 0;
}

int zset_add(ZSet *zs, char *element, double score) {
  r_obj *score_o;
  if ((score_o = hash_table_get(zs->dict, element)) != NULL) {
    double current_score = *(double *)score_o->data;
    if (current_score == score)
      return 0;

    if (zsl_remove(zs->zsl, current_score, element) == 0)
      return 0;

    zsl_insert(zs->zsl, score, strdup(element));
    hash_table_set(zs->dict, element, create_double_object(score));

    return 1;
  }

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

ZSkipListNode *zsl_get_element_by_rank(ZSkipList *zsl, int rank) {
  ZSkipListNode *x = zsl->head->level[0].forward;
  unsigned long traversed = 0;
  int i = 0;

  for (i = zsl->level - 1; i >= 0; i--) {
    while (x->level[i].forward && (traversed + x->level[i].span) <= rank) {
      traversed += x->level[i].span;
      x = x->level[i].forward;
    }

    if (traversed == rank) {
      return x;
    }
  }

  return x;
}

ZSkipListNode *zsl_first_in_range(ZSkipList *zsl, double min) {
  ZSkipListNode *x = zsl->head;
  int i;
  for (i = zsl->level - 1; i >= 0; i--) {
    while (x->level[i].forward && x->level[i].forward->score < min) {
      x = x->level[i].forward;
    }
  }

  x = x->level[0].forward;

  if (x && x->score >= min)
    return x;
  return NULL;
}

ZSkipListNode *zsl_first_in_lex_range(ZSkipList *zsl, char *min,
                                      int inclusive) {
  ZSkipListNode *x = zsl->head;
  int i;
  for (i = zsl->level - 1; i >= 0; i--) {
    while (x->level[i].forward) {
      char *next_val = x->level[i].forward->element;
      int cmp = strcmp(next_val, min);

      if (inclusive ? (cmp < 0) : (cmp <= 0)) {
        x = x->level[i].forward;
      } else {
        break;
      }
    }
  }

  x = x->level[0].forward;
  return x;
}

unsigned long zsl_get_rank(ZSkipList *zsl, double score, char *element) {
  ZSkipListNode *x = zsl->head;
  unsigned long rank = 0;
  int i;

  for (i = zsl->level - 1; i >= 0; i--) {
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
