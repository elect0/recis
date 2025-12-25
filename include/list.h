#ifndef LIST_H
#define LIST_H

#include "redis.h"

typedef struct ListNode_ {
  void *value;
  struct ListNode_ *next;
  struct ListNode_ *prev;
} ListNode;

typedef struct List_ {
  unsigned long size;

  ListNode *tail;
  ListNode *head;
} List;

r_obj *create_list_object(void);

List *list_create(void);
int list_ins_node_head(List *list, const void *value);
int list_ins_node_tail(List *list, const void *value);
void *list_pop_tail(List *list);
void list_destroy(List *list);

#define list_size(list) ((list)->size)

#endif // !LIST_H
