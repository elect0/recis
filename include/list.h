#ifndef LIST_H
#define LIST_H

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

List *list_create(void);
int list_ins_node_head(List *list, const void *value);
int list_ins_node_tail(List *list, const void *value);

#define list_size(list) ((list)->size)

#endif // !LIST_H
