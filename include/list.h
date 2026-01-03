#ifndef LIST_H
#define LIST_H

#include <stddef.h>

struct RObj;
typedef struct RObj r_obj;

// Bitmasks for ZADD command
#define LMOVE_SRC_LEFT 1 << 0
#define LMOVE_SRC_RIGHT 1 << 1
#define LMOVE_DEST_LEFT 1 << 2
#define LMOVE_DEST_RIGHT 1 << 3

typedef struct ListNode_ {
  r_obj *value;
  struct ListNode_ *next;
  struct ListNode_ *prev;
} ListNode;

typedef struct List_ {
  size_t size;

  ListNode *tail;
  ListNode *head;
} List;

r_obj *create_list_object(void);

List *list_create(void);
int list_ins_node_head(List *list, r_obj *value);
int list_ins_node_tail(List *list, r_obj *value);
r_obj *list_pop_tail(List *list);
r_obj *list_pop_head(List *list);
ListNode *list_get_node_at(List *list, size_t index, int from_tail);
void list_destroy(List *list);

#define list_size(list) ((list)->size)

#endif // !LIST_H
