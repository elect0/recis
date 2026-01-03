#include "../include/list.h"
#include "../include/recis.h"
#include "stdlib.h"
#include <stdio.h>

r_obj *create_list_object() {
  r_obj *o;
  if ((o = (r_obj *)malloc(sizeof(r_obj))) == NULL) {
    return NULL;
  }

  o->type = LIST;
  o->data = list_create();

  if (o->data == NULL) {
    free(o);
    return NULL;
  }

  return o;
}

List *list_create() {
  List *list;

  if ((list = (List *)malloc(sizeof(List))) == NULL) {
    return NULL;
  }

  list->head = NULL;
  list->tail = NULL;
  list->size = 0;
  return list;
}

int list_ins_node_head(List *list, r_obj *value) {
  ListNode *new_node;

  if ((new_node = (ListNode *)malloc(sizeof(ListNode))) == NULL) {
    return -1;
  }

  new_node->value = value;

  if (list_size(list) == 0) {
    list->head = list->tail = new_node;
    new_node->prev = new_node->next = NULL;
  } else {
    new_node->prev = NULL;
    new_node->next = list->head;
    list->head->prev = new_node;
    list->head = new_node;
  }

  list->size++;
  return 0;
}

int list_ins_node_tail(List *list, r_obj *value) {
  ListNode *new_node;

  if ((new_node = (ListNode *)malloc(sizeof(ListNode))) == NULL) {
    return -1;
  }

  new_node->value = value;

  if (list_size(list) == 0) {
    list->tail = list->head = new_node;
    new_node->prev = new_node->next = NULL;
  } else {
    new_node->next = NULL;
    new_node->prev = list->tail;
    list->tail->next = new_node;
    list->tail = new_node;
  }

  list->size++;
  return 0;
}

r_obj *list_pop_tail(List *list) {
  if (list_size(list) == 0)
    return NULL;

  ListNode *node = list->tail;
  r_obj *value = node->value;

  if (list->head == list->tail) {
    list->head = NULL;
    list->tail = NULL;
  } else {
    list->tail = node->prev;
    list->tail->next = NULL;
  }

  free(node);
  list->size--;

  return value;
}

r_obj *list_pop_head(List *list) {
  if (list_size(list) == 0)
    return NULL;
  ListNode *node = list->head;
  r_obj *value = node->value;

  if (list->head == list->tail) {
    list->head = NULL;
    list->tail = NULL;
  } else {
    list->head = node->next;
    list->head->prev = NULL;
  }

  free(node);
  list->size--;

  return value;
}

ListNode *list_get_node_at(List *list, size_t index, int from_tail) {
  if (index >= list_size(list))
    return NULL;

  ListNode *member;
  if (from_tail) {
    for (member = list->tail; index > 0; index--) {
      member = member->prev;
    }
  } else {
    for (member = list->head; index > 0; index--) {
      member = member->next;
    }
  }

  return member;
}

void list_destroy(List *list) {

  if (list == NULL)
    return;

  while (list->size > 0) {
    r_obj *value = list_pop_tail(list);

    if (value)
      free_object(value);
  }

  free(list);
}
