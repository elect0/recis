#include "../include/set.h"
#include "../include/redis.h"
#include <stdlib.h>

static char *SET_DUMMY_VALUE = "1";

r_obj *create_set_object() {
  r_obj *o;

  if ((o = (r_obj *)malloc(sizeof(r_obj))) == NULL)
    return NULL;

  o->type = SET;

  o->data = hash_table_create(16);

  if (o->data == NULL) {
    free(o);
    return NULL;
  }

  return o;
}

int set_add(Set *set, char *member) {
  if (hash_table_get(set, member) != NULL)
    return 0;

  hash_table_set(set, member, create_string_object(SET_DUMMY_VALUE));
  return 1;
}

int set_rem(Set *set, char *member) {
  if (hash_table_get(set, member) == NULL)
    return 0;

  hash_table_del(set, member);
  return 1;
}

int set_is_member(Set *set, char *member) {
  return hash_table_get(set, member) != NULL;
}
