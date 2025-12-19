#ifndef SET_H
#define SET_H

#include "redis.h"

typedef HashTable Set;

r_obj *create_set_object();
int set_add(Set *set, char *member);
int set_rem(Set *set, char *member);
int set_is_member(Set *set, char *member);

#endif // !SET_H
