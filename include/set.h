#ifndef SET_H
#define SET_H

#include "hash_table.h"

typedef HashTable Set;

r_obj *create_set_object();
int set_add(Set *set, Bytes *member);
int set_rem(Set *set, Bytes *member);
int set_is_member(Set *set, Bytes *member);

#endif // !SET_H
