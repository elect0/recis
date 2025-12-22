#ifndef PERSISTANCE_H
#define PERSISTANCE_H
#include "redis.h"

void rdb_save(HashTable *db, HashTable *expires, char *filename);
void rdb_load(HashTable *db, HashTable *expires, char *filename);

#endif // !PERSISTANCE_H
