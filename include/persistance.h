#ifndef PERSISTANCE_H
#define PERSISTANCE_H
#include "redis.h"

void rdb_save(HashTable *db, char *filename);

#endif // !PERSISTANCE_H
