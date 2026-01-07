#ifndef RECIS_H
#define RECIS_H

#include "parser.h"
#include <stddef.h>
#include <stdint.h>

// Bitmasks for SET command
#define OBJ_SET_NO_FLAGS 0
#define OBJ_SET_NX (1 << 0)
#define OBJ_SET_XX (1 << 1)
#define OBJ_SET_EX (1 << 2)
#define OBJ_SET_PX (1 << 3)
#define OBJ_SET_KEEPTTL (1 << 4)
#define OBJ_SET_GET (1 << 5)
#define OBJ_SET_IFEQ (1 << 6)
#define OBJ_SET_IFNE (1 << 7)

typedef enum {
  STRING = 0,
  LIST = 1,
  SET = 2,
  HASH = 3,
  INT = 4,
  DOUBLE = 5,
  ZSET = 6,
  COMMAND = 7,
  VECTOR = 8,
  HNSW = 9,
} obj_type;

typedef struct RObj {
  obj_type type;
  void *data;
} r_obj;

r_obj *create_string_object(const char *str, uint32_t length);
r_obj *create_int_object(long long value);
r_obj *create_double_object(double value);
void free_object(r_obj *o);

#endif // !RECIS_H
