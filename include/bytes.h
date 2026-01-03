#ifndef BYTES_H
#define BYTES_H

#include <stdint.h>

typedef struct Bytes_ {
  uint32_t length;
  char *data;
} Bytes;

Bytes *create_bytes_object(const char *data, uint32_t length);
int bytes_equal(const Bytes *b1, const Bytes *b2);
int bytes_compare(const Bytes *b1, const Bytes *b2);
void free_bytes_object(Bytes *b);
Bytes *bytes_dup(const Bytes *src);

#endif // !BYTES_H
