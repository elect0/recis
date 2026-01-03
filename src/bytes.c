#include "../include/bytes.h"

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

Bytes *create_bytes_object(const char *data, uint32_t length) {
  Bytes *b;
  if ((b = (Bytes *)malloc(sizeof(Bytes))) == NULL)
    return NULL;

  b->length = length;
  b->data = malloc(length + 1);
  if (b->data == NULL) {
    free(b);
    return NULL;
  }

  memcpy(b->data, data, length);
  b->data[length] = '\0';
  return b;
}

int bytes_equal(const Bytes *b1, const Bytes *b2) {
  if (b1->length != b2->length)
    return 0;
  return memcmp(b1->data, b2->data, b1->length) == 0;
}

// Binary-safe comp. (for sorting/skip lists)
int bytes_compare(const Bytes *b1, const Bytes *b2) {
  size_t min_len = (b1->length < b2->length) ? b1->length : b2->length;
  int cmp = memcmp(b1->data, b2->data, min_len);

  if (cmp == 0)
    return b1->length - b2->length;
  return cmp;
}

void free_bytes_object(Bytes *b) {
  if (b == NULL)
    return;

  if (b->data != NULL)
    free(b->data);
  free(b);
  return;
}

Bytes *bytes_dup(const Bytes *src){
  if(!src) return NULL;
  return create_bytes_object(src->data, src->length);
}
