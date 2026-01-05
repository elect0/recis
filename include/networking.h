#include <stdlib.h>
#ifndef NETWORKING_H
#define NETWORKING_H

typedef struct OutputBuffer_ {
  char *data;
  size_t length;
  size_t capacity;
  int fd;
} OutputBuffer;

OutputBuffer *create_output_buffer(int fd);
void append_to_output_buffer(OutputBuffer *ob, const char *data, size_t len);
void flush_buffer(OutputBuffer *ob);


#endif // !NETWORKING_H
