#include "../include/networking.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>

OutputBuffer *create_output_buffer(int fd) {
  OutputBuffer *ob;
  if ((ob = (OutputBuffer *)malloc(sizeof(OutputBuffer))) == NULL)
    return NULL;
  ob->fd = fd;
  ob->length = 0;
  ob->capacity = 16384;
  ob->data = calloc(ob->capacity, sizeof(char));
  if (ob->data == NULL) {
    free(ob);
    return NULL;
  }

  return ob;
}

void flush_buffer(OutputBuffer *ob) {
  size_t written = 0;
  while (written < ob->length) {
    ssize_t n = write(ob->fd, ob->data + written, ob->length - written);
    if (n <= 0) {
      break;
    }
    written += n;
  }

  ob->length = 0;
}

void append_to_output_buffer(OutputBuffer *ob, const char *data, size_t len) {
  if (len > ob->capacity) {
    flush_buffer(ob);
    write(ob->fd, data, len);
    return;
  }

  if (ob->length + len > ob->capacity) {
    flush_buffer(ob);
  }

  memcpy(ob->data + ob->length, data, len);
  ob->length += len;
}
