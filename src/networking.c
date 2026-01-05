#include "../include/networking.h"
#include <asm-generic/errno-base.h>
#include <asm-generic/errno.h>
#include <errno.h>
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
  ob->capacity = 32 * 1024;
  ob->data = calloc(ob->capacity, sizeof(char));
  if (ob->data == NULL) {
    free(ob);
    return NULL;
  }

  return ob;
}

void flush_buffer(OutputBuffer *ob) {
  if (ob->length == 0)
    return;

  ssize_t written = write(ob->fd, ob->data, ob->length);

  if (written == -1) {
    if (errno == EAGAIN || errno == EWOULDBLOCK) {
      return;
    }
    ob->length = 0;
    return;
  }

  if (written < ob->length) {
    size_t remaining = ob->length - written;
    memmove(ob->data, ob->data + written, remaining);
    ob->length = remaining;
  } else {
    ob->length = 0;
  }
}

void append_to_output_buffer(OutputBuffer *ob, const char *data, size_t len) {
  if (ob->length + len <= ob->capacity) {
    memcpy(ob->data + ob->length, data, len);
    ob->length += len;
    return;
  }

  flush_buffer(ob);

  if (ob->length + len > ob->capacity) {
    size_t new_cap = ob->capacity * 2;
    if (new_cap < ob->length + len)
      new_cap = ob->length + len + 1024;

    char *new_buf = realloc(ob->data, new_cap);
    if (!new_buf) {
      return;
    }
    ob->data = new_buf;
    ob->capacity = new_cap;
  }

  memcpy(ob->data + ob->length, data, len);
  ob->length += len;
}
