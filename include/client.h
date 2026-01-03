#include "bytes.h"
#include <stdint.h>
#include <stdlib.h>

#include "networking.h"

#ifndef CLIENT_H
#define CLIENT_H
typedef struct Client_ {
  int fd;

  char *query_buffer;
  size_t query_length;
  size_t query_cap;

  int arg_count;
  Bytes **arg_values;
  size_t arg_values_cap;

  OutputBuffer *output_buffer;
} Client;

Client *create_client(int fd);
int read_from_client(Client *client);
void reset_client_args(Client *client);

#endif // !CLIENT_H
