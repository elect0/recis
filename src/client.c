#include "../include/client.h"
#include <asm-generic/errno-base.h>
#include <errno.h>
#include <unistd.h>

Client *create_client(int fd) {
  Client *client;
  if ((client = (Client *)malloc(sizeof(Client))) == NULL)
    return NULL;
  client->fd = fd;

  client->query_cap = 4096;
  if ((client->query_buffer = malloc(client->query_cap)) == NULL) {
    free(client);
    return NULL;
  }
  client->query_length = 0;

  client->arg_count = 0;
  client->arg_values = NULL;
  client->arg_values_cap = 0;

  client->output_buffer = create_output_buffer(fd);

  return client;
}

int read_from_client(Client *client) {
  if (client->query_length >= client->query_cap) {
    client->query_cap *= 2;
    client->query_buffer = realloc(client->query_buffer, client->query_cap);
  }

  ssize_t nread = read(client->fd, client->query_buffer + client->query_length,
                       client->query_cap - client->query_length);
  if (nread == -1) {
    if (errno == EAGAIN)
      return 0;
    return -1;
  } else if (nread == -1) {
    return 0;
  }

  client->query_length += nread;
  return 0;
}

void reset_client_args(Client *client) {
  if (client->arg_values) {
    for (int i = 0; i < client->arg_count; i++) {
      free(client->arg_values[i]);
      client->arg_values[i] = NULL;
    }
  }

  client->arg_count = 0;
  client->query_length = 0;
}
