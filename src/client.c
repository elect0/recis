#include "../include/client.h"
#include <asm-generic/errno-base.h>
#include <errno.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <stdio.h>
#include <unistd.h>

Client *create_client(int fd) {
  Client *client;
  if ((client = (Client *)malloc(sizeof(Client))) == NULL)
    return NULL;

  int yes = 1;
  if (setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes)) == -1) {
    perror("setsockopt TCP_NODELAY");
  }
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
  client->query_pos = 0;

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
    if (errno == EAGAIN || errno == EWOULDBLOCK)
      return 0;
    return -1;
  } else if (nread == 0) {
    return -1;
  }

  client->query_length += nread;
  return 0;
}

void reset_client_args(Client *client) {
  if (client->arg_values) {
    for (int i = 0; i < client->arg_count; i++) {
      if (client->arg_values[i] != NULL) {
        free_bytes_object(client->arg_values[i]);
        client->arg_values[i] = NULL;
      }
    }
  }

  client->arg_count = 0;
}
