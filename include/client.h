#include <stdlib.h>

typedef struct Client_ {
  int fd;

  char *query_buffer;
  size_t query_length;
  size_t query_cap;

  int arg_count;
  char **arg_values;
  size_t arg_values_cap;
} Client;

Client *create_client(int fd);
int read_from_client(Client *client);
void reset_client_args(Client *client);


