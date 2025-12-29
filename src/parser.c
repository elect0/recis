#include <stdlib.h>
#include <string.h>
#include <sys/types.h>

#include "../include/parser.h"

// read until carriage return + new line
int read_until_crlf(const char *buffer, int input_len, int *pos, char *out,
                    int max_out_len) {

  int i = 0;
  while (*pos < input_len && buffer[*pos] != '\r') {
    if (buffer[*pos] == '\n') {
      return -1;
    }

    if (i < max_out_len) {
      out[i++] = buffer[*pos];
    }

    (*pos)++;
  }

  if (*pos >= input_len) {
    return -1;
  }

  if (buffer[*pos] == '\r') {
    if ((*pos + 1) >= input_len || buffer[*pos + 1] != '\n') {
      return -1;
    }
  }

  out[i] = '\0';
  (*pos) += 2;
  return 0;
}

int parse_resp_request(Client *client) {
  char *buffer = client->query_buffer;
  int len = client->query_length;
  if (len <= 0)
    return 0;

  if (client->arg_values == NULL) {
    client->arg_values_cap = 4;
    if ((client->arg_values =
             malloc(sizeof(char *) * client->arg_values_cap)) == NULL)
      return -1;
  }

  client->arg_count = 0;

  if (buffer[0] != '*') {
    char *p = buffer;
    char *end = buffer + len;

    while (p < end) {
      while (p < end && *p != ' ')
        p++;
      if (p == end)
        break;

      char *token_start = p;
      while (p < end && *p != ' ' && *p != '\r' && *p != '\n')
        p++;

      ssize_t token_len = p - token_start;
      if (token_len == 0)
        continue;

      if (client->arg_count > client->arg_values_cap) {
        client->arg_values_cap *= 2;
        client->arg_values = realloc(client->arg_values,
                                     sizeof(char *) * client->arg_values_cap);
      }

      client->arg_values[client->arg_count] = malloc(token_len + 1);
      memcpy(client->arg_values[client->arg_count], token_start, token_len);
      client->arg_values[client->arg_count][token_len] = '\0';
      client->arg_count++;
    }
    return 1;
  }

  int pos = 1;
  char num_str[32];
  if (read_until_crlf(buffer, len, &pos, num_str, 32) == -1)
    return 0;

  int num_args = atoi(num_str);
  if (num_args < 0)
    return -1;

  if (num_args > client->arg_values_cap) {
    client->arg_values_cap = num_args;
    client->arg_values =
        realloc(client->arg_values, sizeof(char *) * client->arg_values_cap);
  }

  for (int i = 0; i < num_args; i++) {
    if (pos >= len)
      return 0;

    if (buffer[pos] != '$')
      return -1;
    pos++;

    char len_str[32];
    if (read_until_crlf(buffer, len, &pos, len_str, 32) == -1)
      return 0;

    int arg_len = atoi(len_str);
    if (pos + arg_len + 2 > len)
      return 0;

    client->arg_values[client->arg_count] = malloc(arg_len + 1);
    memcpy(client->arg_values[client->arg_count], buffer + pos, arg_len);
    client->arg_values[client->arg_count][arg_len] = '\0';
    client->arg_count++;

    pos += arg_len + 2;
  }

  return 1;
}
