#include <stdlib.h>
#include <string.h>

#include "../include/parser.h"

// read until carriage return + new line
int read_until_crlf(const char *buffer, int *pos, char *out, int max_len) {
  int i = 0;
  while (buffer[*pos] != '\r' && buffer[*pos + 1] != '\n') {
    if (buffer[*pos] == '\0')
      return -1;

    if (i < max_len) {
      out[i++] = buffer[*pos];
    }

    (*pos)++;
  }
  out[i] = '\0';
  (*pos) += 2;
  return 0;
}

int parse_resp_request(char *buffer, int len, char **arg_values, int max_args) {
  if (buffer[0] != '*') {
    // fallback for old protocol (nc)

    int arg_count = 0;
    char *token = strtok(buffer, " \r\n");
    while (token && arg_count < max_args) {
      arg_values[arg_count++] = strdup(token);
      token = strtok(NULL, " \r\n");
    }

    return arg_count;
  }

  int pos = 1;
  char num_str[32];
  if (read_until_crlf(buffer, &pos, num_str, 32) == -1)
    return -1;

  int num_args = atoi(num_str);
  if (num_args > max_args)
    return -1;

  for (int i = 0; i < num_args; i++) {
    if(buffer[pos] != '$') return -1;
    pos++;

    char len_str[32];
    if(read_until_crlf(buffer, &pos, len_str, 32) == -1)
      return 0;

    int arg_len = atoi(len_str);

    arg_values[i] = malloc(arg_len + 1);
    memcpy(arg_values[i], buffer + pos, arg_len);
    arg_values[i][arg_len] = '\0';
    
    pos += arg_len +2;
  }

  return num_args;
}
