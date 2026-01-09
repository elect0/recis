#include <ctype.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>

#include "../include/parser.h"

static const char *skip_spaces(const char *s) {
  while (isspace((unsigned char)*s))
    s++;
  return s;
}

// read until carriage return + new line

size_t read_until_crlf(const char *buffer, size_t input_len, size_t *pos,
                       char *out, size_t max_out_len) {
  size_t p = *pos;
  size_t i = 0;

  while (p < input_len) {
    if (buffer[p] == '\r') {
      if (p + 1 < input_len && buffer[p + 1] == '\n') {
        out[i] = '\0';
        *pos = p + 2;
        return 1;
      } else {
        return -1;
      }
    } else if (buffer[p] == '\n') {
      return -1;
    }

    if (i < max_out_len - 1) {
      out[i++] = buffer[p];
    }
    p++;
  }

  return 0;
}

size_t parse_resp_request(Client *client, char *buffer, size_t len) {
  if (len == 0)
    return 0;

  if (client->arg_values == NULL) {
    client->arg_values_cap = 4;
    client->arg_values = malloc(sizeof(Bytes *) * client->arg_values_cap);
    if (client->arg_values == NULL)
      return -1;
  }
  client->arg_count = 0;

  if (buffer[0] != '*') {
    char *new_line = memchr(buffer, '\n', len);
    if (!new_line)
      return 0;

    size_t line_len = new_line - buffer + 1;
    size_t pos = 0;

    while (pos < line_len) {
      while (pos < line_len && buffer[pos] <= ' ')
        pos++;
      if (pos >= line_len)
        break;

      char *token_start = buffer + pos;
      size_t start_idx = pos;

      while (pos < line_len && buffer[pos] > ' ')
        pos++;
      size_t token_len = pos - start_idx;

      if (client->arg_count >= client->arg_values_cap) {
        client->arg_values_cap *= 2;
        client->arg_values = realloc(client->arg_values,
                                     sizeof(Bytes *) * client->arg_values_cap);
      }

      client->arg_values[client->arg_count++] =
          create_bytes_object(token_start, (uint32_t)token_len);
    }

    return line_len;
  }

  size_t pos = 1;
  char num_str[32];

  int rc = read_until_crlf(buffer, len, &pos, num_str, 32);
  if (rc == 0)
    return 0;
  if (rc == -1)
    return -1;

  int num_args = atoi(num_str);
  if (num_args < 0)
    return -1;

  if (num_args > client->arg_values_cap) {
    client->arg_values_cap = num_args;
    client->arg_values =
        realloc(client->arg_values, sizeof(Bytes *) * client->arg_values_cap);
  }

  for (int i = 0; i < num_args; i++) {
    if (pos >= len)
      return 0;

    if (buffer[pos] != '$')
      return -1;
    pos++;

    char len_str[32];
    rc = read_until_crlf(buffer, len, &pos, len_str, 32);
    if (rc == 0)
      return 0;
    if (rc == -1)
      return -1;

    int arg_len = atoi(len_str);
    if (arg_len < 0)
      return -1;

    if (pos + arg_len + 2 > len)
      return 0;

    client->arg_values[client->arg_count++] =
        create_bytes_object(buffer + pos, (uint32_t)arg_len);

    pos += arg_len + 2;
  }

  return pos;
}

Vector *parse_vector(const char *data, uint32_t expected_dimension) {
  if (data == NULL)
    return NULL;

  const char *p = data;

  p = skip_spaces(p);
  if (*p != '[')
    return NULL;
  p++;

  float *temp_data;
  if ((temp_data = (float *)malloc(expected_dimension * sizeof(float))) == NULL)
    return NULL;

  uint32_t count = 0;

  while(*p != '\0'){
    p = skip_spaces(p);

    if(*p == ']') {
      p++;
      break;
    }

    if(count >= expected_dimension){
      free(temp_data);
      return NULL;
    }

    char *end_ptr;
    float val = strtof(p, &end_ptr);

    if(p == end_ptr){
      free(temp_data);
      return NULL;
    }

    temp_data[count++] = val;
    p = end_ptr;

    p = skip_spaces(p);
    if(*p == ','){
      p++;
    } else if(*p == ']'){

    } else {
      free(temp_data);
      return NULL;
    }
  }

  if(count != expected_dimension){
    free(temp_data);
    return NULL;
  }

  Vector *v = vector_create(expected_dimension, temp_data);
  free(temp_data);
  return v;
}
