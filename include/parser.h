#ifndef PARSER_H
#define PARSER_H

#include "client.h"
#include "vector.h"

size_t read_until_crlf(const char *buffer, size_t input_len, size_t *pos, char *out,
                    size_t max_out_len);
/* int parse_resp_request(char *buffer, int len, char **arg_values, int
 * max_args); */
size_t parse_resp_request(Client *client, char *buffer, size_t len);
Vector *parse_vector(const char *data, uint32_t expected_dimension);

#endif // !PARSER_H
