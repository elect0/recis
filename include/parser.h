#ifndef PARSER_H
#define PARSER_H

#include "client.h"

size_t read_until_crlf(const char *buffer, size_t input_len, size_t *pos, char *out,
                    size_t max_out_len);
/* int parse_resp_request(char *buffer, int len, char **arg_values, int
 * max_args); */
size_t parse_resp_request(Client *client, char *buffer, size_t len);

#endif // !PARSER_H
