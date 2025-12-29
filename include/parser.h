#ifndef PARSER_H
#define PARSER_H

#include "client.h"

int read_until_crlf(const char *buffer, int input_len, int *pos, char *out,
                    int max_out_len);
/* int parse_resp_request(char *buffer, int len, char **arg_values, int
 * max_args); */
int parse_resp_request(Client *client);

#endif // !PARSER_H
