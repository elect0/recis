#ifndef PARSER_H
#define PARSER_H

int read_until_crlf(const char *buffer, int *pos, char *out, int max_len);
int parse_resp_request(char *buffer, int len, char **arg_values, int max_args);

#endif // !PARSER_H
