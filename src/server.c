#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#include "../include/list.h"
#include "../include/parser.h"
#include "../include/redis.h"
#include "../include/set.h"

#define PORT 6379
#define BUFFER_SIZE 1024
#define MAX_EVENTS 10

void set_nonblocking(int fd) {
  int flags = fcntl(fd, F_GETFL, 0);
  if (flags == -1) {
    perror("fcntl F_GETFL");
    return;
  }

  if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) == -1) {
    perror("fcntl F_SETFL");
  }
}

long long get_time_ms() { return (long long)time(NULL); }

void active_expire_cycle(HashTable *db, HashTable *expires) {
  for (int i = 0; (size_t)i < expires->size; i++) {
    Node *node = expires->buckets[i];
    while (node) {
      Node *next = node->next;

      long long *kill_time = (long long *)node->value->data;
      if (get_time_ms() > *kill_time) {
        printf("Active expiry: Deleting '%s'\n", node->key);
        hash_table_del(db, node->key);
        hash_table_del(expires, node->key);
      }

      node = next;
    }
  }
}

int main() {
  int server_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (server_fd == 0) {
    perror("Socket failed");
    exit(EXIT_FAILURE);
  }

  struct sockaddr_in server_addr;

  server_addr.sin_family = AF_INET;
  server_addr.sin_addr.s_addr = INADDR_ANY;
  server_addr.sin_port = htons(PORT);

  if (bind(server_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) <
      0) {
    perror("Bind failed, port is already used");
    close(server_fd);
    exit(EXIT_FAILURE);
  }

  if (listen(server_fd, 5) < 0) {
    perror("Listen failed");
    close(server_fd);
    exit(EXIT_FAILURE);
  }

  HashTable *db = hash_table_create(1024);
  HashTable *expires = hash_table_create(1024);

  set_nonblocking(server_fd);

  int epfd = epoll_create1(0); // 0 = no flags, or use EPOLL_CLOEXEC
  if (epfd == -1) {
    perror("epoll_create1");
    return 1;
  }

  printf("epoll instance created: %d\n", epfd);

  struct epoll_event ev;
  ev.events = EPOLLIN;
  ev.data.fd = server_fd;

  if (epoll_ctl(epfd, EPOLL_CTL_ADD, server_fd, &ev) == -1) {
    perror("epoll_ctl: server_socket");
    return 1;
  }

  struct epoll_event events[MAX_EVENTS];

  while (1) {
    int nfds = epoll_wait(epfd, events, MAX_EVENTS, -1);
    if (nfds == -1) {
      perror("epoll_wait");
      break;
    }

    // check TTL
    active_expire_cycle(db, expires);

    for (int n = 0; n < nfds; ++n) {
      int current_fd = events[n].data.fd;

      if (current_fd == server_fd) {
        struct sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);

        int client_fd =
            accept(server_fd, (struct sockaddr *)&client_addr, &client_len);

        if (client_fd == -1) {
          perror("accept");
          continue;
        }

        set_nonblocking(client_fd);

        struct epoll_event ev;
        ev.events = EPOLLIN;
        ev.data.fd = client_fd;
        if (epoll_ctl(epfd, EPOLL_CTL_ADD, client_fd, &ev) == -1) {
          perror("epoll_ctl: client");
          close(client_fd);
        }

        printf("New client connected from: FD %d\n", client_fd);
      } else {

        char buffer[BUFFER_SIZE] = {0};
        ssize_t valread = read(current_fd, buffer, BUFFER_SIZE - 1);

        if (valread > 0) {
          buffer[valread] = '\0';

          char *arg_values[10] = {0};

          int arg_count = parse_resp_request(buffer, valread, arg_values, 10);

          if (arg_count > 0) {

            printf("%d", arg_count);
            if (strcasecmp(arg_values[0], "SET") == 0) {
              if (arg_count >= 3) {
                r_obj *o = create_string_object(arg_values[2]);
                hash_table_set(db, arg_values[1], o);

                if (arg_count >= 5 && strcasecmp(arg_values[3], "EX") == 0) {
                  long long t = get_time_ms() + atoll(arg_values[4]);
                  hash_table_set(expires, arg_values[1], create_int_object(t));
                }

                write(current_fd, "+OK\r\n", 5);
              } else {
                write(current_fd, "-ERR args\r\n", 11);
              }
            } else if (strcasecmp(arg_values[0], "GET") == 0) {
              if (arg_count >= 2) {
                r_obj *exp_obj = hash_table_get(expires, arg_values[1]);
                if (exp_obj) {
                  long long *kill_time = (long long *)exp_obj->data;
                  if (get_time_ms() > *kill_time) {
                    hash_table_del(db, arg_values[1]);
                    hash_table_del(expires, arg_values[1]);

                    write(current_fd, "$-1\r\n", 5);
                    continue;
                  }
                }
                r_obj *o = hash_table_get(db, arg_values[1]);

                if (o == NULL) {
                  write(current_fd, "$-1\r\n", 5);
                } else if (o->type == STRING) {
                  char *val = (char *)o->data;
                  int len = strlen(val);

                  char header[32];
                  sprintf(header, "$%d\r\n", len);

                  write(current_fd, header, strlen(header));
                  write(current_fd, val, len);
                  write(current_fd, "\r\n", 2);
                } else if (o->type == LIST) {
                  List *list = (List *)o->data;

                  size_t total_len = 2;

                  ListNode *node = list->head;
                  while (node != NULL) {
                    char *val = (char *)node->value;
                    total_len += strlen(val);

                    if (node->next) {
                      total_len += 2; // add 2 bytes for comma (", ") separator
                    }
                    node = node->next;
                  }

                  char *output_str =
                      malloc(total_len + 1); //  + 1 for null termiantor
                  if (output_str == NULL) {
                    perror("malloc failed in GET list");
                    return 1;
                  }

                  char *ptr = output_str;
                  *ptr++ = '[';

                  node = list->head;
                  while (node != NULL) {
                    char *val = (char *)node->value;
                    size_t vlen = strlen(val);

                    memcpy(ptr, val, vlen);
                    ptr += vlen;

                    if (node->next) {
                      *ptr++ = ',';
                      *ptr++ = ' ';
                    }

                    node = node->next;
                  }

                  *ptr++ = ']';
                  *ptr++ = '\0';

                  char header[32];
                  sprintf(header, "$%lu\r\n", total_len);

                  write(current_fd, header, strlen(header));
                  write(current_fd, output_str, total_len);
                  write(current_fd, "\r\n", 2);

                  free(output_str);
                }
              } else {
                write(current_fd, "-ERR args\r\n", 11);
              }
            } else if (strcasecmp(arg_values[0], "LPUSH") == 0) {
              if (arg_count >= 3) {
                r_obj *o = hash_table_get(db, arg_values[1]);

                if (!o) {
                  o = create_list_object();
                  hash_table_set(db, arg_values[1], o);
                }

                if (o->type != LIST) {
                  char *msg = "-WRONGTYPE Operation against a key holding "
                              "the wrong kind of value\r\n";
                  write(current_fd, msg, strlen(msg));
                } else {
                  list_ins_node_head((List *)o->data, strdup(arg_values[2]));

                  write(current_fd, "+OK\r\n", 5);
                }
              } else {
                write(current_fd, "-ERR args\r\n", 11);
              }
            } else if (strcasecmp(arg_values[0], "RPOP") == 0) {
              if (arg_count >= 2) {
                r_obj *o = hash_table_get(db, arg_values[1]);

                if (!o) {
                  write(current_fd, "$-1\r\n", 5);
                } else if (o->type != LIST) {
                  char *msg = "-WRONGTYPE Operation against a key holding "
                              "the wrong kind of value\r\n";
                  write(current_fd, msg, strlen(msg));
                } else {
                  List *list = (List *)o->data;
                  char *val = (char *)list_pop_tail(list);

                  if (val) {
                    int len = strlen(val);

                    char header[32];
                    sprintf(header, "$%d\r\n", len);

                    write(current_fd, header, strlen(header));
                    write(current_fd, val, strlen(val));
                    write(current_fd, "\r\n", 2);

                    free(val);
                  } else {
                    write(current_fd, "$-1\r\n", 5);
                  }
                }
              }
            } else if (strcasecmp(arg_values[0], "SADD") == 0) {
              if (arg_count >= 3) {
                r_obj *o = hash_table_get(db, arg_values[1]);

                if (o == NULL) {
                  o = create_set_object();
                  hash_table_set(db, arg_values[1], o);
                }

                if (o->type != SET) {
                  char *msg = "-WRONGTYPE Operation against a key holding "
                              "the wrong kind of value\r\n";
                  write(current_fd, msg, strlen(msg));
                } else {
                  int added =
                      set_add((HashTable *)o->data, strdup(arg_values[2]));

                  char resp[32];
                  sprintf(resp, ":%d\r\n", added);
                  write(current_fd, resp, strlen(resp));
                }
              } else {
                write(current_fd, "-ERR args\r\n", 11);
              }
            } else if (strcasecmp(arg_values[0], "SISMEMBER") == 0) {
              if (arg_count >= 3) {
                r_obj *o = hash_table_get(db, arg_values[1]);

                if (!o) {
                  write(current_fd, ":0\r\n", 4);
                } else if (o->type != SET) {
                  char *err = "-WRONGTYPE Operation against a key holding the "
                              "wrong kind of value\r\n";
                  write(current_fd, err, strlen(err));
                } else {
                  int is_member =
                      set_is_member((HashTable *)o->data, arg_values[2]);
                  char resp[32];
                  sprintf(resp, ":%d\r\n", is_member);
                  write(current_fd, resp, strlen(resp));
                }
              } else {
                write(current_fd, "-ERR args\r\n", 11);
              }
            } else if (strcasecmp(arg_values[0], "PING") == 0) {
              write(current_fd, "+PONG\r\n", 7);
            } else {
              char err_msg[64];
              snprintf(err_msg, sizeof(err_msg),
                       "-ERR unknown command '%s'\r\n", arg_values[0]);
              write(current_fd, err_msg, strlen(err_msg));
            }
            printf("Received command: %s\n", arg_values[0]);

            for (int i = 0; i < arg_count; i++) {
              free(arg_values[i]);
            }
          } else {
            char *err = "-ERR protocol error or incomplete packet\r\n";
            write(current_fd, err, strlen(err));
          }
        } else {
          epoll_ctl(epfd, EPOLL_CTL_DEL, current_fd, NULL);
          close(current_fd);
          printf("client dissconnected: FD :%d", current_fd);
        }
      }
    }
  }

  close(server_fd);
  return 0;
}
