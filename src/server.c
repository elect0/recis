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
#include "../include/redis.h"

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
          printf("Received: %s\n", buffer);
          char *command = strtok(buffer, " \r\n");
          if (command == NULL)
            continue;

          if (strcmp(command, "SET") == 0) {
            char *key = strtok(NULL, " \r\n");
            char *value = strtok(NULL, "\"");
            char *flag = strtok(NULL, " \r\n");
            char *seconds = strtok(NULL, " \r\n");

            if (key == NULL || value == NULL) {
              char *err = "Error: wrong number of arguments\r\n";
              write(current_fd, err, strlen(err));
            } else {
              r_obj *o = create_string_object(value);
              hash_table_set(db, key, o);

              if (flag && strcmp(flag, "EX") == 0 && seconds) {
                long long seconds_int = atoll(seconds);
                long long dead_at = get_time_ms() + seconds_int;

                hash_table_set(expires, key, create_int_object(dead_at));
              }
              char *ok = "+OK\r\n";
              write(current_fd, ok, strlen(ok));
            }
          } else if (strcmp(command, "GET") == 0) {
            char *key = strtok(NULL, " \r\n");
            if (key == NULL) {
              char *err = "Error: wrong number of arguments\r\n";
              write(current_fd, err, strlen(err));
            } else {
              r_obj *exp_obj = hash_table_get(expires, key);
              if (exp_obj) {
                long long *kill_time = (long long *)exp_obj->data;
                if (get_time_ms() > *kill_time) {
                  hash_table_del(db, key);
                  hash_table_del(expires, key);

                  char *msg = "(nil)\r\n";
                  write(current_fd, msg, strlen(msg));
                  continue;
                }
              }

              r_obj *o = hash_table_get(db, key);

              if (o == NULL) {
                char *msg =
                    "Error: Couldn't find your value based on the key\r\n";
                write(current_fd, msg, strlen(msg));
              } else if (o->type == STRING) {
                char *val = (char *)o->data;
                write(current_fd, val, strlen(val));
                write(current_fd, "\r\n", 2);
              } else if (o->type == LIST) {
                List *list = (List *)o->data;

                write(current_fd, "[", 1);

                ListNode *node = list->head;
                while (node) {
                  char *value = (char *)node->value;
                  write(current_fd, value, strlen(value));

                  if (node->next)
                    write(current_fd, ", ", 2);
                  node = node->next;
                }
                write(current_fd, "]\r\n", 3);
              } else {
                char *err = "-ERR unknown type\r\n";
                write(current_fd, err, strlen(err));
              }
            }
          } else if (strcmp(command, "LPUSH") == 0) {
            char *key = strtok(NULL, " \r\n");
            char *value = strtok(NULL, " \r\n");

            if (!key || !value) {
              char *msg = "-ERR args\r\n";
              write(current_fd, msg, strlen(msg));
            } else {
              r_obj *o = hash_table_get(db, key);

              if (!o) {
                o = create_list_object();
                hash_table_set(db, key, o);
              }

              if (o->type != LIST) {
                char *msg = "-WRONGTYPE\r\n";
                write(current_fd, msg, strlen(msg));
              } else {
                list_ins_node_head((List *)o->data, strdup(value));

                char *msg = "+OK\r\n";
                write(current_fd, msg, strlen(msg));
              }
            }
          } else if (strcmp(command, "RPOP") == 0) {
            char *key = strtok(NULL, " \r\n");

            if (!key) {
              char *msg = "-ERR args\r\n";
              write(current_fd, msg, strlen(msg));
            } else {
              r_obj *o = hash_table_get(db, key);

              if (!o) {
                char *msg = "(nil)\r\n";
                write(current_fd, msg, strlen(msg));
              }

              if (o->type != LIST) {
                char *msg = "-WRONGTYPE\r\n";
                write(current_fd, msg, strlen(msg));
              } else {
                List *list = (List *)o->data;
                char *val = (char *)list_pop_tail(list);

                if (val) {
                  write(current_fd, val, strlen(val));
                  write(current_fd, "\r\n", 2);

                  free(val);
                } else {
                  char *msg = "(nil)\r\n";
                  write(current_fd, msg, strlen(msg));
                }
              }
            }
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
