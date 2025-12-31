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

#include "../include/command.h"
#include "../include/list.h"
#include "../include/networking.h"
#include "../include/parser.h"
#include "../include/persistance.h"
#include "../include/redis.h"
#include "../include/set.h"
#include "../include/zset.h"

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
  if (server_fd < 0) {
    perror("Socket failed");
    exit(EXIT_FAILURE);
  }

  int opt = 1;
  if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
    perror("setsockopt(SO_REUSEADDR)");
    close(server_fd);
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
  HashTable *cmd_registry = hash_table_create(32);

  populate_command_table(cmd_registry);

  rdb_load(db, expires, "dump.rdb");

  set_nonblocking(server_fd);

  int epfd = epoll_create1(0); // 0 = no flags, or use EPOLL_CLOEXEC
  if (epfd == -1) {
    perror("epoll_create1");
    return 1;
  }

  Client *server_client = create_client(server_fd);
  printf("epoll instance created: %d\n", epfd);

  struct epoll_event ev;
  ev.events = EPOLLIN | EPOLLET;
  ev.data.ptr = server_client;
  /* ev.data.fd = server_fd; */

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
      /* int current_fd = events[n].data.fd; */

      Client *c = (Client *)events[n].data.ptr;
      int current_fd = c->fd;

      if (c == server_client) {
        struct sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);

        int client_fd =
            accept(server_fd, (struct sockaddr *)&client_addr, &client_len);

        if (client_fd == -1) {
          perror("accept");
          continue;
        }

        set_nonblocking(client_fd);

        Client *new_client = create_client(client_fd);

        struct epoll_event ev;
        ev.events = EPOLLIN;
        ev.data.ptr = new_client;

        if (epoll_ctl(epfd, EPOLL_CTL_ADD, client_fd, &ev) == -1) {
          perror("epoll_ctl: client");
          close(client_fd);
        }

        printf("New client connected from: FD %d\n", client_fd);
      } else {

        if (read_from_client(c) != 0) {
          epoll_ctl(epfd, EPOLL_CTL_DEL, current_fd, NULL);
          close(current_fd);
          printf("client dissconnected: FD :%d", current_fd);
          continue;
        }

        if (parse_resp_request(c) == 1) {
          if (c->arg_count > 0) {
            char **arg_values = c->arg_values;
            int arg_count = c->arg_count;
            OutputBuffer *ob = c->output_buffer;

            char *cmd_name = arg_values[0];
            r_obj *cmd_obj = hash_table_get(cmd_registry, cmd_name);

            if (cmd_obj == NULL || cmd_obj->type != COMMAND) {
              append_to_output_buffer(ob, "-ERR unknown command\r\n", 22);
            } else {
              Command *cmd = (Command *)cmd_obj->data;

              cmd->proc(c, db, expires, ob);
            }
          }
          reset_client_args(c);
        }

        flush_buffer(c->output_buffer);
      }
    }
  }

  close(server_fd);
  return 0;
}
