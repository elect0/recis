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

long long get_time_ms() { return (long long)time(NULL) * 1000; }

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

  rdb_load(db, expires, "dump.rdb");

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
            if (strcasecmp(arg_values[0], "SET") == 0) {
              if (arg_count >= 3) {
                int flags = OBJ_SET_NO_FLAGS;
                long long expire_at = -1;

                char *ifeq_val = NULL;
                char *ifne_val = NULL;

                int j;
                for (j = 3; j < arg_count; j++) {
                  char *option = arg_values[j];

                  if (strcasecmp(option, "NX") == 0) {
                    flags |= OBJ_SET_NX;
                  } else if (strcasecmp(option, "XX") == 0) {
                    flags |= OBJ_SET_XX;
                  } else if (strcasecmp(option, "GET") == 0) {
                    flags |= OBJ_SET_GET;
                  } else if (strcasecmp(option, "KEEPTTL") == 0) {
                    flags |= OBJ_SET_KEEPTTL;
                  } else if (strcasecmp(option, "EX") == 0 &&
                             j + 1 < arg_count) {
                    long long value = atoll(arg_values[++j]);
                    if (value <= 0) {
                      write(current_fd, "-ERR invalid expire time\r\n", 26);
                    }
                    expire_at = get_time_ms() + (value * 1000);
                  } else if (strcasecmp(option, "PX") == 0 &&
                             j + 1 < arg_count) {
                    long long value = atoll(arg_values[++j]);
                    if (value <= 0) {
                      write(current_fd, "-ERR invalid expire time\r\n", 26);
                    }
                    expire_at = get_time_ms() + value;
                  } else if (strcasecmp(option, "EXAT") == 0 &&
                             j + 1 < arg_count) {
                    long long value = atoll(arg_values[++j]);
                    if (value <= 0) {
                      write(current_fd, "-ERR invalid expire time\r\n", 26);
                    }
                    expire_at = value * 1000;
                  } else if (strcasecmp(option, "PXAT") == 0 &&
                             j + 1 < arg_count) {
                    long long value = atoll(arg_values[++j]);
                    if (value <= 0) {
                      write(current_fd, "-ERR invalid expire time\r\n", 26);
                    }
                    expire_at = value;
                  } else if (strcasecmp(option, "IFEQ") == 0 &&
                             j + 1 < arg_count) {
                    flags |= OBJ_SET_IFEQ;
                    ifeq_val = arg_values[++j];
                  } else if (strcasecmp(option, "IFNE") == 0 &&
                             j + 1 < arg_count) {
                    flags |= OBJ_SET_IFNE;
                    ifne_val = arg_values[++j];
                  } else {
                    write(current_fd, "-ERR syntax error\r\n", 19);
                  }
                }

                if ((flags & OBJ_SET_NX) && (flags & OBJ_SET_XX)) {
                  write(current_fd, "-ERR syntax error\r\n", 19);
                }

                if ((flags & OBJ_SET_IFEQ) && (flags & OBJ_SET_IFNE)) {
                  write(current_fd, "-ERR syntax error\r\n", 19);
                }

                r_obj *o = hash_table_get(db, arg_values[1]);

                if ((flags & OBJ_SET_NX) && o != NULL) {
                  write(current_fd, "_\r\n", 3);
                }

                if ((flags & OBJ_SET_XX) && o == NULL) {
                  write(current_fd, "_\r\n", 3);
                }

                if (flags & OBJ_SET_IFEQ) {
                  if (o == NULL) {
                    write(current_fd, "_\r\n", 3);
                  }

                  if (o->type != STRING) {
                    write(current_fd, "_\r\n", 3);
                  }

                  if (strcmp((char *)o->data, ifeq_val) != 0) {
                    write(current_fd, "_\r\n", 3);
                  }
                }

                if (flags & OBJ_SET_IFNE) {
                  if (o != NULL) {
                    if (o->type == STRING &&
                        strcmp((char *)o->data, ifne_val) == 0) {
                      write(current_fd, "_\r\n", 3);
                    }
                  }
                }

                char resp_buf[256];
                int resp_len = 0;

                if (flags & OBJ_SET_GET) {
                  if (o == NULL) {
                    strcpy(resp_buf, "_\r\n");
                  } else if (o->type != STRING) {
                    write(current_fd,
                          "-WRONGTYPE Operation against a key holding the "
                          "wrong kind of value\r\n",
                          68);
                  } else {
                    char *old = (char *)o->data;
                    sprintf(resp_buf, "$%lu\r\n%s\r\n", strlen(old), old);
                  }
                } else {
                  strcpy(resp_buf, "+OK\r\n");
                }

                resp_len = strlen(resp_buf);

                if ((flags & OBJ_SET_KEEPTTL) && o != NULL && expire_at == -1) {
                  r_obj *ttl = hash_table_get(expires, arg_values[1]);
                  if (ttl != NULL) {
                    expire_at = *(long long *)ttl->data;
                  }
                }

                r_obj *new_obj = create_string_object(arg_values[2]);
                hash_table_set(db, arg_values[1], new_obj);

                if (expire_at != -1) {
                  hash_table_set(expires, arg_values[1],
                                 create_int_object(expire_at));
                } else {
                  hash_table_del(expires, arg_values[1]);
                }

                write(current_fd, resp_buf, resp_len);
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

                if (!o) {
                  write(current_fd, "_\r\n", 3);
                }
                if (o->type != STRING) {
                  write(current_fd,
                        "-WRONGTYPE Operation against a key holding the "
                        "wrong kind of value\r\n",
                        68);
                } else {
                  char *value = (char *)o->data;
                  char resp[256];
                  sprintf(resp, "$%lu\r\n%s\r\n", strlen(value), value);
                  write(current_fd, resp, strlen(resp));
                }

              } else {
                write(current_fd, "-ERR args\r\n", 11);
              }
            } else if (strcasecmp(arg_values[0], "DEL") == 0) {
              if (arg_count >= 2) {
                int deleted_count = 0;

                for (int i = 1; i < arg_count; i++) {
                  char *key = arg_values[i];
                  r_obj *o = hash_table_get(db, key);
                  hash_table_del(db, key);
                  deleted_count++;
                }

                char resp[32];
                sprintf(resp, ":%d\r\n", deleted_count);
                write(current_fd, resp, strlen(resp));

              } else {
                write(current_fd, "-ERR args\r\n", 11);
              }
            } else if (strcasecmp(arg_values[0], "TTL") == 0) {
              if (arg_count >= 2) {
                if (hash_table_get(db, arg_values[1]) == NULL) {
                  write(current_fd, ":-2\r\n", 5);
                } else {
                  r_obj *ttl = hash_table_get(expires, arg_values[1]);
                  if (ttl == NULL) {
                    write(current_fd, ":-1\r\n", 5);
                  } else if (ttl->data != NULL) {
                    char resp[32];

                    long long seconds =
                        (*(long long *)ttl->data - get_time_ms()) / 1000;
                    sprintf(resp, ":%lld\r\n", seconds);
                    write(current_fd, resp, strlen(resp));
                  }
                }
              }

            } else if (strcasecmp(arg_values[0], "LPUSH") == 0) {
              if (arg_count >= 3) {
                r_obj *o = hash_table_get(db, arg_values[1]);

                if (o != NULL && o->type != LIST) {
                  char *msg = "-WRONGTYPE Operation against a key holding "
                              "the wrong kind of value\r\n";
                  write(current_fd, msg, strlen(msg));
                }

                if (!o) {
                  o = create_list_object();
                  hash_table_set(db, arg_values[1], o);
                }

                int j;
                List *list = (List *)o->data;
                for (j = 2; j < arg_count; j++) {
                  list_ins_node_head(list, strdup(arg_values[j]));
                }
                char resp[64];
                snprintf(resp, sizeof(resp), ":%lu\r\n", list->size);
                write(current_fd, resp, strlen(resp));
              } else {
                write(current_fd, "-ERR args\r\n", 11);
              }
            } else if (strcasecmp(arg_values[0], "RPUSH") == 0) {
              if (arg_count >= 3) {
                r_obj *o = hash_table_get(db, arg_values[1]);

                if (o != NULL && o->type != LIST) {
                  char *msg = "-WRONGTYPE Operation against a key holding "
                              "the wrong kind of value\r\n";
                  write(current_fd, msg, strlen(msg));
                }

                if (!o) {
                  o = create_list_object();
                  hash_table_set(db, arg_values[1], o);
                }

                int j;
                List *list = (List *)o->data;
                for (j = 2; j < arg_count; j++) {
                  list_ins_node_tail(list, strdup(arg_values[j]));
                }

                char resp[64];
                snprintf(resp, sizeof(resp), ":%lu\r\n", list->size);
                write(current_fd, resp, strlen(resp));

              } else {
                write(current_fd, "-ERR args\r\n", 11);
              }
            } else if (strcasecmp(arg_values[0], "RPOP") == 0) {
              if (arg_count >= 2) {
                r_obj *o = hash_table_get(db, arg_values[1]);

                if (!o) {
                  write(current_fd, "_\r\n", 3);
                } else if (o->type != LIST) {
                  char *msg = "-WRONGTYPE Operation against a key holding "
                              "the wrong kind of value\r\n";
                  write(current_fd, msg, strlen(msg));
                } else {
                  List *list = (List *)o->data;

                  if (arg_count == 3) {
                    int count = atoi(arg_values[2]);

                    if (count > list->size) {
                      count = list->size;
                    }

                    char header[64];
                    snprintf(header, sizeof(header), "*%d\r\n", count);
                    write(current_fd, header, strlen(header));

                    for (int i = 0; i < count; i++) {
                      char *value = (char *)list_pop_tail(list);
                      char bulk_header[64];

                      snprintf(bulk_header, sizeof(bulk_header), "$%lu\r\n",
                               strlen(value));
                      write(current_fd, bulk_header, strlen(bulk_header));
                      write(current_fd, value, strlen(value));
                      write(current_fd, "\r\n", 2);

                      free(value);
                    }
                  } else {
                    char *value = (char *)list_pop_tail(list);

                    if (value) {
                      int len = strlen(value);

                      char bulk_header[64];
                      snprintf(bulk_header, sizeof(bulk_header), "$%d\r\n",
                               len);

                      write(current_fd, bulk_header, strlen(bulk_header));
                      write(current_fd, value, strlen(value));
                      write(current_fd, "\r\n", 2);

                      free(value);
                    } else {
                      write(current_fd, "$-1\r\n", 5);
                    }
                  }
                  if (list->size == 0) {
                    hash_table_del(db, arg_values[1]);
                  }
                }
              } else {
                write(current_fd, "-ERR args\r\n", 11);
              }
            } else if (strcasecmp(arg_values[0], "SADD") == 0) {
              if (arg_count >= 3) {
                r_obj *o = hash_table_get(db, arg_values[1]);

                if (o != NULL && o->type != SET) {
                  char *msg = "-WRONGTYPE Operation against a key holding "
                              "the wrong kind of value\r\n";
                  write(current_fd, msg, strlen(msg));
                  continue;
                }

                if (o == NULL) {
                  o = create_set_object();
                  hash_table_set(db, arg_values[1], o);
                }

                int j;
                int count = 0;

                HashTable *set = (HashTable *)o->data;

                for (j = 2; j < arg_count; j++) {
                  char *member = strdup(arg_values[j]);
                  if (set_add(set, member) == 1) {
                    count++;
                  } else {
                    free(member);
                  }
                }

                char resp[64];
                snprintf(resp, sizeof(resp), ":%d\r\n", count);
                write(current_fd, resp, strlen(resp));

              } else {
                write(current_fd, "-ERR args\r\n", 11);
              }
            } else if (strcasecmp(arg_values[0], "SISMEMBER") == 0) {
              if (arg_count >= 3) {
                r_obj *o = hash_table_get(db, arg_values[1]);

                if (o != NULL && o->type != SET) {
                  char *err = "-WRONGTYPE Operation against a key holding the "
                              "wrong kind of value\r\n";
                  write(current_fd, err, strlen(err));
                  continue;
                }

                if (!o) {
                  write(current_fd, ":0\r\n", 4);
                } else {
                  int is_member =
                      set_is_member((HashTable *)o->data, arg_values[2]);
                  char resp[64];
                  snprintf(resp, sizeof(resp), ":%d\r\n", is_member);
                  write(current_fd, resp, strlen(resp));
                }
              } else {
                write(current_fd, "-ERR args\r\n", 11);
              }
            } else if (strcasecmp(arg_values[0], "ZADD") == 0) {
              if (arg_count >= 4) {

                int flags = ZADD_SET_NO_FLAGS;
                int flags_count = 0;

                int j;
                for (j = 2; j < arg_count; j++) {
                  char *option = arg_values[j];

                  if (strcasecmp(option, "NX") == 0) {
                    flags |= ZADD_SET_NX;
                    flags_count++;
                  } else if (strcasecmp(option, "XX") == 0) {
                    flags |= ZADD_SET_XX;
                    flags_count++;
                  } else if (strcasecmp(option, "GT") == 0) {
                    flags |= ZADD_SET_GT;
                    flags_count++;
                  } else if (strcasecmp(option, "LT") == 0) {
                    flags |= ZADD_SET_LT;
                    flags_count++;
                  } else if (strcasecmp(option, "CH") == 0) {
                    flags |= ZADD_SET_CH;
                    flags_count++;
                  } else if (strcasecmp(option, "INCR") == 0) {
                    flags |= ZADD_SET_INCR;
                    flags_count++;
                  } else
                    break;
                }

                int nx = flags & ZADD_SET_NX;
                int xx = flags & ZADD_SET_XX;
                int gt = flags & ZADD_SET_GT;
                int lt = flags & ZADD_SET_LT;

                if ((nx && xx) || (gt && lt) || (gt && nx) || (lt && nx)) {
                  write(current_fd, "-ERR syntax error\r\n", 19);
                  continue;
                }

                int remaining_args = arg_count - j;
                if (remaining_args == 0 || remaining_args % 2 != 0) {
                  write(current_fd, "-ERR syntax error\r\n", 19);
                  continue;
                }

                if ((flags & ZADD_SET_INCR) && remaining_args != 2) {
                  char *err = "-ERR INCR option supports a single "
                              "increment-element pair\r\n";
                  write(current_fd, err, strlen(err));
                  continue;
                }

                r_obj *o = hash_table_get(db, arg_values[1]);

                if (o != NULL && o->type != ZSET) {
                  char *err = "-WRONGTYPE Operation against a key holding the "
                              "wrong kind of value\r\n";
                  write(current_fd, err, strlen(err));
                  continue;
                }

                if (!o) {
                  if (xx) {
                    if (flags & ZADD_SET_INCR)
                      write(current_fd, "$-1\r\n", 5);
                    else
                      write(current_fd, ":0\r\n", 4);
                    continue;
                  }
                  o = create_zset_object();
                  hash_table_set(db, arg_values[1], o);
                }

                ZSet *zs = (ZSet *)o->data;

                int added = 0;
                int changed = 0;
                int processed = 0;

                for (int i = j; i < arg_count; i++) {
                  double score = atof(arg_values[i]);
                  char *member = arg_values[++i];

                  if (flags & ZADD_SET_INCR) {
                    r_obj *score_o = hash_table_get(zs->dict, member);
                    double current_score = 0.0;

                    if (score_o != NULL) {
                      current_score = *(double *)o->data;
                    } else {
                      if (xx) {
                        write(current_fd, "_\r\n", 3);
                        continue;
                      }
                    }

                    double new_score = current_score + score;

                    zset_add(zs, member, new_score);
                    char num_str[64];
                    int len =
                        snprintf(num_str, sizeof(num_str), "%.17g", new_score);

                    char bulk_header[64];
                    snprintf(bulk_header, sizeof(bulk_header), "$%d\r\n", len);

                    write(current_fd, bulk_header, strlen(bulk_header));
                    write(current_fd, num_str, strlen(num_str));
                    write(current_fd, "\r\n", 2);
                    processed++;
                    continue;
                  }

                  r_obj *member_o = hash_table_get(zs->dict, member);

                  if (nx && member_o != NULL)
                    continue;
                  if (xx && member_o == NULL)
                    continue;

                  if (member_o != NULL) {
                    double current_score = *(double *)member_o->data;
                    if (gt && score <= current_score)
                      continue;
                    if (lt && score >= current_score)
                      continue;

                    zset_add(zs, member, score);
                    changed++;
                  } else {
                    zset_add(zs, member, score);
                    added++;
                  }
                }
                if (!(flags & ZADD_SET_INCR)) {
                  int ret_val = added;
                  if (flags & ZADD_SET_CH) {
                    ret_val = added + changed;
                  }

                  char resp[64];
                  snprintf(resp, sizeof(resp), ":%d\r\n", ret_val);
                  write(current_fd, resp, strlen(resp));
                }
              } else {
                write(current_fd, "-ERR args\r\n", 11);
              }
            } else if (strcasecmp(arg_values[0], "ZRANGE") == 0) {
              if (arg_count >= 4) {
                r_obj *o = hash_table_get(db, arg_values[1]);

                int start = atoi(arg_values[2]);
                int stop = atoi(arg_values[3]);

                if (!o) {
                  write(current_fd, "*0\r\n", 4);
                } else if (o->type != ZSET) {
                  char *err = "-WRONGTYPE Operation against a key holding the "
                              "wrong kind of value\r\n";
                  write(current_fd, err, strlen(err));

                } else {
                  ZSet *zs = (ZSet *)o->data;
                  ZSkipList *zsl = zs->zsl;

                  int len = zsl->length;

                  if (start < 0)
                    start = len + start;
                  if (stop < 0)
                    stop = len + stop;
                  if (start < 0)
                    start = 0;

                  if (start > stop || start >= len) {
                    write(current_fd, "*0\r\n", 4);
                  } else {
                    if (stop >= len)
                      stop = len - 1;

                    int range_len = stop - start + 1;

                    char header[32];
                    sprintf(header, "*%d\r\n", range_len);
                    write(current_fd, header, strlen(header));

                    ZSkipListNode *node = zsl_get_element_by_rank(zsl, start);

                    while (node && range_len > 0) {
                      // bulk string : "$len\r\nVal\r\n"

                      char bulk_header[32];
                      int val_len = strlen(node->element);
                      sprintf(bulk_header, "$%d\r\n", val_len);
                      write(current_fd, bulk_header, strlen(bulk_header));
                      write(current_fd, node->element, val_len);
                      write(current_fd, "\r\n", 2);

                      node = node->level[0].forward;
                      range_len--;
                    }
                  }
                }
              } else {
                write(current_fd, "-ERR args\r\n", 11);
              }
            } else if (strcasecmp(arg_values[0], "ZRANK") == 0) {
              if (arg_count >= 3) {
                r_obj *o = hash_table_get(db, arg_values[1]);
                if (!o) {
                  write(current_fd, "$-1\r\n", 5);
                } else if (o->type != ZSET) {
                  char *err = "-WRONGTYPE Operation against a key holding the "
                              "wrong kind of value\r\n";
                  write(current_fd, err, strlen(err));
                } else {
                  ZSet *zs = (ZSet *)o->data;

                  r_obj *score_o = hash_table_get(zs->dict, arg_values[2]);
                  if (!score_o) {
                    write(current_fd, "$-1\r\n", 5);
                  } else {
                    double score = *(double *)score_o->data;
                    printf("scoru : %lf\r\n", score);

                    unsigned long rank =
                        zsl_get_rank(zs->zsl, score, arg_values[2]);

                    if (rank > 0) {
                      rank--;
                      char resp[32];
                      sprintf(resp, ":%lu\r\n", rank);
                      write(current_fd, resp, strlen(resp));
                    } else {
                      write(current_fd, "$-1\r\n", 5);
                    }
                  }
                }
              } else {
                write(current_fd, "-ERR args\r\n", 11);
              }

            } else if (strcasecmp(arg_values[0], "ZSCORE") == 0) {
              if (arg_count >= 3) {
                r_obj *o = hash_table_get(db, arg_values[1]);
                if (o == NULL) {
                  write(current_fd, "$-1\r\n", 5);
                } else if (o->type != ZSET) {
                  char *err = "-WRONGTYPE Operation against a key holding the "
                              "wrong kind of value\r\n";
                  write(current_fd, err, strlen(err));
                } else {
                  ZSet *zs = (ZSet *)o->data;
                  r_obj *score_o = hash_table_get(zs->dict, arg_values[2]);
                  if (score_o == NULL) {
                    write(current_fd, "$-1\r\n", 5);
                  } else {
                    double score = *(double *)score_o->data;

                    char resp[128];
                    sprintf(resp, ",%.17g\r\n", score);
                    write(current_fd, resp, strlen(resp));
                  }
                }
              } else {
                write(current_fd, "-ERR args\r\n", 11);
              }
            } else if (strcasecmp(arg_values[0], "SAVE") == 0) {
              rdb_save(db, expires, "dump.rdb");
              char *resp = "+OK\r\n";
              write(current_fd, resp, strlen(resp));
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
