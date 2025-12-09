#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#define PORT 6379
#define BUFFER_SIZE 1024

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

  printf("Sever listening on port %d...\n", PORT);

  for (;;) {
    struct sockaddr_in client_addr;
    socklen_t client_len = sizeof(client_addr);

    int client_fd =
        accept(server_fd, (struct sockaddr *)&client_addr, &client_len);
    if (client_fd < 0) {
      perror("Accept failed");
      continue;
    }

    printf("Client connnected: %s\n", inet_ntoa(client_addr.sin_addr));

    char buffer[BUFFER_SIZE] = {0};
    ssize_t valread = read(client_fd, buffer, BUFFER_SIZE - 1);

    if (valread > 0) {
      printf("Received: %s\n", buffer);
      char *msg = "+OK\r\n";
      write(client_fd, msg, strlen(msg));
    }
    close(client_fd);
    printf("Client disconnected.\n");
  }

  close(server_fd);
  return 0;
}
