CC = gcc
CFLAGS = -Wall -Wextra -I include

TARGET = server

SRC = $(wildcard src/*.c)

all:
				$(CC) $(CFLAGS) $(SRC) -o $(TARGET)

clean:
				rm -f $(TARGET)
