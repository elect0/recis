CC = gcc
CFLAGS = -Wall -Wextra -I include -march=native -O3 -lm

TARGET = server

SRC = $(wildcard src/*.c)

all:
				$(CC) $(CFLAGS) $(SRC) -o $(TARGET)

clean:
				rm -f $(TARGET)
