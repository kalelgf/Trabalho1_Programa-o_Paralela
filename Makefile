CC = gcc
CFLAGS = -Wall -pthread
TARGET = simulador

all: $(TARGET)

$(TARGET): main.c
	$(CC) $(CFLAGS) -o $(TARGET) main.c

clean:
	rm -f $(TARGET)
