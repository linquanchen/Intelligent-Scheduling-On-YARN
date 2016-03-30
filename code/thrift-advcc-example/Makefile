TARGETS = TetrischedService_server YARNTetrischedService_client schedpolserver
HPPFILES = tetrisched_constants.h TetrischedService.h tetrisched_types.h YARNTetrischedService.h
OBJS = tetrisched_constants.o TetrischedService.o tetrisched_types.o YARNTetrischedService.o
CC = g++
CFLAGS = -Wall -Werror -DDEBUG -g # debug flags
#CFLAGS = -Wall -Werror -Os # release flags
LDFLAGS += -lthrift

default:	$(TARGETS)
all:		$(TARGETS)

TetrischedService_server:	$(OBJS) TetrischedService_server.o
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

YARNTetrischedService_client:	$(OBJS) YARNTetrischedService_client.o
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

schedpolserver:	$(OBJS) schedpolserver.o
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

%.o: %.cpp $(HPPFILES)
	$(CC) $(CFLAGS) -c -o $@ $<

clean:
	-rm $(TARGETS) *.o *.class
