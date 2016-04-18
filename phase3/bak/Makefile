#TARGETS = YARNTetrischedService_client Ultimate_server
TARGETS = Ultimate_server
#HPPFILES = tetrisched_constants.h TetrischedService.h tetrisched_types.h YARNTetrischedService.h inter.h
HPPFILES = inter.h
#OBJS = tetrisched_constants.o TetrischedService.o tetrisched_types.o YARNTetrischedService.o
OBJS = 
CC = g++
CFLAGS = -Wall -Werror -DDEBUG -g # debug flags
#CFLAGS = -Wall -Werror -Os # release flags
#LDFLAGS += -lthrift

default:	Ultimate_server
all:		$(TARGETS)

YARNTetrischedService_client:	$(OBJS) YARNTetrischedService_client.o
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

Ultimate_server:	$(OBJS) Ultimate_server.o Cluster.o MyJob.o MyMachine.o
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

%.o: %.cpp $(HPPFILES)
	$(CC) $(CFLAGS) -c -o $@ $<

clean:
	-rm $(TARGETS) *.o *.class
