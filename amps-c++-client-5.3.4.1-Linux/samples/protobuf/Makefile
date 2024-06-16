CC	= g++
CFLAGS	= -Wall -g -Werror -Wno-unused-variable -I../include -std=c++0x -I$(PROTOBUF_HOME)/include  -L$(PROTOBUF_HOME)/lib 
SOURCES	= $(shell echo *.cpp)
PROGRAMS	= $(SOURCES:.cpp=)
PROTOSOURCES       = $(shell echo *.cc)
PROTOLIBS = $(PROTOSOURCES:.cc=)
PROTOLINK = $(PROTOSOURCES:.cc=.a)
LIBS	= -L../lib/x86_64/release -lamps -lpthread -lrt -lprotobuf
RPATH = -Wl,-rpath=$(PROTOBUF_HOME)/lib

all: check-env $(PROTOLIBS) $(PROGRAMS)

.PHONY: check-env

check-env:
ifndef PROTOBUF_HOME
	$(error PROTOBUF_HOME is not defined -- please set this environment variable to the root directory of the protobuf installation.)
endif

$(PROTOLIBS): % : %.cc
	echo $@
	$(CC) $(CFLAGS) -o $@.a -c $< 

$(PROGRAMS): % : %.cpp
	$(CC) $(CFLAGS) $(RPATH) -o $@ $< $(LIBS) $(PROTOLINK)

clean:
	rm $(PROGRAMS)
