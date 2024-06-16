ifeq ("$(PROCESSOR)","")
PROCESSOR = $(shell uname -m)
endif

ifeq ("$(PROCESSOR)","unknown")
PROCESSOR = x86_64
endif

ifeq ("$(PROCESSOR)","x86")
ARCH=-m32 -march=pentium2
endif

ifeq ("$(PROCESSOR)","x86_64")
ARCH=-m64
endif

TARGETDIR=../lib/$(PROCESSOR)/debug

ifeq ("$(SHARED)","1")
PIC=1
TARGET=$(TARGETDIR)/libamps.so
else
TARGET=$(TARGETDIR)/libamps.a
endif

ifeq ("$(PIC)","1")
PICFLAG = -fPIC
endif

C_STD_VAL=gnu11

ifneq ("$(AMPS_C_STD)","")
C_STD_VAL=$(AMPS_C_STD)
endif

CPP_STD_VAL=c++11

ifneq ("$(AMPS_CPP_STD)","")
CPP_STD_VAL=$(AMPS_CPP_STD)
endif

ifeq ("$(AMPS_SSE42)","1")
SSE42_FLAG=-msse4.2
endif
ifeq ("$(PROCESSOR)","arm64")
SSE42_FLAG=
endif
ifeq ("$(PROCESSOR)","aarch64")
SSE42_FLAG=
endif

ifeq ("$(AMPS_THREAD_SANITIZE)","1")
SANITIZE_FLAGS = -fsanitize=thread
endif
ifeq ("$(SANITIZE)","")
SANITIZE_FLAGS=
else
SANITIZE_FLAGS=-fsanitize=address -fno-omit-frame-pointer
endif


all: $(TARGET)

.PHONY: all
objects = transports.o tcp.o tcps.o unix.o client.o message.o amps_protocol.o amps_ssl.o uri.o atfork.o
DEBUGOPTS =  -g
OPTIMIZER = -O3
ifneq ("$(AMPS_DEBUG)","")
	OPTIMIZER = -O0
endif

ifneq ("$(shell uname)","SunOS")
EXTRA_CFLAGS=-Wall -Wconversion -std=$(C_STD_VAL) -Wsign-compare
EXTRA_CXXFLAGS=-Wall -Wconversion -pedantic -Wsign-compare -Wunused-parameter
endif

CFLAGS = $(PICFLAG) -I ../include $(DEBUGOPTS) $(OPTIMIZER) $(ARCH) -std=$(C_STD_VAL) $(SSE42_FLAG) $(SANITIZE_FLAGS) $(EXTRA_CFLAGS)
CXXFLAGS = $(PICFLAG) -I ../include $(DEBUGOPTS) $(OPTIMIZER) $(ARCH) -std=$(CPP_STD_VAL) $(SSE42_FLAG) $(SANITIZE_FLAGS) $(EXTRA_CXXFLAGS)

$(TARGETDIR)/libamps.a: $(objects)
	mkdir -p $(TARGETDIR) >/dev/null
	ar rcs $(TARGET) $(objects)

$(TARGETDIR)/libamps.so: $(objects)
	mkdir -p $(TARGETDIR) >/dev/null
	$(CC) -shared -o $(TARGETDIR)/libamps.so $(objects)

clean:
	rm -rf *.o
	rm -rf $(TARGETDIR)/libamps.a
	rm -rf $(TARGETDIR)/libamps.so

