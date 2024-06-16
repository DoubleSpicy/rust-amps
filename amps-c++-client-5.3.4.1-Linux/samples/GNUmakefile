OPTIMIZER= -O0
DEBUGOPTS=-g
AMPSCPPDIR=..

ifeq ("$(PROCESSOR)","")
PROCESSOR = $(shell uname -m)
endif
ifeq ("$(PROCESSOR)","unknown")
PROCESSOR = x86_64
endif

ifeq ($(PROCESSOR),x86)
ARCH=-m32 -march=pentium2
endif

ifeq ($(PROCESSOR),x86_64)
ARCH=-m64
endif

ifeq ("$(AMPS_DEBUG)","")
OPTIMIZER=-O3
LIBDIR=$(AMPSCPPDIR)/lib/${PROCESSOR}/release
else
LIBDIR=$(AMPSCPPDIR)/lib/${PROCESSOR}/debug
endif

C_STD_VAL=gnu11

ifneq ("$(AMPS_C_STD)","")
C_STD_VAL=$(AMPS_C_STD)
endif

CPP_STD_VAL=c++11

ifneq ("$(AMPS_CPP_STD)","")
CPP_STD_VAL=$(AMPS_CPP_STD)
endif

ifeq ("$(AMPS_SSE42)","")
SSE42_FLAG=
else ifeq ("$(PROCESSOR)","arm64")
SSE42_FLAG=
else ifeq ("$(PROCESSOR)","aarch64")
SSE42_FLAG=
else
SSE42_FLAG=-msse4.2
endif

ifneq ("$(AMPS_THREAD_SANITIZE)","")
SANITIZE_FLAGS = -fsanitize=thread
else ifeq ("$(SANITIZE)","")
SANITIZE_FLAGS=
else
SANITIZE_FLAGS=-fsanitize=address -fno-omit-frame-pointer
endif

ifeq ("$(shell uname)","SunOS")
OSLIBS=-lrt -ldl -lsocket -lnsl
else
EXTRA_FLAGS=-Wall -Wconversion -Wsign-compare
EXTRA_CFLAGS=$(EXTRA_FLAGS) -std=$(C_STD_VAL)
EXTRA_CXXFLAGS=$(EXTRA_FLAGS) -Wunused-parameter -pedantic -Wno-long-long
ifeq ("$(shell uname)","Linux")
OSLIBS=-lrt -ldl
else
OSLIBS=-ldl
endif
endif

CFLAGS=$(OPTIMIZER) $(DEBUGOPTS) $(ARCH) -I../include -std=$(C_STD_VAL) $(SSE42_FLAG) $(SANITIZE_FLAGS) $(EXTRA_CFLAGS)
CXXFLAGS=$(OPTIMIZER) $(DEBUGOPTS) $(ARCH) -I../include -std=$(CPP_STD_VAL) $(SSE42_FLAG) $(SANITIZE_FLAGS) $(EXTRA_CXXFLAGS)
LDFLAGS=-L$(LIBDIR)
LDLIBS=-lamps -lpthread $(OSLIBS)
SAMPLES = publish_and_sow disconnect logon subscribe sow functional ping \
    exceptions interop fix amps_consume_queue amps_fix_builder_publisher \
    amps_fix_shredder_subscriber amps_nvfix_builder_publisher amps_nvfix_shredder_subscriber \
    amps_publish_composite amps_publish amps_publish_for_replay amps_publish_queue \
    amps_publish_sow amps_query_sow amps_sow_and_subscribe amps_sow_and_subscribe_with_oof \
    amps_subscribe_composite amps_subscribe amps_subscribe_with_replay amps_update_for_oof

.PHONY: all

all: $(SAMPLES)
$(SAMPLES):
clean:
	rm -rf $(SAMPLES)

