#
# This is the makefile for use with GNU make and Linux or similar systems.
#
# To build using Microsoft Visual C++ and Microsoft NMAKE, use the "makefile"
# provided in this directory.
#
libdir = src
samplesdir = samples
ifeq ("$(PROCESSOR)","")
PROCESSOR = $(shell uname -m)
endif
ifeq ("$(PROCESSOR)","unknown")
PROCESSOR = x86_64
endif

ifeq ("$(SHARED)","1")
	PIC=1
	SHARED=1
endif

.PHONY: all samples clean

all: project samples


project:
	mkdir -p lib/$(PROCESSOR)/release
	mkdir -p lib/$(PROCESSOR)/debug
	+$(MAKE) -B TARGETDIR=../lib/$(PROCESSOR)/release PROCESSOR=$(PROCESSOR) -C $(libdir) PIC=$(PIC)
	+$(MAKE) -B TARGETDIR=../lib/$(PROCESSOR)/debug PROCESSOR=$(PROCESSOR) DEBUGOPTS=-g OPTIMIZER=-O0 -C $(libdir) PIC=$(PIC)

samples: project
	+$(MAKE) -C $(samplesdir) PROCESSOR=$(PROCESSOR)

clean:
	$(MAKE) -B TARGETDIR=../lib/$(PROCESSOR)/release PROCESSOR=$(PROCESSOR) -C $(libdir) clean
	$(MAKE) -B TARGETDIR=../lib/$(PROCESSOR)/debug PROCESSOR=$(PROCESSOR) DEBUGOPTS=-g OPTIMIZER=-O0 -C $(libdir) clean
	$(MAKE) -C $(samplesdir) clean
