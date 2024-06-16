#
# This is the makefile for use with Microsoft NMAKE and Microsoft Visual C++.
#
# For building with GNU make on Linux or similar systems, use the GNUmakefile
# provided in this directory.
#
!include platform_test.nmakefile

LIB_DIR=lib\$(TARGET_PLATFORM)
OBJ_DIR=obj\$(TARGET_PLATFORM)

DEBUG_LIB_DIR=$(LIB_DIR)\Debug
RELEASE_LIB_DIR=$(LIB_DIR)\Release

SOURCES=src\*.c

!if "$(AMPS_SHARED)"==""
TARGET_LIB=lib
TARGET_LINK=lib
DEBUG_CFLAGS=/TP /W3 /Zi /Od /Fd$(DEBUG_LIB_DIR)\amps.pdb /EHsc /MDd /DAMPS_BUILD
RELEASE_CFLAGS=/TP /W3 /Zi /Ox /Ot /Fd$(RELEASE_LIB_DIR)\amps.pdb /EHsc /MD /DAMPS_BUILD
!else
TARGET_LIB=dll
TARGET_LINK=link /DLL
DEBUG_CFLAGS=/TP /W3 /Zi /Od /Fd$(DEBUG_LIB_DIR)\amps.pdb /EHsc /MDd /DAMPS_BUILD /DAMPS_SHARED /LD
RELEASE_CFLAGS=/TP /W3 /Zi /Ox /Ot /Fd$(RELEASE_LIB_DIR)\amps.pdb /EHsc /MD /DAMPS_BUILD /DAMPS_SHARED /LD
!endif

all: $(RELEASE_LIB_DIR)\amps.lib $(DEBUG_LIB_DIR)\amps.lib

$(RELEASE_LIB_DIR)\amps.lib: $(SOURCES)
	-md $(RELEASE_LIB_DIR)
	del *.obj 2>nul
	cl /c /Iinclude $(RELEASE_CFLAGS) $(SOURCES)
	$(TARGET_LINK) /out:$(RELEASE_LIB_DIR)\amps.$(TARGET_LIB) *.obj
	del *.obj 2>nul

$(DEBUG_LIB_DIR)\amps.lib: $(SOURCES)
	-md $(DEBUG_LIB_DIR)
	del *.obj 2>nul
	cl /c /Iinclude $(DEBUG_CFLAGS) $(SOURCES)
	$(TARGET_LINK) /out:$(DEBUG_LIB_DIR)\amps.$(TARGET_LIB) *.obj
	del *.obj 2>nul

clean:
	-del *.obj 2>nul
	-del *.pdb 2>nul
	-del $(RELEASE_LIB_DIR)\amps.* 2>nul
	-del $(DEBUG_LIB_DIR)\amps.* 2>nul


