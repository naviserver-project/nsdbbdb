ifndef NAVISERVER
    NAVISERVER  = /usr/local/ns
endif

#
# Module name
#
MOD      =  nsdbbdb.so

#
# Objects to build.
#
OBJS     = nsdbbdb.o
MODLIBS  = -ldb

include  $(NAVISERVER)/include/Makefile.module
