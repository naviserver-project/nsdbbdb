ifndef NAVISERVER
    NAVISERVER  = /usr/local/ns
endif

#
# Module name
#
MOD      =  nsberkeleydb.so

#
# Objects to build.
#
OBJS     = nsberkeleydb.o
MODLIBS  = -ldb

include  $(NAVISERVER)/include/Makefile.module
