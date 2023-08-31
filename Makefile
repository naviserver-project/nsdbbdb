ifndef NAVISERVER
    NAVISERVER  = /usr/local/ns
endif

#
# Use of Berkeley DB vs. LMDB (Lightning Memory-Mapped Database Manager)
#
ifdef LMDB
    CFLAGS  += -DLMDB=1
    MODLIBS += -llmdb
    MOD      =  nsdblmdb.so
else
    MODLIBS += -ldb
    MOD      =  nsdbbdb.so
endif

#
# Were special include/load paths specified?
#
ifdef DBINCLUDE
    CFLAGS += -I$(DBINCLUDE)
endif
ifdef DBLIB
    MODLIBS    += -L$(DBLIB) -Wl,-rpath,$(DBLIB)
endif


#
# Objects to build.
#
MODOBJS  = nsdbbdb.o
MODLIBS  += -lnsdb

include  $(NAVISERVER)/include/Makefile.module
