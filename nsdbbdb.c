/*
 * The contents of this file are subject to the Mozilla Public License
 * Version 1.1(the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.mozilla.org/.
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis,WITHOUT WARRANTY OF ANY KIND,either express or implied. See
 * the License for the specific language governing rights and limitations
 * under the License.
 *
 * Alternatively,the contents of this file may be used under the terms
 * of the GNU General Public License(the "GPL"),in which case the
 * provisions of GPL are applicable instead of those above.  If you wish
 * to allow use of your version of this file only under the terms of the
 * GPL and not to allow others to use your version of this file under the
 * License,indicate your decision by deleting the provisions above and
 * replace them with the notice and other provisions required by the GPL.
 * If you do not delete the provisions above,a recipient may use your
 * version of this file under either the License or the GPL.
 *
 */

/*
 * nsdbbdb.c --
 *
 *	NaviServer driver for the Sleepycat Database from
 *      http://www.sleepycat.com/.
 *
 *   Author
 *      Vlad Seryakov vlad@crystalballinc.com
 *      Gustaf Neumann
 */

#include "ns.h"
#include "nsdb.h"

#define MODULE_VERSION "0.2"

#ifdef LMDB
# include "lmdb.h"
# define NS_DB_ENV    MDB_env
# define NS_DBI       MDB_dbi
# define NS_DB_TXN    MDB_txn
# define NS_DB_CURSOR MDB_cursor
# define NS_DB_VAL    MDB_val
# define NS_DB_STRERR mdb_strerror
# define NS_DB_SIZE_T size_t
# define NS_DB_APPEND      MDB_APPEND
# define NS_DB_NODUPDATA   MDB_NODUPDATA
# define NS_DB_NOOVERWRITE MDB_NOOVERWRITE
# define NS_DB_SET_RANGE   MDB_SET_RANGE
# define NS_DB_SET         MDB_SET
# define NS_DB_FIRST       MDB_FIRST
# define NS_DB_NOTFOUND    MDB_NOTFOUND
# define NS_DB_NEXT        MDB_NEXT
# define NS_DB_DBT_REALLOC 0x10000000
# define NS_DB_DBT_MALLOC  0x20000000
# define NS_DB_ENV_CREATE(dbEnv)         mdb_env_create(dbEnv)
# define NS_DB_ENV_OPEN(dbEnv,dbHome,dbEnvFlags) mdb_env_open((dbEnv), (dbHome), (dbEnvFlags), 0664)
# define NS_DB_ENV_CLOSE(dbEnv)          mdb_env_close((dbEnv))
# define NS_DB_ENV_TXN_BEGIN(dbEnv,txn)  mdb_txn_begin((dbEnv), NULL, 0, (txn))
# define NS_DB_ENV_TXN_COMMIT(txn)       mdb_txn_commit((txn))
# define NS_DB_ENV_TXN_ABORT(txn)        (mdb_txn_abort((txn)),0)
# define NS_DB_DBI_CLOSE(dbEnv,dbi)      mdb_close((dbEnv), (dbi))
# define NS_DB_DBI_CURSOR_OPEN(txn,dbi,c) mdb_cursor_open((txn), (dbi), (c))
# define NS_DB_DBI_CURSOR_CLOSE(c)       mdb_cursor_close((c))
# define NS_DB_DBI_GET(txn,dbi,key,data) mdb_get((txn),(dbi), (key), (data))
# define NS_DB_CURSOR_GET(c,k,d,flags)   mdb_cursor_get((c), (k), (d), (flags))
# define NS_DB_ENV_ERR(dbEnv,rc,command) Ns_Log(Error,"bdb: %s returns error: %s", (command), mdb_strerror(rc))
# define NS_DB_ERR0(db,rc,data)          Ns_Log(Error, "error %s: %s", mdb_strerror(rc), (data))
# define NS_DB_ERR1(db,rc,fmt,data)      Ns_Log(Error, "error %s:" (fmt), mdb_strerror(rc), (data))
# define NS_DB_VAL_DATA(data)            (data).mv_data
# define NS_DB_VAL_SIZE(data)            (data).mv_size
# define NS_DB_VAL_FLAGS(data)           data ## _flags
#else
# include "db.h"
# define NS_DB_ENV    DB_ENV
# define NS_DBI       DB*
# define NS_DB_TXN    DB_TXN
# define NS_DB_CURSOR DBC
# define NS_DB_VAL    DBT
# define NS_DB_STRERR db_strerror
# define NS_DB_SIZE_T u_int32_t
# define NS_DB_APPEND      DB_APPEND
# define NS_DB_NODUPDATA   DB_NODUPDATA
# define NS_DB_NOOVERWRITE DB_NOOVERWRITE
# define NS_DB_SET_RANGE   DB_SET_RANGE
# define NS_DB_SET         DB_SET
# define NS_DB_FIRST       DB_FIRST
# define NS_DB_NOTFOUND    DB_NOTFOUND
# define NS_DB_NEXT        DB_NEXT
# define NS_DB_DBT_REALLOC DB_DBT_REALLOC
# define NS_DB_DBT_MALLOC  DB_DBT_MALLOC
# define NS_DB_ENV_CREATE(dbEnv)          db_env_create((dbEnv), 0)
# define NS_DB_ENV_OPEN(dbEnv,dbHome,dbEnvFlags) dbEnv->open((dbEnv), (dbHome), (dbEnvFlags), 0)
# define NS_DB_ENV_CLOSE(dbEnv)           (dbEnv)->close((dbEnv), 0)
# define NS_DB_ENV_TXN_BEGIN(dbEnv,txn)   (dbEnv)->txn_begin((dbEnv), NULL, (txn), 0)
# define NS_DB_ENV_TXN_COMMIT(txn)        (txn)->commit((txn), 0)
# define NS_DB_ENV_TXN_ABORT(txn)         (txn)->abort((txn))
# define NS_DB_DBI_CLOSE(dbEnv,dbi)       (dbi)->close((dbi),0)
# define NS_DB_DBI_CURSOR_OPEN(txn,dbi,c) (dbi)->cursor((dbi), NULL, (c), DB_READ_UNCOMMITTED)
# define NS_DB_DBI_CURSOR_CLOSE(c)        (c)->c_close((c))
# define NS_DB_DBI_GET(txn,dbi,key,data)  (dbi)->get((dbi), NULL, (key), (data), DB_READ_UNCOMMITTED)
# define NS_DB_CURSOR_GET(c,k,d,flags)    (c)->c_get(c, (k), (d), (flags))
# define NS_DB_ENV_ERR(dbEnv,rc,command)  (dbEnv)->err((dbEnv), (rc), (command))
# define NS_DB_ERR0(db,rc,data)           (db)->err((db), (rc), "%s", (data))
# define NS_DB_ERR1(db,rc,fmt,data)       (db)->err((db), (rc), (fmt), (data))
# define NS_DB_DBI_TRUNCATE(db,countPtr)  (db)->truncate((db), NULL, (countPtr), 0)
# define NS_DB_VAL_DATA(d)                (d).data
# define NS_DB_VAL_SIZE(d)                (d).size
# define NS_DB_VAL_FLAGS(d)               (d).flags
#endif

#include <sys/stat.h>

#define DB_SELECT       1
#define DB_GET          2
#define DB_UPDATE       3
#define DB_DELETE       4
#define DB_CHECK        5

static const char *DbName(void);
static const char *DbDbType(void);
static int DbServerInit(const char *hServer, char *hModule, char *hDriver);
static int DbOpenDb(Ns_DbHandle *handle);
static int DbCloseDb(Ns_DbHandle *handle);
static int DbDML(Ns_DbHandle *handle, char *sql);
static int DbGetRow(Ns_DbHandle *handle, Ns_Set *row);
static int DbFlush(Ns_DbHandle *handle);
static int DbCancel(Ns_DbHandle *handle);
static int DbExec(Ns_DbHandle *handle, char *sql);
static int DbResetHandle(Ns_DbHandle *handle);
static int DbFree(Ns_DbHandle *handle);
static void DbShutdown(void *arg);
static Ns_Set *DbBindRow(Ns_DbHandle *handle);

static Ns_TclTraceProc DbInterpInit;
static Ns_LogSeverity BdbDebug;    /* Severity at which to log verbose debugging. */

#ifndef LMDB
static void DbError(const NS_DB_ENV *env, const char *errpfx, const char *msg);
#endif

static Ns_DbProc dbProcs[] = {
    {DbFn_ServerInit,  (ns_funcptr_t)DbServerInit},
    {DbFn_Name,        (ns_funcptr_t)DbName},
    {DbFn_DbType,      (ns_funcptr_t)DbDbType},
    {DbFn_OpenDb,      (ns_funcptr_t)DbOpenDb},
    {DbFn_CloseDb,     (ns_funcptr_t)DbCloseDb},
    {DbFn_DML,         (ns_funcptr_t)DbDML},
    {DbFn_GetRow,      (ns_funcptr_t)DbGetRow},
    {DbFn_Flush,       (ns_funcptr_t)DbFlush},
    {DbFn_Cancel,      (ns_funcptr_t)DbCancel},
    {DbFn_Exec,        (ns_funcptr_t)DbExec},
    {DbFn_BindRow,     (ns_funcptr_t)DbBindRow},
    {DbFn_ResetHandle, (ns_funcptr_t)DbResetHandle},
    {0, NULL}
};

typedef struct _dbConn {
    struct _dbConn *next;
    int cmd;
    int status;
    NS_DBI dbi;
    NS_DB_TXN *txn;
    NS_DB_CURSOR *cursor;
    NS_DB_VAL key;
    NS_DB_VAL data;
#ifdef LMDB
    unsigned int key_flags;
    unsigned int data_flags;
#endif
    int count;
} dbConn;

static int GetTempTxn(dbConn *conn, NS_DB_TXN **txnPtr);
static void CleanTempTxn(dbConn *conn, NS_DB_TXN *txn);

static const char *dbHome = NULL;
static NS_DB_ENV *dbEnv = NULL;
static const char *dbDelimiter = "\n";
static bool dbDebug = NS_FALSE;
#ifdef LMDB
static const char *dbName = "LMDB";
static unsigned int dbEnvFlags = 0;
#else
static const char *dbName = "BerkeleyDB";
static unsigned int dbDbFlags = DB_READ_UNCOMMITTED;
static unsigned int dbEnvFlags = DB_CREATE | DB_THREAD | DB_INIT_MPOOL | DB_READ_UNCOMMITTED;
static unsigned int dbHFactor = 0;
static unsigned int dbBtMinKey = 0;
static unsigned int dbPageSize = 0;
static unsigned int dbCacheSize = 0;
static bool dbSync = NS_TRUE;
#endif
static Tcl_HashTable dbTable;
static Ns_Mutex dbLock = NULL;

NS_EXPORT int Ns_ModuleVersion = 1;
NS_EXPORT NsDb_DriverInitProc Ns_DbDriverInit;

NS_EXPORT int Ns_DbDriverInit(const char *hModule, const char *configPath)
{
    int         rc;
    const char *str;
    Tcl_DString ds;

    if (dbEnv != NULL) {
        Ns_Log(Error, "nsdbbdb: db_env has already been initialized");
        return NS_ERROR;
    }

    Ns_Log(Notice, "loading module %s version %s using %s",
           hModule, MODULE_VERSION,
#ifdef LMDB
           mdb_version(NULL, NULL, NULL)
#else
           DB_VERSION_STRING
#endif
           );

    Tcl_InitHashTable(&dbTable, TCL_ONE_WORD_KEYS);
    Ns_MutexInit(&dbLock);
    BdbDebug = Ns_CreateLogSeverity("Debug(bdb)");

    if (Ns_DbRegisterDriver(hModule, dbProcs) != NS_OK) {
        Ns_Log(Error, "nsdbbdb: could not register the %s/%s driver", dbName, hModule);
        return NS_ERROR;
    }

    rc = NS_DB_ENV_CREATE(&dbEnv);
    if (rc != 0) {
        Ns_Log(Error, "nsdbbdb: db_env_create: %s", NS_DB_STRERR(rc));
        return NS_ERROR;
    }

    Tcl_DStringInit(&ds);
    configPath = Ns_ConfigGetPath(0, 0, "db", "pool", hModule, (char *)0L);
    dbDelimiter = Ns_ConfigGetValue(configPath, "delimiter");
    if (dbDelimiter == NULL) {
        dbDelimiter = "\n";
    }
    if ((dbHome = Ns_ConfigGetValue(configPath, "home"))) {
        Ns_DStringAppend(&ds, dbHome);
    }
    if (ds.length == 0) {
        Ns_DStringPrintf(&ds, "%s/db", Ns_InfoHomePath());
    }
    dbHome = ns_strdup(ds.string);
#ifndef LMDB
    Ns_ConfigGetInt(configPath, "pagesize", (int *)&dbPageSize);
    Ns_ConfigGetInt(configPath, "cachesize", (int *)&dbCacheSize);
    Ns_ConfigGetInt(configPath, "hfactor", (int *)&dbHFactor);
    Ns_ConfigGetInt(configPath, "btminkey", (int *)&dbBtMinKey);
    Ns_ConfigGetBool(configPath, "dbsync", (bool *)&dbSync);
#endif
    Ns_ConfigGetBool(configPath, "debug", (bool *)&dbDebug);

    if (dbDebug) {
        Ns_LogSeveritySetEnabled(BdbDebug, NS_TRUE);
    }

    /*
     * DB flags
     */
    str = Ns_ConfigGetValue(configPath, "dbflags");
    if (str == NULL) {
        str = "";
    }
    if (strstr(str, "dup")) {
#ifdef LMDB
        Ns_Log(Warning, "nsdbbdb: ignoring DB flag 'dup'");
#else
        dbDbFlags |= DB_DUP;
#endif
    }
    if (strstr(str, "onlycommitted") != NULL) {
#ifdef LMDB
        Ns_Log(Warning, "nsdbbdb: ignoring DB flag 'onlycommitted'");
#else
        dbDbFlags &= ~(unsigned int)DB_READ_UNCOMMITTED;
#endif
    }

    /*
     *  Environment flags
     */
    str = Ns_ConfigGetValue(configPath, "envflags");
    if (str == NULL) {
        str = "";
    }
    if (!strstr(str, "nolock")) {
#ifdef LMDB
        dbEnvFlags |= MDB_NOLOCK;
#else
        dbEnvFlags |= DB_INIT_LOCK;
#endif
    }
    if (strstr(str, "nolog") != NULL) {
#ifdef LMDB
        Ns_Log(Warning, "nsdbbdb: ignoring environment flag 'nolog'");
#else
        dbEnvFlags |= DB_INIT_LOG;
#endif
    }
    if (strstr(str, "notxn") != NULL) {
#ifdef LMDB
        Ns_Log(Warning, "nsdbbdb: ignoring environment flag 'notxn'");
#else
        dbEnvFlags |= DB_INIT_TXN | DB_RECOVER;
#endif
    }
    if (strstr(str, "noprivate") != NULL) {
#ifdef LMDB
        Ns_Log(Warning, "nsdbbdb: ignoring environment flag 'noprivate'");
#else
        dbEnvFlags |= DB_PRIVATE;
#endif
    }
    if (strstr(str, "onlycommitted") != NULL) {
#ifdef LMDB
        Ns_Log(Warning, "nsdbbdb: ignoring environment flag 'onlycommitted'");
#else
        dbEnvFlags &= ~(unsigned int)DB_READ_UNCOMMITTED;
#endif
    }

#ifdef LMDB
#else
    if (dbCacheSize) {
        dbEnv->set_cachesize(dbEnv, 0, dbCacheSize, 0);
    }
    dbEnv->set_lk_detect(dbEnv, DB_LOCK_DEFAULT);
    dbEnv->set_errpfx(dbEnv, "nsdbbdb");
    dbEnv->set_errcall(dbEnv, DbError);
    dbEnv->set_alloc(dbEnv, ns_malloc, ns_realloc, ns_free);
#endif

    mkdir(dbHome, 0777);

    if ((rc = NS_DB_ENV_OPEN(dbEnv, dbHome, dbEnvFlags)) != 0) {
        NS_DB_ENV_ERR(dbEnv, rc, "environment open");
        NS_DB_ENV_CLOSE(dbEnv);
        Tcl_DStringFree(&ds);
        dbEnv = NULL;
        return NS_ERROR;
    }

    Ns_RegisterAtExit(DbShutdown, 0);
    //Ns_Log(Notice, "%s/%s: Home=%s, Cache=%ud, Flags=%x", dbName, hModule, dbHome, dbCacheSize, dbEnvFlags);
    Tcl_DStringFree(&ds);
    return NS_OK;
}


static void DbShutdown(void *UNUSED(arg))
{
    Tcl_HashEntry *hPtr;
    Tcl_HashSearch search;

    hPtr = Tcl_FirstHashEntry(&dbTable, &search);
    while (hPtr != NULL) {
        dbConn *conn;

        conn = (dbConn *)Tcl_GetHashKey(&dbTable, hPtr);
        if (conn->dbi != 0) {
            Ns_Log(Notice, "DbShutdown: %p: closing %s", (void*)conn, dbHome);
            NS_DB_DBI_CLOSE(dbEnv, conn->dbi);
        }
        hPtr = Tcl_NextHashEntry(&search);
    }
    Tcl_DeleteHashTable(&dbTable);
    if (dbEnv != NULL) {
        NS_DB_ENV_CLOSE(dbEnv);
    }
    dbEnv = 0;
}

static const char *DbName(void)
{
    return dbName;
}


static const char *DbDbType(void)
{
    return dbName;
}

/*
 * Does both the SQLAllocConnect AND the SQLConnect
 *
 */

static int DbOpenDb(Ns_DbHandle *handle)
{
    dbConn     *conn;
    const char *dbpath;
    int         rc;
    NS_DBI      dbi;
#ifdef LMDB
    NS_DB_TXN  *txn;
#else
    DBTYPE      dbtype = DB_BTREE;
#endif

    Ns_Log(BdbDebug, "DbOpenDb '%s'", dbName);

#ifdef LMDB
    /*
     * We need for everything in LMDB a transaction.
     * On failure, abort it.
     */
    rc = mdb_txn_begin(dbEnv, NULL, 0, &txn);
    if (rc == 0) {
        rc = mdb_dbi_open(txn, NULL, 0, &dbi);
        if (rc != 0) {
            mdb_txn_abort(txn);
        } else {
            mdb_txn_commit(txn);
        }
    }
#else
    rc = db_create(&dbi, dbEnv, 0);
#endif
    if (rc != 0) {
        NS_DB_ENV_ERR(dbEnv, rc, "db_create");
        return NS_ERROR;
    }

    dbpath = handle->datasource;
#ifdef LMDB
    Ns_Log(Warning, "nsdbbdb: ignoring '%s'", dbpath);
#else
    if (strncmp(dbpath, "btree:", 6) == 0) {
        dbpath += 6;
        dbtype = DB_BTREE;
        if (dbBtMinKey) {
            dbi->set_bt_minkey(dbi, dbBtMinKey);
        }

    } else if (strncmp(dbpath, "hash:", 5) == 0) {
        dbpath += 5;
        dbtype = DB_HASH;
        if (dbHFactor) {
            dbi->set_h_ffactor(dbi, dbHFactor);
        }
    }
    if (dbDbFlags) {
        dbi->set_flags(dbi, dbDbFlags);
    }
    if (dbPageSize) {
        dbi->set_pagesize(dbi, dbPageSize);
    }
    if ((rc = dbi->open(dbi, 0, dbpath, 0, dbtype, DB_CREATE | DB_THREAD, 0664)) != 0) {
        NS_DB_ERR1(dbi, rc, "%s: open", handle->datasource);
        dbi->close(dbi, 0);
        return NS_ERROR;
    }
#endif
    /*
     * Finally, allocating the connection data.
     */
    conn = ns_calloc(1, sizeof(dbConn));
    conn->dbi = dbi;
    handle->connection = conn;
    Ns_MutexLock(&dbLock);
    Tcl_CreateHashEntry(&dbTable, (void *) conn, &rc);
    Ns_Log(BdbDebug, "DbOpen: %p, open handles=%d", (void*)conn, dbTable.numEntries);
    Ns_MutexUnlock(&dbLock);
    return NS_OK;
}

static int DbCloseDb(Ns_DbHandle *handle)
{
    dbConn        *conn = handle->connection;
    Tcl_HashEntry *hPtr;

    DbCancel(handle);
    NS_DB_DBI_CLOSE(dbEnv, conn->dbi);
    ns_free(conn);
    handle->connection = 0;
    Ns_MutexLock(&dbLock);
    hPtr = Tcl_FindHashEntry(&dbTable, (void *) conn);
    if (hPtr != NULL) {
        Tcl_DeleteHashEntry(hPtr);
    }
    Ns_Log(BdbDebug, "DbClose: %p, open handles=%d", (void*)conn, dbTable.numEntries);
    Ns_MutexUnlock(&dbLock);
    return NS_OK;
}

static int DbDML(Ns_DbHandle *UNUSED(handle), char *UNUSED(query))
{
    return NS_OK;
}

#ifdef LMDB
/*
 * LMDB needs for most operations a txn value. If we have no ongoing
 * transaction in conn, create a temporary one, which will be
 * committed/aborted after the operation.
 */
static int GetTempTxn(dbConn *conn, NS_DB_TXN **txnPtr)
{
    int rc = 0;
    if (likely(conn->txn != NULL)) {
        *txnPtr = conn->txn;
    } else {
        rc = mdb_txn_begin(dbEnv, NULL, 0, txnPtr);
    }
    return rc;
}

static void CleanTempTxn(dbConn *conn, NS_DB_TXN *txn)
{
    if (likely(conn->txn == NULL)) {
        if (likely(conn->status == 0)) {
            mdb_txn_commit(txn);
        } else {
            mdb_txn_abort(txn);
        }
        Ns_Log(BdbDebug, "... CleanTempTxn closes cursor %p", (void*)conn->cursor);
        conn->cursor = NULL;
    }
}
#else
static int GetTempTxn(dbConn *UNUSED(conn), NS_DB_TXN **UNUSED(txnPtr))
{
    return 0;
}
static void CleanTempTxn(dbConn *UNUSED(conn), NS_DB_TXN *UNUSED(txn))
{
}
#endif

static int DbExec(Ns_DbHandle *handle, char *query)
{
    dbConn    *conn = handle->connection;
    NS_DB_TXN *tempTxn;

    DbCancel(handle);

    /*
     * Retrieve one matching record
     */
    if (strncasecmp(query, "GET ", 4) == 0) {
        conn->cmd = DB_GET;
        NS_DB_VAL_DATA(conn->key) = query + 4;
        NS_DB_VAL_SIZE(conn->key) = ((NS_DB_SIZE_T)strlen(NS_DB_VAL_DATA(conn->key))) + 1;
#ifndef LMDB
        /*
         * LMDB: The memory pointed to by the returned values is owned by the
         * database, no need to free.
         */
        NS_DB_VAL_FLAGS(conn->data) = NS_DB_DBT_MALLOC;
#endif
        conn->status = GetTempTxn(conn, &tempTxn);
        conn->status = NS_DB_DBI_GET(tempTxn, conn->dbi, &conn->key, &conn->data);
        switch (conn->status) {
        case 0:
        case NS_DB_NOTFOUND:
            handle->fetchingRows = NS_TRUE;
            CleanTempTxn(conn, tempTxn);
            return NS_ROWS;
        default:
            NS_DB_ERR0(conn->dbi, conn->status, "DB->get");
            Ns_DbSetException(handle, "ERROR", NS_DB_STRERR(conn->status));
            CleanTempTxn(conn, tempTxn);
            return NS_ERROR;
        }
    }

    /*
     * Returns 1 if entry exists
     */
    if (strncasecmp(query, "CHECK ", 6) == 0) {
        Ns_Log(BdbDebug, "... CHECK");
        conn->cmd = DB_CHECK;
        NS_DB_VAL_DATA(conn->key) = query + 6;
        NS_DB_VAL_SIZE(conn->key) = ((NS_DB_SIZE_T)strlen(NS_DB_VAL_DATA(conn->key))) + 1;
        conn->status = GetTempTxn(conn, &tempTxn);
        conn->status = NS_DB_DBI_CURSOR_OPEN(tempTxn, conn->dbi, &conn->cursor);

        if (conn->status != 0) {
            NS_DB_ERR0(conn->dbi, conn->status, "DB->cursor");
            Ns_DbSetException(handle, "ERROR", NS_DB_STRERR(conn->status));
            CleanTempTxn(conn, tempTxn);
            return NS_ERROR;
        }
        Ns_Log(BdbDebug, "... cursor get key '%s' len %ld",
               (char*)NS_DB_VAL_DATA(conn->key),
               (long)NS_DB_VAL_SIZE(conn->key));

        conn->status = NS_DB_CURSOR_GET(conn->cursor, &conn->key, &conn->data, NS_DB_SET_RANGE);
        Ns_Log(BdbDebug, "... cursor get %p rc %d", (void*)conn->cursor, conn->status);

        switch (conn->status) {
        case 0:
        case NS_DB_NOTFOUND:
            Ns_Log(BdbDebug, "... cursor get %p rc %d return ROWS", (void*)conn->cursor, conn->status);
            handle->fetchingRows = NS_TRUE;
            CleanTempTxn(conn, tempTxn);
            return NS_ROWS;
        default:
            NS_DB_ERR0(conn->dbi, conn->status, "DB->get");
            Ns_DbSetException(handle, "ERROR", NS_DB_STRERR(conn->status));
            CleanTempTxn(conn, tempTxn);
            return NS_ERROR;
        }
    }

    if (strncasecmp(query, "TRUNCATE", 8) == 0) {
#ifdef LMDB
        int rc;
        NS_DB_TXN  *txn;

        rc = mdb_txn_begin(dbEnv, NULL, 0, &txn);
        if (rc == 0) {
            conn->status = mdb_drop(txn, conn->dbi, 0);
            mdb_txn_commit(txn);
        }
#else
        u_int32_t count;
        conn->status = conn->dbi->truncate(conn->dbi, NULL, &count, 0);
#endif
        if (conn->status == 0) {
            return NS_DML;
        }

        NS_DB_ENV_ERR(dbEnv, conn->status, "DB->truncate");
        Ns_DbSetException(handle, "ERROR", NS_DB_STRERR(conn->status));
        return NS_ERROR;
    }

    if (strncasecmp(query, "COMPACT", 7) == 0) {
#ifdef LMDB
        Ns_Log(Warning, "nsdbbdb: command COMPACT is not supported."
               "Future versions might support 'mdb_env_copy2' with the COMPACT option");
        return NS_DML;
#else
        DB_COMPACT c_data;
        memset(&c_data, 0, sizeof(c_data));
        conn->status = conn->dbi->compact(conn->dbi, NULL, NULL, NULL, NULL, 0, NULL);
        if (conn->status == 0) {
            return NS_DML;
        }
        NS_DB_ENV_ERR(dbEnv, conn->status, "DB->compact");
        Ns_DbSetException(handle, "ERROR", NS_DB_STRERR(conn->status));
        return NS_ERROR;
#endif
    }

    if (strncasecmp(query, "BEGIN", 5) == 0) {
        if (conn->txn == NULL) {
            conn->status = NS_DB_ENV_TXN_BEGIN(dbEnv, &conn->txn);
        }
        if (conn->status != 0) {
            return NS_DML;
        }
        NS_DB_ENV_ERR(dbEnv, conn->status, "NS_DB_ENV->txn_begin");
        Ns_DbSetException(handle, "ERROR", NS_DB_STRERR(conn->status));
        return NS_ERROR;
    }

    if (strncasecmp(query, "COMMIT", 6) == 0) {
        if (conn->txn != NULL) {
            conn->status = NS_DB_ENV_TXN_COMMIT(conn->txn);
        }
        conn->txn = 0;
        if (conn->status != 0) {
            return NS_DML;
        }
        NS_DB_ENV_ERR(dbEnv, conn->status, "NS_DB_ENV->txn_commit");
        Ns_DbSetException(handle, "ERROR", NS_DB_STRERR(conn->status));
        return NS_ERROR;
    }

    if (strncasecmp(query, "ABORT", 5) == 0) {
        if (conn->txn != NULL) {
            conn->status = NS_DB_ENV_TXN_ABORT(conn->txn);
        }
        conn->txn = NULL;
        if (conn->status != 0) {
            return NS_DML;
        }
        NS_DB_ENV_ERR(dbEnv, conn->status, "NS_DB_ENV->txn_commit");
        Ns_DbSetException(handle, "ERROR", NS_DB_STRERR(conn->status));
        return NS_ERROR;
    }

    if (strncasecmp(query, "PUT ", 4) == 0
        || strncasecmp(query, "PUT/", 4) == 0) {
        unsigned int  flags = 0;
        char         *ptr, orig = 0;

        if (query[3] == '/') {
            for (ptr = query + 3; *ptr && *ptr != ' '; ptr++) {
                if (*ptr == 'a') {
                    flags |= NS_DB_APPEND;
                } else
                if (*ptr == 'd') {
                    flags |= NS_DB_NODUPDATA;
                } else
                if (*ptr == 'o') {
                    flags |= NS_DB_NOOVERWRITE;
                }
            }
            NS_DB_VAL_DATA(conn->key) = ptr + 1;
        } else {
            NS_DB_VAL_DATA(conn->key) = query + 4;
        }
        conn->cmd = DB_UPDATE;
        if ((ptr = strstr(NS_DB_VAL_DATA(conn->key), dbDelimiter))) {
            orig = *ptr;
            *ptr = 0;
            NS_DB_VAL_DATA(conn->data) = ptr + 1;
            NS_DB_VAL_SIZE(conn->data) = ((NS_DB_SIZE_T)strlen(NS_DB_VAL_DATA(conn->data))) + 1;
        }
        NS_DB_VAL_SIZE(conn->key) = ((NS_DB_SIZE_T)strlen(NS_DB_VAL_DATA(conn->key))) + 1;
#ifdef LMDB
        conn->status = GetTempTxn(conn, &tempTxn);
        if (conn->status == 0) {
            conn->status = mdb_put(tempTxn, conn->dbi, &conn->key, &conn->data, flags);
            CleanTempTxn(conn, tempTxn);
        }
#else
        conn->status = conn->dbi->put(conn->dbi, 0, &conn->key, &conn->data, flags);
#endif
        // Restore original delimiter
        if (orig != 0) {
            *ptr = orig;
        }
        if (conn->status == 0) {
            return NS_DML;
        }
        // Report error situation
        NS_DB_ERR0(conn->dbi, conn->status, "DB->put");
        Ns_DbSetException(handle, "ERROR", NS_DB_STRERR(conn->status));
        return NS_ERROR;
    }

    if (strncasecmp(query, "DEL ", 4) == 0) {
        conn->cmd = DB_DELETE;
        NS_DB_VAL_DATA(conn->key) = query + 4;
        NS_DB_VAL_SIZE(conn->key) = ((NS_DB_SIZE_T)strlen(NS_DB_VAL_DATA(conn->key))) + 1;
#ifdef LMDB
        conn->status = GetTempTxn(conn, &tempTxn);
        if (conn->status == 0) {
            conn->status = mdb_del(tempTxn, conn->dbi, &conn->key, &conn->data);
            CleanTempTxn(conn, tempTxn);
        }
#else
        conn->status = conn->dbi->del(conn->dbi, 0, &conn->key, 0);
#endif
        if (conn->status == 0) {
            return NS_DML;
        }
        // Report error situation
        NS_DB_ERR0(conn->dbi, conn->status, "DB->del");
        Ns_DbSetException(handle, "ERROR", NS_DB_STRERR(conn->status));
        return NS_ERROR;
    }

    /*
     * Open cursor and retrieve all matching records
     */
    if (strncasecmp(query, "CURSOR", 6) == 0) {
        Ns_Log(BdbDebug, "... CURSOR");

        conn->cmd = DB_SELECT;
        conn->count = 0;
#ifdef LMDB
        /*
         * Do not use the usual tmpTxn, but allocate a transaction for the
         * cursor in the connection data. This txn is managed on the usual
         * cleanup.
         */
        if (conn->txn == NULL) {
            Ns_Log(BdbDebug, "... CURSOR allocating txn");
            conn->status = mdb_txn_begin(dbEnv, NULL, 0, &conn->txn);
        }
#endif
        conn->status = NS_DB_DBI_CURSOR_OPEN(conn->txn, conn->dbi, &conn->cursor);
        if (conn->status != 0) {
            NS_DB_ERR0(conn->dbi, conn->status, "DB->cursor");
            Ns_DbSetException(handle, "ERROR", NS_DB_STRERR(conn->status));
            return NS_ERROR;
        }

#ifndef LMDB
        /*
         * LMDB: The memory pointed to by the returned values is owned by the
         * database, no need to free.
         */
        NS_DB_VAL_FLAGS(conn->key) = NS_DB_VAL_FLAGS(conn->data) = NS_DB_DBT_REALLOC;
#endif

        /*
         * Range request, try to position cursor to the closest key
         */
        if (*(query + 6) != 0) {
            NS_DB_VAL_DATA(conn->key) = ns_strdup(query + 7);
            NS_DB_VAL_SIZE(conn->key) = ((NS_DB_SIZE_T)strlen(NS_DB_VAL_DATA(conn->key))) + 1;
            conn->status = NS_DB_CURSOR_GET(conn->cursor, &conn->key, &conn->data, NS_DB_SET_RANGE);

        } else {
            /*
             * Full table scan
             */
            conn->status = NS_DB_CURSOR_GET(conn->cursor, &conn->key, &conn->data, NS_DB_FIRST);
        }
        switch (conn->status) {
        case 0:
        case NS_DB_NOTFOUND:
            handle->fetchingRows = NS_TRUE;
            return NS_ROWS;
        default:
            NS_DB_ERR0(conn->dbi, conn->status, "DB->c_get");
            Ns_DbSetException(handle, "ERROR", NS_DB_STRERR(conn->status));
            return NS_ERROR;
        }
    }

    return NS_ERROR;
}

static int DbGetRow(Ns_DbHandle *handle, Ns_Set *row)
{
    int rc = 0;
    dbConn *conn = handle->connection;

    Ns_Log(BdbDebug, "getrow: %d: %d: %s %s",
           conn->cmd, conn->status, (char *) NS_DB_VAL_DATA(conn->key),
           (char *)NS_DB_VAL_DATA(conn->data));

    if (handle->fetchingRows == 0) {
        Ns_Log(Error, "getrow nsdbbdb(%s):  No rows waiting to fetch.", handle->datasource);
        Ns_DbSetException(handle, "NSDB", "no rows waiting to fetch.");
        return NS_ERROR;
    }

    if (conn->status == NS_DB_NOTFOUND) {
        Ns_Log(BdbDebug, "getrow: NS_END_DATA");
        handle->statement = 0;
        handle->fetchingRows = NS_FALSE;
        return NS_END_DATA;
    }

    switch (conn->cmd) {
    case DB_CHECK:
        Ns_Log(BdbDebug, "getrow: CHECK");
        // Only one record should be returned
        conn->status = NS_DB_NOTFOUND;
        Ns_SetPutValueSz(row, 0, "1", 1);
        return NS_OK;

    case DB_GET:
        // Only one record should be returned
        Ns_Log(BdbDebug, "getrow: DB_GET");
        conn->status = NS_DB_NOTFOUND;
        Ns_SetPutValueSz(row, 0,
                         NS_DB_VAL_DATA(conn->data), (TCL_SIZE_T)NS_DB_VAL_SIZE(conn->data));
        return NS_OK;

    case DB_SELECT:
        /*
         * On the first invocation, data is already provided.
         * For later calls, we have to get the next item.
         */
        conn->count ++;
        Ns_Log(BdbDebug, "getrow: DB_SELECT data %p cursor %p txn %p count: %d",
               (void*)NS_DB_VAL_DATA(conn->data),
               (void*)conn->cursor,
               (void*)conn->txn,
               conn->count
               );
        if (conn->count > 1) {
            conn->status = NS_DB_CURSOR_GET(conn->cursor, &conn->key, &conn->data, NS_DB_NEXT);

        }
        Ns_Log(BdbDebug, "getrow: status %d %s", conn->status, NS_DB_STRERR(conn->status));

        switch (conn->status) {
        case 0:
            Ns_Log(BdbDebug, "getrow: set data key <%s> data <%s>",
                   (char*)NS_DB_VAL_DATA(conn->key),
                   (char*)NS_DB_VAL_DATA(conn->data));
            Ns_SetPutValueSz(row, 0, NS_DB_VAL_DATA(conn->key), (TCL_SIZE_T)NS_DB_VAL_SIZE(conn->key));
            Ns_SetPutValueSz(row, 1, NS_DB_VAL_DATA(conn->data),(TCL_SIZE_T)NS_DB_VAL_SIZE(conn->data));
            DbFree(handle);
            return NS_OK;

        case NS_DB_NOTFOUND:
            handle->fetchingRows = NS_FALSE;
            return NS_END_DATA;

        default:
            NS_DB_ERR0(conn->dbi, rc, "DB->c_get");
            Ns_DbSetException(handle, "ERROR", NS_DB_STRERR(rc));
            handle->fetchingRows = NS_FALSE;
            return NS_ERROR;
        }
        break;
    }
    return NS_ERROR;
}

static int DbFlush(Ns_DbHandle *handle)
{
    DbCancel(handle);
#ifndef LMDB
    if (dbSync) {
        dbConn *conn = handle->connection;
        conn->dbi->sync(conn->dbi, 0);
    }
#endif
    return NS_OK;
}

#ifndef LMDB
static void DbError(const NS_DB_ENV *UNUSED(env), const char *errpfx, const char *msg)
{
    Ns_Log(Error, "nsdbbdb: %s: %s", errpfx, msg);
}
#endif

static int DbFree(Ns_DbHandle *handle)
{
    dbConn *conn = handle->connection;

#ifdef LMDB
    NS_DB_VAL_FLAGS(conn->key) = 0u;
    NS_DB_VAL_FLAGS(conn->data) = 0u;
#else
    if (NS_DB_VAL_FLAGS(conn->key) & (NS_DB_DBT_MALLOC | NS_DB_DBT_REALLOC)) {
        Ns_Log(BdbDebug, "... free key %p flags %8x", (void*)NS_DB_VAL_DATA(conn->key), NS_DB_VAL_FLAGS(conn->key));
        ns_free(NS_DB_VAL_DATA(conn->key));
    }
    if (NS_DB_VAL_FLAGS(conn->data) & (NS_DB_DBT_MALLOC | NS_DB_DBT_REALLOC)) {
        Ns_Log(BdbDebug, "... free data %p flags %8x", (void*)NS_DB_VAL_DATA(conn->data), NS_DB_VAL_FLAGS(conn->data));
        ns_free(NS_DB_VAL_DATA(conn->data));
    }
    memset(&conn->key, 0, sizeof(NS_DB_VAL));
    memset(&conn->data, 0, sizeof(NS_DB_VAL));
#endif
    return NS_OK;
}

static int DbCancel(Ns_DbHandle *handle)
{
    dbConn *conn = handle->connection;

    if (conn->cursor != NULL) {
        Ns_Log(BdbDebug, "... DbCancel closes cursor %p", (void*)conn->cursor);
        NS_DB_DBI_CURSOR_CLOSE(conn->cursor);
        conn->cursor = NULL;
    }
    if (conn->txn != NULL) {
        Ns_Log(BdbDebug, "... DbCancel aborts transaction %p", (void*)conn->txn);
        NS_DB_ENV_TXN_ABORT(conn->txn);
        conn->txn = NULL;
    }
    DbFree(handle);
    handle->statement = NULL;
    handle->fetchingRows = NS_FALSE;
    return NS_OK;
}

static int DbResetHandle(Ns_DbHandle *handle)
{
    return DbCancel(handle);
}

static Ns_Set *DbBindRow(Ns_DbHandle *handle)
{
    const dbConn *conn = handle->connection;

    switch (conn->cmd) {
    case DB_GET:
        Ns_SetPutSz(handle->row, "data", 4, NULL, 0);
        break;
    default:
        Ns_SetPutSz(handle->row, "key", 3, NULL, 0);
        Ns_SetPutSz(handle->row, "data", 3, NULL, 0);
    }
    return handle->row;
}

/*
 * DbCmd - This function implements the "ns_berkeleydb" Tcl command installed
 * into each interpreter of each virtual server.  It provides access to
 * features specific to the Db driver.
 */

static int DbCmd(ClientData UNUSED(dummy), Tcl_Interp *interp, int argc, const char **argv)
{
    Ns_DbHandle *handle;

    if (argc != 3) {
        Tcl_AppendResult(interp, "wrong # args: should be ", argv[0], " cmd handle", 0);
        return TCL_ERROR;
    }
    if (Ns_TclDbGetHandle(interp, (char *) argv[2], &handle) != TCL_OK) {
        return TCL_ERROR;
    }
    /*
     * Make sure this is an Db handle before accessing handle->connection.
     */
    if (Ns_DbDriverName(handle) != dbName) {
        Tcl_AppendResult(interp, argv[1], " is not of type ", dbName, 0);
        return TCL_ERROR;
    }
    // Deadlock detection
    if (strcmp(argv[1], "deadlock") == 0) {
#ifdef LMDB
        Ns_Log(Warning, "nsdbbdb: 'ns_berkeleydb deadlock' is not supported by LMDB.");
#else
        if (dbEnv != NULL) {
            dbEnv->lock_detect(dbEnv, 0, DB_LOCK_DEFAULT, 0);
        }
#endif
    }
    return TCL_OK;
}


static Ns_ReturnCode DbInterpInit(Tcl_Interp *interp, const void *arg)
{
    Tcl_CreateCommand(interp, "ns_berkeleydb", DbCmd, (void*)arg, NULL);
    return NS_OK;
}


static int DbServerInit(const char *hServer, char *UNUSED(hModule), char *UNUSED(hDriver))
{
    Ns_TclRegisterTrace(hServer, DbInterpInit, 0, NS_TCL_TRACE_CREATE);
    return NS_OK;
}

/*
 * Local Variables:
 * mode: c
 * c-basic-offset: 4
 * fill-column: 78
 * indent-tabs-mode: nil
 * End:
 */
