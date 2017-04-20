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
 * nsberkeleydb.c --
 *
 *	AOLserver driver for the Sleepycat Database from
 *      http://www.sleepycat.com/.
 *
 *   Author
 *      Vlad Seryakov vlad@crystalballinc.com
 */

#include "ns.h"
#include "nsdb.h"
#include "db.h"
#include <sys/stat.h>

#define DB_SELECT       1
#define DB_GET          2
#define DB_UPDATE       3
#define DB_DELETE       4
#define DB_CHECK        5

static char *DbName(void);
static char *DbDbType(void);
static int DbServerInit(char *hServer, char *hModule, char *hDriver);
static int DbOpenDb(Ns_DbHandle * handle);
static int DbCloseDb(Ns_DbHandle * handle);
static int DbDML(Ns_DbHandle * handle, char *sql);
static int DbGetRow(Ns_DbHandle * handle, Ns_Set * row);
static int DbFlush(Ns_DbHandle * handle);
static int DbCancel(Ns_DbHandle * handle);
static int DbExec(Ns_DbHandle * handle, char *sql);
static int DbResetHandle(Ns_DbHandle * handle);
static int DbFree(Ns_DbHandle * handle);
static void DbShutdown(void *arg);
static void DbError(const DB_ENV * env, const char *errpfx, const char *msg);
static Ns_Set *DbBindRow(Ns_DbHandle * handle);

static Ns_DbProc dbProcs[] = {
    {DbFn_ServerInit, DbServerInit},
    {DbFn_Name, DbName},
    {DbFn_DbType, DbDbType},
    {DbFn_OpenDb, DbOpenDb},
    {DbFn_CloseDb, DbCloseDb},
    {DbFn_DML, DbDML},
    {DbFn_GetRow, DbGetRow},
    {DbFn_Flush, DbFlush},
    {DbFn_Cancel, DbCancel},
    {DbFn_Exec, DbExec},
    {DbFn_BindRow, DbBindRow},
    {DbFn_ResetHandle, DbResetHandle},
    {0, NULL}
};

typedef struct _dbConn {
    struct _dbConn *next;
    int cmd;
    int status;
    DB *db;
    DB_TXN *txn;
    DBC *cursor;
    DBT key;
    DBT data;
} dbConn;

static char *dbHome = 0;
static DB_ENV *dbEnv = 0;
static char *dbDelimiter = "\n";
static char *dbName = "BerkeleyDB";
static unsigned int dbHFactor = 0;
static unsigned int dbBtMinKey = 0;
static unsigned int dbPageSize = 0;
static unsigned int dbCacheSize = 0;
static unsigned int dbSyncFlags = 1;
static unsigned int dbDebugFlags = 0;
static unsigned int dbDbFlags = DB_READ_UNCOMMITTED;
static unsigned int dbEnvFlags = DB_CREATE | DB_THREAD | DB_INIT_MPOOL | DB_READ_UNCOMMITTED;
static Tcl_HashTable dbTable;
static Ns_Mutex dbLock;

NS_EXPORT int Ns_ModuleVersion = 1;
NS_EXPORT NsDb_DriverInitProc Ns_DbDriverInit;

NS_EXPORT int Ns_DbDriverInit(const char *hModule, const char *configPath)
{
    int rc;
    char *str;
    Ns_DString ds;

    if (dbEnv) {
        Ns_Log(Error, "nsberkeleydb: db_env has already been initialized");
        return NS_ERROR;
    }
    Tcl_InitHashTable(&dbTable, TCL_ONE_WORD_KEYS);

    if (Ns_DbRegisterDriver(hModule, dbProcs) != NS_OK) {
        Ns_Log(Error, "nsberkeleydb: could not register the %s/%s driver", dbName, hModule);
        return NS_ERROR;
    }
    if ((rc = db_env_create(&dbEnv, 0)) != 0) {
        Ns_Log(Error, "nsberkeleydb: db_env_create: %s", db_strerror(rc));
        return NS_ERROR;
    }
    Ns_DStringInit(&ds);
    configPath = Ns_ConfigGetPath(0, 0, "db", "pool", hModule, NULL);
    if (!(dbDelimiter = Ns_ConfigGetValue(configPath, "delimiter"))) {
        dbDelimiter = "\n";
    }
    if ((dbHome = Ns_ConfigGetValue(configPath, "home"))) {
        Ns_DStringAppend(&ds, dbHome);
    }
    if (!ds.length) {
        Ns_DStringPrintf(&ds, "%s/db", Ns_InfoHomePath());
    }
    dbHome = ns_strdup(ds.string);
    Ns_ConfigGetInt(configPath, "pagesize", (int *) &dbPageSize);
    Ns_ConfigGetInt(configPath, "cachesize", (int *) &dbCacheSize);
    Ns_ConfigGetInt(configPath, "hfactor", (int *) &dbHFactor);
    Ns_ConfigGetInt(configPath, "btminkey", (int *) &dbBtMinKey);
    Ns_ConfigGetBool(configPath, "dbsync", (int *) &dbSyncFlags);
    Ns_ConfigGetBool(configPath, "debug", (int *) &dbDebugFlags);

    // Db flags
    if (!(str = Ns_ConfigGetValue(configPath, "dbflags"))) {
        str = "";
    }
    if (strstr(str, "dup")) {
        dbDbFlags |= DB_DUP;
    }
    if (!strstr(str, "onlycommited")) {
        dbDbFlags &= ~DB_READ_UNCOMMITTED;
    }
    // Environment flags
    if (!(str = Ns_ConfigGetValue(configPath, "envflags"))) {
        str = "";
    }
    if (!strstr(str, "nolock")) {
        dbEnvFlags |= DB_INIT_LOCK;
    }
    if (!strstr(str, "nolog")) {
        dbEnvFlags |= DB_INIT_LOG;
    }
    if (!strstr(str, "notxn")) {
        dbEnvFlags |= DB_INIT_TXN | DB_RECOVER;
    }
    if (!strstr(str, "noprivate")) {
        dbEnvFlags |= DB_PRIVATE;
    }
    if (!strstr(str, "onlycommited")) {
        dbEnvFlags &= ~DB_READ_UNCOMMITTED;
    }
    if (dbCacheSize) {
        dbEnv->set_cachesize(dbEnv, 0, dbCacheSize, 0);
    }
    dbEnv->set_lk_detect(dbEnv, DB_LOCK_DEFAULT);
    dbEnv->set_errpfx(dbEnv, "nsberkeleydb");
    dbEnv->set_errcall(dbEnv, DbError);
    dbEnv->set_alloc(dbEnv, ns_malloc, ns_realloc, ns_free);

    mkdir(dbHome, 0777);
    if ((rc = dbEnv->open(dbEnv, dbHome, dbEnvFlags, 0)) != 0) {
        dbEnv->err(dbEnv, rc, NULL);
        dbEnv->close(dbEnv, 0);
        Ns_DStringFree(&ds);
        dbEnv = 0;
        return NS_ERROR;
    }
    Ns_RegisterAtExit(DbShutdown, 0);
    Ns_Log(Notice, "%s/%s: Home=%s, Cache=%ud, Flags=%x", dbName, hModule, dbHome, dbCacheSize, dbEnvFlags);
    Ns_DStringFree(&ds);
    return NS_OK;
}


static void DbShutdown(void *arg)
{
    dbConn *conn;
    Tcl_HashEntry *hPtr;
    Tcl_HashSearch search;

    hPtr = Tcl_FirstHashEntry(&dbTable, &search);
    while (hPtr != NULL) {
        conn = (dbConn *) Tcl_GetHashKey(&dbTable, hPtr);
        if (conn->db) {
            Ns_Log(Notice, "DbShutdown: %p: closing %s", conn, conn->db->fname);
            conn->db->close(conn->db, 0);
        }
        hPtr = Tcl_NextHashEntry(&search);
    }
    Tcl_DeleteHashTable(&dbTable);
    if (dbEnv) {
        dbEnv->close(dbEnv, 0);
    }
    dbEnv = 0;
}

static char *DbName(void)
{
    return dbName;
}


static char *DbDbType(void)
{
    return dbName;
}

/*
 * Does both the SQLAllocConnect AND the SQLConnect
 *
 */

static int DbOpenDb(Ns_DbHandle * handle)
{
    DB *db;
    dbConn *conn;
    char *dbpath;
    int rc, dbtype = DB_BTREE;

    if ((rc = db_create(&db, dbEnv, 0)) != 0) {
        dbEnv->err(dbEnv, rc, "db_create");
        return NS_ERROR;
    }
    dbpath = handle->datasource;
    if (!strncmp(dbpath, "btree:", 6)) {
        dbpath += 6;
        dbtype = DB_BTREE;
        if (dbBtMinKey) {
            db->set_bt_minkey(db, dbBtMinKey);
        }
    } else 
    if (!strncmp(dbpath, "hash:", 5)) {
        dbpath += 5;
        dbtype = DB_HASH;
        if (dbHFactor) {
            db->set_h_ffactor(db, dbHFactor);
        }
    }
    if (dbDbFlags) {
        db->set_flags(db, dbDbFlags);
    }
    if (dbPageSize) {
        db->set_pagesize(db, dbPageSize);
    }
    if ((rc = db->open(db, 0, dbpath, 0, dbtype, DB_CREATE | DB_THREAD, 0664)) != 0) {
        db->err(db, rc, "%s: open", handle->datasource);
        db->close(db, 0);
        return NS_ERROR;
    }
    conn = ns_calloc(1, sizeof(dbConn));
    conn->db = db;
    handle->connection = conn;
    Ns_MutexLock(&dbLock);
    Tcl_CreateHashEntry(&dbTable, (void *) conn, &rc);
    if (dbDebugFlags) {
        Ns_Log(Notice, "DbOpen: %p, open handles=%d", conn, dbTable.numEntries);
    }
    Ns_MutexUnlock(&dbLock);
    return NS_OK;
}

static int DbCloseDb(Ns_DbHandle * handle)
{
    dbConn *conn = handle->connection;
    Tcl_HashEntry *hPtr;

    DbCancel(handle);
    conn->db->close(conn->db, 0);
    ns_free(conn);
    handle->connection = 0;
    Ns_MutexLock(&dbLock);
    hPtr = Tcl_FindHashEntry(&dbTable, (void *) conn);
    if (hPtr != NULL) {
        Tcl_DeleteHashEntry(hPtr);
    }
    if (dbDebugFlags) {
        Ns_Log(Notice, "DbClose: %p, open handles=%d", conn, dbTable.numEntries);
    }
    Ns_MutexUnlock(&dbLock);
    return NS_OK;
}

static int DbDML(Ns_DbHandle * handle, char *query)
{
    return NS_OK;
}

static int DbExec(Ns_DbHandle * handle, char *query)
{
    dbConn *conn = handle->connection;

    DbCancel(handle);

    if (!strncasecmp(query, "TRUNCATE", 8)) {
        u_int32_t count;
        conn->status = conn->db->truncate(conn->db, NULL, &count, 0);
        if (!conn->status) {
            return NS_DML;
        }
        dbEnv->err(dbEnv, conn->status, "DB->truncate");
        Ns_DbSetException(handle, "ERROR", db_strerror(conn->status));
        return NS_ERROR;
    }

    if (!strncasecmp(query, "COMPACT", 7)) {
        DB_COMPACT c_data;
        memset(&c_data, 0, sizeof(c_data));
        conn->status = conn->db->compact(conn->db, NULL, NULL, NULL, NULL, 0, NULL);
        if (!conn->status) {
            return NS_DML;
        }
        dbEnv->err(dbEnv, conn->status, "DB->compact");
        Ns_DbSetException(handle, "ERROR", db_strerror(conn->status));
        return NS_ERROR;
    }

    if (!strncasecmp(query, "BEGIN", 5)) {
        if (!conn->txn) {
            conn->status = dbEnv->txn_begin(dbEnv, NULL, &conn->txn, 0);
        }
        if (!conn->status) {
            return NS_DML;
        }
        dbEnv->err(dbEnv, conn->status, "DB_ENV->txn_begin");
        Ns_DbSetException(handle, "ERROR", db_strerror(conn->status));
        return NS_ERROR;
    }

    if (!strncasecmp(query, "COMMIT", 6)) {
        if (conn->txn) {
            conn->status = conn->txn->commit(conn->txn, 0);
        }
        conn->txn = 0;
        if (!conn->status) {
            return NS_DML;
        }
        dbEnv->err(dbEnv, conn->status, "DB_ENV->txn_commit");
        Ns_DbSetException(handle, "ERROR", db_strerror(conn->status));
        return NS_ERROR;
    }

    if (!strncasecmp(query, "ABORT", 5)) {
        if (conn->txn) {
            conn->status = conn->txn->abort(conn->txn);
        }
        conn->txn = 0;
        if (!conn->status) {
            return NS_DML;
        }
        dbEnv->err(dbEnv, conn->status, "DB_ENV->txn_commit");
        Ns_DbSetException(handle, "ERROR", db_strerror(conn->status));
        return NS_ERROR;
    }

    if (!strncasecmp(query, "PUT ", 4) || !strncasecmp(query, "PUT/", 4)) {
        unsigned int flags = 0;
        char *ptr, orig = 0;
        if (query[3] == '/') {
            for (ptr = query + 3; *ptr && *ptr != ' '; ptr++) {
                if (*ptr == 'a') {
                    flags |= DB_APPEND;
                } else 
                if (*ptr == 'd') {
                    flags |= DB_NODUPDATA;
                } else 
                if (*ptr == 'o') {
                    flags |= DB_NOOVERWRITE;
                }
            }
            conn->key.data = ptr + 1;
        } else {
            conn->key.data = query + 4;
        }
        conn->cmd = DB_UPDATE;
        if ((ptr = strstr(conn->key.data, dbDelimiter))) {
            orig = *ptr;
            *ptr = 0;
            conn->data.data = ptr + 1;
            conn->data.size = strlen(conn->data.data) + 1;
        }
        conn->key.size = strlen(conn->key.data) + 1;
        conn->status = conn->db->put(conn->db, 0, &conn->key, &conn->data, flags);
        // Restore original delimiter
        if (orig) {
            *ptr = orig;
        }
        if (!conn->status) {
            return NS_DML;
        }
        // Report error situation
        conn->db->err(conn->db, conn->status, "DB->put");
        Ns_DbSetException(handle, "ERROR", db_strerror(conn->status));
        return NS_ERROR;
    }

    if (!strncasecmp(query, "DEL ", 4)) {
        conn->cmd = DB_DELETE;
        conn->key.data = query + 4;
        conn->key.size = strlen(conn->key.data) + 1;
        conn->status = conn->db->del(conn->db, 0, &conn->key, 0);
        if (!conn->status) {
            return NS_DML;
        }
        // Report error situation
        conn->db->err(conn->db, conn->status, "DB->del");
        Ns_DbSetException(handle, "ERROR", db_strerror(conn->status));
        return NS_ERROR;
    }

    /* Open cursor and retrieve all matching records */
    if (!strncasecmp(query, "CURSOR", 6)) {
        conn->cmd = DB_SELECT;
        if ((conn->status = conn->db->cursor(conn->db, NULL, &conn->cursor, DB_READ_UNCOMMITTED)) != 0) {
            conn->db->err(conn->db, conn->status, "DB->cursor");
            Ns_DbSetException(handle, "ERROR", db_strerror(conn->status));
            return NS_ERROR;
        }
        conn->key.flags = conn->data.flags = DB_DBT_REALLOC;
        /* Range request, try to position cursor to the closest key */
        if (*(query + 6)) {
            conn->key.data = ns_strdup(query + 7);
            conn->key.size = strlen(conn->key.data) + 1;
            conn->status = conn->cursor->c_get(conn->cursor, &conn->key, &conn->data, DB_SET_RANGE);
        } else {
            /* Full table scan */
            conn->status = conn->cursor->c_get(conn->cursor, &conn->key, &conn->data, DB_FIRST);
        }
        switch (conn->status) {
        case 0:
        case DB_NOTFOUND:
            handle->fetchingRows = 1;
            return NS_ROWS;
        default:
            conn->db->err(conn->db, conn->status, "DB->c_get");
            Ns_DbSetException(handle, "ERROR", db_strerror(conn->status));
            return NS_ERROR;
        }
    }

    /* Retrieve one matching record */
    if (!strncasecmp(query, "GET ", 4)) {
        conn->cmd = DB_GET;
        conn->key.data = query + 4;
        conn->key.size = strlen(conn->key.data) + 1;
        conn->data.flags = DB_DBT_MALLOC;
        conn->status = conn->db->get(conn->db, NULL, &conn->key, &conn->data, DB_READ_UNCOMMITTED);
        switch (conn->status) {
        case 0:
        case DB_NOTFOUND:
            handle->fetchingRows = 1;
            return NS_ROWS;
        default:
            conn->db->err(conn->db, conn->status, "DB->get");
            Ns_DbSetException(handle, "ERROR", db_strerror(conn->status));
            return NS_ERROR;
        }
    }

    /* Returns 1 if entry exists */
    if (!strncasecmp(query, "CHECK ", 6)) {
        conn->cmd = DB_CHECK;
        conn->key.data = query + 6;
        conn->key.size = strlen(conn->key.data) + 1;
        if ((conn->status = conn->db->cursor(conn->db, NULL, &conn->cursor, DB_READ_UNCOMMITTED)) != 0) {
            conn->db->err(conn->db, conn->status, "DB->cursor");
            Ns_DbSetException(handle, "ERROR", db_strerror(conn->status));
            return NS_ERROR;
        }
        conn->status = conn->cursor->c_get(conn->cursor, &conn->key, &conn->data, DB_SET);
        switch (conn->status) {
        case 0:
        case DB_NOTFOUND:
            handle->fetchingRows = 1;
            return NS_ROWS;
        default:
            conn->db->err(conn->db, conn->status, "DB->get");
            Ns_DbSetException(handle, "ERROR", db_strerror(conn->status));
            return NS_ERROR;
        }
    }
    return NS_ERROR;
}

static int DbGetRow(Ns_DbHandle * handle, Ns_Set * row)
{
    int rc = 0;
    dbConn *conn = handle->connection;

    if (handle->verbose)
        Ns_Log(Debug, "nsberkeleydb: getrow: %d: %d: %s %s", conn->cmd, conn->status, (char *) conn->key.data,
               (char *) conn->data.data);

    if (!handle->fetchingRows) {
        Ns_Log(Error, "Ns_FreeTDS_GetRow(%s):  No rows waiting to fetch.", handle->datasource);
        Ns_DbSetException(handle, "NSDB", "no rows waiting to fetch.");
        return NS_ERROR;
    }

    if (conn->status == DB_NOTFOUND) {
        handle->statement = 0;
        handle->fetchingRows = 0;
        return NS_END_DATA;
    }

    switch (conn->cmd) {
    case DB_CHECK:
        // Only one record should be returned
        conn->status = DB_NOTFOUND;
        Ns_SetPutValue(row, 0, "1");
        return NS_OK;

    case DB_GET:
        // Only one record should be returned
        conn->status = DB_NOTFOUND;
        Ns_SetPutValue(row, 0, conn->data.data);
        return NS_OK;

    case DB_SELECT:
        // For the first time we have already called c_get
        if (!conn->data.data) {
            conn->status = conn->cursor->c_get(conn->cursor, &conn->key, &conn->data, DB_NEXT);
        }
        switch (conn->status) {
        case 0:
            Ns_SetPutValue(row, 0, conn->key.data);
            Ns_SetPutValue(row, 1, conn->data.data);
            DbFree(handle);
            return NS_OK;
        case DB_NOTFOUND:
            handle->fetchingRows = 0;
            return NS_END_DATA;
        default:
            conn->db->err(conn->db, rc, "DB->c_get");
            Ns_DbSetException(handle, "ERROR", db_strerror(rc));
            handle->fetchingRows = 0;
            return NS_ERROR;
        }
        break;
    }
    return NS_ERROR;
}

static int DbFlush(Ns_DbHandle * handle)
{
    dbConn *conn = handle->connection;

    DbCancel(handle);
    if (dbSyncFlags) {
        conn->db->sync(conn->db, 0);
    }
    return NS_OK;
}

static void DbError(const DB_ENV * env, const char *errpfx, const char *msg)
{
    Ns_Log(Error, "nsberkeleydb: %s: %s", errpfx, msg);
}

static int DbFree(Ns_DbHandle * handle)
{
    dbConn *conn = handle->connection;

    if (conn->key.flags & (DB_DBT_MALLOC | DB_DBT_REALLOC)) {
        ns_free(conn->key.data);
    }
    if (conn->data.flags & (DB_DBT_MALLOC | DB_DBT_REALLOC)) {
        ns_free(conn->data.data);
    }
    memset(&conn->key, 0, sizeof(DBT));
    memset(&conn->data, 0, sizeof(DBT));
    return NS_OK;
}

static int DbCancel(Ns_DbHandle * handle)
{
    dbConn *conn = handle->connection;

    if (conn->cursor) {
        conn->cursor->c_close(conn->cursor);
        conn->cursor = 0;
    }
    if (conn->txn) {
        conn->txn->abort(conn->txn);
    }
    conn->txn = 0;
    DbFree(handle);
    handle->statement = 0;
    handle->fetchingRows = 0;
    return NS_OK;
}

static int DbResetHandle(Ns_DbHandle * handle)
{
    return DbCancel(handle);
}

static Ns_Set *DbBindRow(Ns_DbHandle * handle)
{
    dbConn *conn = handle->connection;

    switch (conn->cmd) {
    case DB_GET:
        Ns_SetPut(handle->row, "data", NULL);
        break;
    default:
        Ns_SetPut(handle->row, "key", NULL);
        Ns_SetPut(handle->row, "data", NULL);
    }
    return handle->row;
}

/*
 * DbCmd - This function implements the "ns_berkeleydb" Tcl command installed
 * into each interpreter of each virtual server.  It provides access to
 * features specific to the Db driver.
 */

static int DbCmd(ClientData dummy, Tcl_Interp * interp, int argc, CONST char **argv)
{
    Ns_DbHandle *handle;

    if (argc != 3) {
        Tcl_AppendResult(interp, "wrong # args: should be ", argv[0], " cmd handle", 0);
        return TCL_ERROR;
    }
    if (Ns_TclDbGetHandle(interp, (char *) argv[2], &handle) != TCL_OK) {
        return TCL_ERROR;
    }
    /* Make sure this is an Db handle before accessing handle->connection. */
    if (Ns_DbDriverName(handle) != dbName) {
        Tcl_AppendResult(interp, argv[1], " is not of type ", dbName, 0);
        return TCL_ERROR;
    }
    // Deadlock detection
    if (!strcmp(argv[1], "deadlock")) {
        if (dbEnv) {
            dbEnv->lock_detect(dbEnv, 0, DB_LOCK_DEFAULT, 0);
        }
    }
    return TCL_OK;
}


static int DbInterpInit(Tcl_Interp * interp, void *arg)
{
    Tcl_CreateCommand(interp, "ns_berkeleydb", DbCmd, arg, NULL);
    return NS_OK;
}


static int DbServerInit(char *hServer, char *hModule, char *hDriver)
{
    Ns_TclRegisterTrace(hServer, DbInterpInit, 0, NS_TCL_TRACE_CREATE);
    return NS_OK;
}
