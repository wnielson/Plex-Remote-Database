//
//  mysqlite.c
//  MySQLite
//
//  Created by Weston Nielson on 10/20/15.
//  Copyright © 2015 Weston Nielson. All rights reserved.
//

#include <jansson.h>
#include <my_global.h>
#include <mysql.h>
#include <stdio.h>
#include <ctype.h>

#define SQLITE_OK           0   /* Successful result */
/* beginning-of-error-codes */
#define SQLITE_ERROR        1   /* SQL error or missing database */
#define SQLITE_INTERNAL     2   /* Internal logic error in SQLite */
#define SQLITE_PERM         3   /* Access permission denied */
#define SQLITE_ABORT        4   /* Callback routine requested an abort */
#define SQLITE_BUSY         5   /* The database file is locked */
#define SQLITE_LOCKED       6   /* A table in the database is locked */
#define SQLITE_NOMEM        7   /* A malloc() failed */
#define SQLITE_READONLY     8   /* Attempt to write a readonly database */
#define SQLITE_INTERRUPT    9   /* Operation terminated by sqlite3_interrupt()*/
#define SQLITE_IOERR       10   /* Some kind of disk I/O error occurred */
#define SQLITE_CORRUPT     11   /* The database disk image is malformed */
#define SQLITE_NOTFOUND    12   /* Unknown opcode in sqlite3_file_control() */
#define SQLITE_FULL        13   /* Insertion failed because database is full */
#define SQLITE_CANTOPEN    14   /* Unable to open the database file */
#define SQLITE_PROTOCOL    15   /* Database lock protocol error */
#define SQLITE_EMPTY       16   /* Database is empty */
#define SQLITE_SCHEMA      17   /* The database schema changed */
#define SQLITE_TOOBIG      18   /* String or BLOB exceeds size limit */
#define SQLITE_CONSTRAINT  19   /* Abort due to constraint violation */
#define SQLITE_MISMATCH    20   /* Data type mismatch */
#define SQLITE_MISUSE      21   /* Library used incorrectly */
#define SQLITE_NOLFS       22   /* Uses OS features not supported on host */
#define SQLITE_AUTH        23   /* Authorization denied */
#define SQLITE_FORMAT      24   /* Auxiliary database format error */
#define SQLITE_RANGE       25   /* 2nd parameter to sqlite3_bind out of range */
#define SQLITE_NOTADB      26   /* File opened that is not a database file */
#define SQLITE_NOTICE      27   /* Notifications from sqlite3_log() */
#define SQLITE_WARNING     28   /* Warnings from sqlite3_log() */
#define SQLITE_ROW         100  /* sqlite3_step() has another row ready */
#define SQLITE_DONE        101  /* sqlite3_step() has finished executing */

typedef void sqlite3;
//typedef void sqlite3_stmt;

typedef struct {
    MYSQL* mydb;
    
} mysqlite3;

typedef struct {
    MYSQL*        mydb;
    
    MYSQL_STMT*   mystmt;
    char*         sql;
    
    unsigned char executed;
    
    unsigned int  ncolumns;

    MYSQL_RES*    myres;
    
    unsigned long nparams;
    MYSQL_BIND*   myparams;

    unsigned int   nrow;
    unsigned long  nresults;
    unsigned long* lengths;
    MYSQL_BIND*    results;
} mysqlite3_stmt;

/*******************************************************************************
**  Internal Functions
*/

/*
**  Parses an SQL string ``buffer`` and returns the length of the first full
**  statement.  Statements are separated by semicolons, or a single statement
**  can just end (without a semicolon).  This function returns the length,
**  including the semicolon (if present), of the first statement.
*/
static int statement_length(const char* buffer, int length)
{
    const char* p;
    int c;
    char stringStart = '"';
    enum states { DULL, IN_WORD, IN_STRING } state = DULL;
    int pos = -1;
    
    for (p = buffer; *p != '\0'; p++)
    {
        pos++;
        
        if (length > 0 && pos >= length) { break; }
        
        c = (unsigned char) *p;
        switch (state)
        {
            case DULL:
                if (isspace(c)) { continue; }
                if (c == ';') { break; }
                
                if (c == '"' || c == '\'')
                {
                    state = IN_STRING;
                    stringStart = *p;
                    continue;
                }
                
                state = IN_WORD;
                continue;
                
            case IN_STRING:
                if (c == stringStart) { state = DULL; }
                continue;
                
            case IN_WORD:
                if (isspace(c)) { state = DULL; }
                if (c == ';') { break; }
                continue;
        }
        
        // If we get here, it means that we encountered a semicolon
        break;
    }
    
    return (pos > 0) ? pos+1 : pos;
}


static int finish_with_error(MYSQL* con)
{
    fprintf(stderr, "Error #%d: %s\n", mysql_errno(con), mysql_error(con));
    mysql_close(con);
    return -1;
};


/*******************************************************************************
**  SQLite API Functions
*/

int sqlite3_open(const char* filename, /* Database filename (UTF-8) */
                 sqlite3** ppDb        /* OUT: SQLite db handle */
)
{
    MYSQL* con = mysql_init(NULL);
    
    const char* host = NULL;
    const char* user = NULL;
    const char* pass = NULL;
    const char* db = NULL;
    
    if (con == NULL)
    {
        fprintf(stderr, "sqlite3_open() -> mysql_init() failed\n");
        return 0;
    }
    
    FILE* fh = fopen(filename, "r");
    if (fh)
    {
        json_t* root = json_loadf(fh, JSON_DECODE_ANY, NULL);
        if (root)
        {
            // We got JSON
            host = json_string_value(json_object_get(root, "host"));
            user = json_string_value(json_object_get(root, "user"));
            pass = json_string_value(json_object_get(root, "pass"));
            db   = json_string_value(json_object_get(root, "db"));
            
            if (mysql_real_connect(con, host, user, pass, NULL, 0, NULL, 0) == NULL)
            {
                finish_with_error(con);
                *ppDb = NULL;
            }
            else
            {
                if (mysql_select_db(con, db) != 0)
                {
                    char* sql;
                    int len = asprintf(&sql, "CREATE DATABASE %s", db);
                    mysql_real_query(con, sql, len);
                    free(sql);
                }
                
                if (mysql_select_db(con, db) != 0) {
                    finish_with_error(con);
                    *ppDb = NULL;
                }
                else {
                    *ppDb = (sqlite3*)con;
                }
            }
            
            // Cleanup
            json_decref(root);
        }
        fclose(fh);
    }
    
    return 0;
};

int sqlite3_open_v2(const char* filename,   /* Database filename (UTF-8) */
                    void** ppDb,            /* OUT: SQLite db handle */
                    int flags,              /* Flags */
                    const char* zVfs        /* Name of VFS module to use */
)
{
    return sqlite3_open(filename, ppDb);
};


int sqlite3_prepare(sqlite3* db,              /* Database handle */
                    const char* zSql,         /* SQL statement, UTF-8 encoded */
                    int nBytes,               /* Maximum length of zSql in bytes. */
                    mysqlite3_stmt** ppStmt,  /* OUT: Statement handle */
                    const char** pzTail       /* OUT: Pointer to unused portion of zSql */
)
{
    int rc = SQLITE_OK;
    
    mysqlite3_stmt* stmt = (mysqlite3_stmt*)malloc(sizeof(mysqlite3_stmt));
    
    stmt->mydb   = db;
    stmt->mystmt = mysql_stmt_init(db);
    stmt->myres  = NULL;
    stmt->myparams = NULL;
    stmt->results  = NULL;
    stmt->nresults = 0;
    stmt->executed = 0;
    stmt->nrow     = 0;
    stmt->lengths  = NULL;
    
    int stmt_len = statement_length(zSql, nBytes);
    
    if (nBytes > 0 && nBytes < stmt_len)
    {
        stmt_len = nBytes;
    }
    
    // "Strip" trailing semicolon, if present
    if (zSql[stmt_len-1] == ';')
    {
        stmt_len -= 1;
    }
    
    // Make a copy of the SQL and save it
    stmt->sql = strndup(zSql, stmt_len);
    
    mysql_stmt_prepare(stmt->mystmt, stmt->sql, stmt_len);
    
    stmt->nparams = mysql_stmt_param_count(stmt->mystmt);
    if (stmt->nparams > 0)
    {
        stmt->myparams = (MYSQL_BIND*)malloc(sizeof(MYSQL_BIND)*stmt->nparams);
    }
    
    /* Result set information */
    stmt->myres = mysql_stmt_result_metadata(stmt->mystmt);

    stmt->ncolumns = 0;
    if (stmt->myres)
    {
        /* Get total columns in the query */
        stmt->ncolumns = mysql_num_fields(stmt->myres);
        
        // Set STMT_ATTR_UPDATE_MAX_LENGTH attribute
        my_bool aBool = 1;
        mysql_stmt_attr_set(stmt->mystmt, STMT_ATTR_UPDATE_MAX_LENGTH, &aBool);
        
        if (stmt->ncolumns > 0)
        {
            stmt->results = malloc(sizeof(MYSQL_BIND)* stmt->ncolumns);
            stmt->lengths = malloc(sizeof(unsigned long)* stmt->ncolumns);
            memset(stmt->results, 0, sizeof(MYSQL_BIND)* stmt->ncolumns);
            
            int i;
            for (i=0; i < stmt->ncolumns; i++)
            {
                //stmt->results[i].buffer          = malloc(stmt->myres->fields[i].max_length);
                //stmt->results[i].buffer_type     = stmt->myres->fields[i].type;
                //stmt->results[i].buffer_length   = stmt->myres->fields[i].max_length;
                
                stmt->results[i].buffer          = 0;
                stmt->results[i].is_null         = 0;
                stmt->results[i].buffer_length   = 0;
                stmt->results[i].length          = &stmt->lengths[i];
            }
        }
    }
    
    *ppStmt = (mysqlite3_stmt*)stmt;
    
    if (pzTail)
    {
        (*pzTail) = NULL;
        if (stmt_len < strlen(zSql))
        {
            (*pzTail) = zSql+stmt_len;
            if (*pzTail[0] == ';') {
                (*pzTail)++;
            }
            if (*pzTail[0] == '\0') {
                *pzTail = NULL;
            }
        }
    }
    
    return rc;
};

int sqlite3_prepare_v2(sqlite3* db,              /* Database handle */
                       const char* zSql,         /* SQL statement, UTF-8 encoded */
                       int nBytes,               /* Maximum length of zSql in bytes. */
                       mysqlite3_stmt** ppStmt,  /* OUT: Statement handle */
                       const char** pzTail       /* OUT: Pointer to unused portion of zSql */
)
{
    return sqlite3_prepare(db, zSql, nBytes, ppStmt, pzTail);
};


int sqlite3_step(mysqlite3_stmt* stmt)
{
    int rc = SQLITE_ERROR;
    
    if (stmt->executed == 0)
    {
        if (stmt->nparams > 0)
        {
            if (mysql_stmt_bind_param(stmt->mystmt, stmt->myparams))
            {
                rc = SQLITE_ERROR;
                goto done;
            }
        }
        
        if (mysql_stmt_execute(stmt->mystmt))
        {
            printf("Statement execute failed: %s\n", mysql_stmt_error(stmt->mystmt));
            rc = SQLITE_ERROR;
            goto done;
        }
        
        stmt->executed = 1;

        if (stmt->ncolumns > 0)
        {
            if (mysql_stmt_store_result(stmt->mystmt))
            {
                printf("mysql_stmt_store_result failed: %s\n",
                       mysql_stmt_error(stmt->mystmt));
                rc = SQLITE_ERROR;
            }
            
            stmt->nresults = mysql_stmt_num_rows(stmt->mystmt);
            
            if (mysql_stmt_bind_result(stmt->mystmt, stmt->results))
            {
                printf("mysql_stmt_bind_result: %s\n",
                       mysql_stmt_error(stmt->mystmt));
                rc = SQLITE_ERROR;
            }
        }
    }
    
    if (stmt->results)
    {
        // XXX: Dunno why this returns an unknown (blank) error, but it
        //      seems to be working fine...
        if (mysql_stmt_fetch(stmt->mystmt))
        {
            //fprintf(stderr, "Statement fetch failed (%d): %s\n",
            //       mysql_stmt_errno(stmt->mystmt),
            //       mysql_stmt_error(stmt->mystmt));
        }
        
        stmt->nrow++;
    }
    
    
    if (stmt->nrow == stmt->nresults) {
        rc = SQLITE_DONE;
    }
    
    else if (stmt->nrow < stmt->nresults) {
        rc = SQLITE_ROW;
    }
    
done:
    return rc;
};


int sqlite3_finalize(mysqlite3_stmt* pStmt)
{
    if (pStmt)
    {
        if (pStmt->sql)
        {
            free(pStmt->sql);
        }
        
        if (pStmt->mystmt)
        {
            mysql_stmt_free_result(pStmt->mystmt);
            mysql_stmt_close(pStmt->mystmt);

        }
        
        int i;
        for (i=0; i < pStmt->nparams; i++)
        {
            // Need to clean up buffers
            if (pStmt->myparams[i].buffer)
            {
                free(pStmt->myparams[i].buffer);
            }
            
            if (pStmt->results && pStmt->results[i].buffer)
            {
                free(pStmt->results[i].buffer);
            }
        }
        
        if (pStmt->lengths) {
            free(pStmt->lengths);
        }
        
        if (pStmt->results) {
            free(pStmt->results);
        }
        
        if (pStmt->myparams)
        {
            free(pStmt->myparams);
        }
        
        mysql_free_result(pStmt->myres);
        
        free(pStmt);
    }
    return 0;
};


int sqlite3_close(mysqlite3* db)
{
    if (db)
    {
        // XXX: This segfaults...
        //if (db->mydb) { mysql_close(db->mydb); }
    }
    return 0;
};

int sqlite3_close_v2(mysqlite3* db)
{
    return sqlite3_close(db);
};

int sqlite3_bind_int(mysqlite3_stmt* stmt, int idx, int val)
{
    if (stmt->nparams > 0 && idx < stmt->nparams && idx >= 0)
    {
        memset(&stmt->myparams[idx], 0, sizeof(stmt->myparams[idx]));
        
        stmt->myparams[idx].buffer_type = MYSQL_TYPE_LONG;
        stmt->myparams[idx].buffer      = malloc(sizeof(val));
        memcpy(stmt->myparams[idx].buffer, &val, sizeof(val));
        stmt->myparams[idx].buffer_length = sizeof(val);
    }
    
    return 0;
};


int sqlite3_bind_null(mysqlite3_stmt* stmt, int idx)
{
    if (stmt->nparams > 0 && idx < stmt->nparams && idx >= 0)
    {
        memset(&stmt->myparams[idx], 0, sizeof(stmt->myparams[idx]));
        
        stmt->myparams[idx].buffer_type   = MYSQL_TYPE_NULL;
        stmt->myparams[idx].buffer        = NULL;
        stmt->myparams[idx].buffer_length = 0;
    }
    
    return 0;
};

/*
**  More details: 
**      https://dev.mysql.com/doc/refman/5.0/en/c-api-prepared-statement-data-structures.html
*/
int sqlite3_bind_text(mysqlite3_stmt* stmt, int idx, const char* val,int nBytes,void(*callback)(void*))
{
    if (stmt->nparams > 0 && idx < stmt->nparams && idx >= 0)
    {
        unsigned long length = nBytes;
        if (nBytes < 0) {
            length = strlen(val);
        }
        
        memset(&stmt->myparams[idx], 0, sizeof(stmt->myparams[idx]));
        
        stmt->myparams[idx].buffer_type   = MYSQL_TYPE_STRING;
        stmt->myparams[idx].buffer        = (char*)strndup(val, length);
        stmt->myparams[idx].buffer_length = length;
        
        if (callback) {
            callback((void*)val);
        }
    }
    
    return 0;
};

int sqlite3_column_int(mysqlite3_stmt* stmt, int iCol)
{
    MYSQL_BIND* result = &stmt->results[iCol];
    
    result->buffer_type = MYSQL_TYPE_LONG;
    if (result->length)
    {
        unsigned long length = *result->length;
        if (result->buffer)
        {
            free(result->buffer);
        }
        
        result->buffer        = (int*)malloc(length);
        result->buffer_length = length;
    }
    
    mysql_stmt_fetch_column(stmt->mystmt, result, iCol, 0);
    return *(int*)result->buffer;
};

const unsigned char* sqlite3_column_text(mysqlite3_stmt* stmt, int iCol)
{
    MYSQL_BIND* result = &stmt->results[iCol];

    result->buffer_type = MYSQL_TYPE_STRING;
    if (result->length)
    {
        unsigned long length = *result->length;
        if (result->buffer)
        {
            free(result->buffer);
        }
        
        // The +1 for the \0 at the end of the string
        result->buffer        = (unsigned char*)malloc(length+1);
        memset(result->buffer, 0, length+1);
        
        // Don't tell MySQL about the extra byte at the end so that we return
        // a proper zero-byte terminated string.
        result->buffer_length = length;
    }
    
    mysql_stmt_fetch_column(stmt->mystmt, result, iCol, 0);
    return (const unsigned char*)result->buffer;
};
