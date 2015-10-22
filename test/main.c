//
//  main.c
//  test
//
//  Created by Weston Nielson on 10/21/15.
//  Copyright © 2015 Weston Nielson. All rights reserved.
//

#include <stdio.h>
#include <sqlite3.h>

int main(int argc, const char * argv[]) {
    sqlite3* db;
    sqlite3_stmt* res;
    
    sqlite3_open("test.db", &db);
    
    /*
    ** Insert some preformatted data
    */
    const char* sql = "DROP TABLE IF EXISTS Cars;"
                        "CREATE TABLE Cars(Id INT, Name TEXT, Price INT);"
                        "INSERT INTO Cars VALUES(1, 'Audi', 52642);"
                        "INSERT INTO Cars VALUES(3, 'Skoda', 9000);"
                        "INSERT INTO Cars VALUES(4, 'Volvo', 29000);"
                        "INSERT INTO Cars VALUES(5, 'Bentley', 350000);"
                        "INSERT INTO Cars VALUES(6, 'Citroen', 21000);"
                        "INSERT INTO Cars VALUES(7, 'Hummer', 41400);"
                        "INSERT INTO Cars VALUES(8, 'Volkswagen', 21600);";
    const char* tail = sql;
    while (1)
    {
        sqlite3_prepare(db, tail, -1, &res, &tail);
        sqlite3_step(res);
        sqlite3_finalize(res);
        
        if (tail == NULL)
        {
            break;
        }
    }
    
    /*
    **  Manual insert, using bound parameters
    */
    sqlite3_prepare(db, "INSERT INTO Cars VALUES(?, ?, ?)", -1, &res, NULL);
    sqlite3_bind_int(res,  0, 2);
    sqlite3_bind_text(res, 1, "Mercedes", -1, NULL);
    sqlite3_bind_int(res,  2, 57127);
    sqlite3_step(res);
    sqlite3_finalize(res);
    
    /*
    **  Select results using "proper" data types
    */
    sqlite3_prepare(db, "SELECT * FROM Cars ORDER BY Id ASC", -1, &res, NULL);
    while (1)
    {
        int rc = sqlite3_step(res);
        printf("%d %s %d\n", sqlite3_column_int(res, 0),
                             sqlite3_column_text(res, 1),
                             sqlite3_column_int(res, 2));
        if (rc == SQLITE_DONE || rc == SQLITE_ERROR)
        {
            break;
        }
    }
    sqlite3_finalize(res);
    
    printf("\n");
    
    /*
    **  Select results treating everything as text
    */
    sqlite3_prepare(db, "SELECT * FROM Cars WHERE Id > 5", -1, &res, NULL);
    while (1)
    {
        int rc = sqlite3_step(res);
        printf("%s %s %s\n", sqlite3_column_text(res, 0),
                             sqlite3_column_text(res, 1),
                             sqlite3_column_text(res, 2));
        if (rc == SQLITE_DONE || rc == SQLITE_ERROR)
        {
            break;
        }
    }
    sqlite3_finalize(res);
    
    
    /*
    **  Cleanup
    */
    sqlite3_close(db);
    
    return 0;
}
