#!/usr/bin/python2.7
#
# Assignment3 Interface
#
import threading
import thread
import psycopg2
import os
import sys

##################### This needs to changed based on what kind of table we want to sort. ##################
##################### To know how to change this, see Assignment 3 Instructions carefully #################
FIRST_TABLE_NAME = 'ratings'
SECOND_TABLE_NAME = 'movies'
SORT_COLUMN_NAME_FIRST_TABLE = 'Rating'
SORT_COLUMN_NAME_SECOND_TABLE = 'MovieId1'
JOIN_COLUMN_NAME_FIRST_TABLE = 'MovieID'
JOIN_COLUMN_NAME_SECOND_TABLE = 'MovieId1'

##########################################################################################################


def joinThread(openconnection,thread_Index,tableName1,tableName2, columnName1,columnName2,OutputTable):
    curs=openconnection.cursor()
    try:
        curs.execute("DROP TABLE IF EXISTS "+OutputTable)
    except psycopg2.DatabaseError, error:
        print "Table "+OutputTable+" does not exist.!!"
    print "inside join thread"
    curs.execute("CREATE TABLE "+OutputTable+ " AS (SELECT * FROM %s JOIN %s ON %s.%s=%s.%s)" %(tableName1,tableName2,tableName1,columnName1,tableName2,columnName2))
    curs.execute("DROP TABLE "+tableName1+","+tableName2)
    openconnection.commit()



def sortThread(openconnection,thread_Index, tableName, OutputTable, SortingColumnName ):
    curs = openconnection.cursor()
    try:
        curs.execute("DROP TABLE IF EXISTS "+OutputTable + " CASCADE")
    except psycopg2.DatabaseError, error:
        print "Table "+OutputTable+" does not exist.!!"
    print "inside thread"
    print SortingColumnName
    curs.execute("CREATE TABLE "+OutputTable+" AS (SELECT * FROM %s ORDER BY %s)" %(tableName,SortingColumnName))
    curs.execute("DROP TABLE IF EXISTS "+tableName+" CASCADE")
    openconnection.commit()



def rangePartition(tableName1, tableName2, numberofpartitions, openconnection, columnName1, columnName2):
    N1 = "Range" + tableName1 + "Part"
    N2 = "Range" + tableName2 + "Part"
    print "inside range"
    try:
        curs = openconnection.cursor()
        curs.execute("select * from information_schema.tables where table_name='%s'" % tableName1)
        if not bool(curs.rowcount):
            print "Load Ratings Table"
            return


        print " after load"
        curs.execute("select min(%s),max(%s) from %s" % (columnName1, columnName1, tableName1))
        results = curs.fetchall()
        MinVal = results[0][0]
        MaxVal = results[0][1]

        i = 0;
        x = (MaxVal - MinVal) / (float)(numberofpartitions)
        while MinVal < MaxVal:
            l = MinVal
            r = MinVal + x

            if i == 0:
                t = N1 + `i`
                curs.execute("CREATE TABLE IF NOT EXISTS %s AS (SELECT * FROM %s WHERE %s >= %f AND %s <= %f)" %(t,tableName1,columnName1,l,columnName1, r))
                if tableName2 != '':
                    t = N2 + `i`
                    curs.execute("CREATE TABLE IF NOT EXISTS %s AS (SELECT * FROM %s WHERE %s >= %f AND %s <= %f)" %(t,tableName2,columnName2,l,columnName2, r))
                openconnection.commit()

            if i != 0:
                t = N1 + `i`
                curs.execute("CREATE TABLE IF NOT EXISTS %s AS (SELECT * FROM %s WHERE %s > %f AND %s <= %f)" %(t,tableName1,columnName1,l,columnName1, r))
                if tableName2 != '':
                    t = N2 + `i`
                    curs.execute("CREATE TABLE IF NOT EXISTS %s AS (SELECT * FROM %s WHERE %s > %f AND %s <= %f)" %(t,tableName2,columnName2,l,columnName2, r))
                openconnection.commit()
            MinVal = r
            i += 1;
        openconnection.commit()
    except psycopg2.DatabaseError, error:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % error
        sys.exit(1)
    except IOError, error:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % error
        sys.exit(1)
    finally:
        if curs:
            curs.close()

def ParallelSort(InputTable, SortingColumnName, OutputTable, openconnection):
    curs = openconnection.cursor()
    print "in parallel sort"
    rangePartition(InputTable, '', 5, openconnection, SortingColumnName, '');
    print " after range"
    try:
        curs.execute("DROP TABLE IF EXISTS "+OutputTable+" CASCADE" )
    except psycopg2.DatabaseError, error:
        print "Table"+ OutputTable+"dosen't exist"
    curs.execute("CREATE TABLE "+OutputTable+" AS (SELECT * FROM "+InputTable+" WHERE 1=2)")
    tableName = "Range" + InputTable + "Part"

    thr1 = threading.Thread(target=sortThread,args=(openconnection,0,tableName + "0", OutputTable + "part0", SortingColumnName))
    thr2 = threading.Thread(target=sortThread,args=(openconnection,1,tableName + "1", OutputTable + "part1", SortingColumnName))
    thr3 = threading.Thread(target=sortThread,args=(openconnection,2,tableName + "2", OutputTable + "part2", SortingColumnName))
    thr4 = threading.Thread(target=sortThread,args=(openconnection,3,tableName + "3", OutputTable + "part3", SortingColumnName))
    thr5 = threading.Thread(target=sortThread,args=(openconnection,4,tableName + "4", OutputTable + "part4", SortingColumnName))
    thr1.start()
    thr2.start()
    thr3.start()
    thr4.start()
    thr5.start()
    print "after start"

    thr1.join()
    thr2.join()
    thr3.join()
    thr4.join()
    thr5.join()
    print "after join"

    newOutputTable = OutputTable+"part"
    for part in range(0, 5):
        curs.execute("INSERT INTO %s (SELECT * FROM %s)" % (OutputTable, OutputTable + "part" + str(part)))

    print "end"
    openconnection.commit()



def ParallelJoin(InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    curs = openconnection.cursor()
    rangePartition(InputTable1, InputTable2, 5, openconnection, Table1JoinColumn, Table2JoinColumn);


    try:
        curs.execute("DROP TABLE IF EXISTS " +OutputTable)
    except psycopg2.DatabaseError, error:
        print "Table "+OutputTable +" dosen't exist"
    curs.execute("CREATE TABLE "+OutputTable+" AS (SELECT * FROM %s,%s WHERE 1=2)" % (InputTable1, InputTable2))
    name1 = "Range" + InputTable1 + "Part"
    name2 = "Range" + InputTable2 + "Part"

    thr1 = threading.Thread(target=joinThread,args=(openconnection,0,name1 + "0", name2 + "0", Table1JoinColumn, Table2JoinColumn,OutputTable + "part0"))
    thr2 = threading.Thread(target=joinThread,args=(openconnection,1,name1 + "1", name2 + "1", Table1JoinColumn, Table2JoinColumn,OutputTable + "part1"))
    thr3 = threading.Thread(target=joinThread,args=(openconnection,2,name1 + "2", name2 + "2", Table1JoinColumn, Table2JoinColumn,OutputTable + "part2"))
    thr4 = threading.Thread(target=joinThread,args=(openconnection,3,name1 + "3", name2 + "3", Table1JoinColumn, Table2JoinColumn,OutputTable + "part3"))
    thr5 = threading.Thread(target=joinThread,args=(openconnection,4,name1 + "4", name2 + "4", Table1JoinColumn, Table2JoinColumn,OutputTable + "part4"))
    thr1.start()
    thr2.start()
    thr3.start()
    thr4.start()
    thr5.start()

    thr1.join()
    thr2.join()
    thr3.join()
    thr4.join()
    thr5.join()

    newOutputTable = OutputTable+"part"
    for part in range(0, 5):
        curs.execute("INSERT INTO "+OutputTable+" (SELECT * FROM %s)" % (OutputTable + "part" + str(part)))

    print "end join"
    openconnection.commit()


################### DO NOT CHANGE ANYTHING BELOW THIS #############################


# Donot change this function
def getOpenConnection(user='postgres', password='1234', dbname='ddsassignment3'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


# Donot change this function
def createDB(dbname='ddsassignment3'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.commit()
    con.close()


# Donot change this function
def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()


# Donot change this function
def saveTable(ratingstablename, fileName, openconnection):
    try:
        cursor = openconnection.cursor()
        cursor.execute("Select * from %s" % (ratingstablename))
        data = cursor.fetchall()
        openFile = open(fileName, "w")
        for row in data:
            for d in row:
                openFile.write(`d` + ",")
            openFile.write('\n')
        openFile.close()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()


if __name__ == '__main__':
    try:
        # Creating Database ddsassignment2
        print "Creating Database named as ddsassignment3"
        createDB();

        # Getting connection to the database
        print "Getting connection from the ddsassignment3 database"
        con = getOpenConnection();
        # tester.loadRatings(FIRST_TABLE_NAME, 'ratings1.dat', con);
        # tester.loadMovies(SECOND_TABLE_NAME, 'movies.dat', con);
        # Calling ParallelSort
        print "Performing Parallel Sort"
        ParallelSort(FIRST_TABLE_NAME, SORT_COLUMN_NAME_FIRST_TABLE, 'parallelSortOutputTable', con);

        # Calling ParallelJoin
        print "Performing Parallel Join"
        ParallelJoin(FIRST_TABLE_NAME, SECOND_TABLE_NAME, JOIN_COLUMN_NAME_FIRST_TABLE, JOIN_COLUMN_NAME_SECOND_TABLE,
                     'parallelJoinOutputTable', con);

        # Saving parallelSortOutputTable and parallelJoinOutputTable on two files
        saveTable('parallelSortOutputTable', 'parallelSortOutputTable.txt', con);
        saveTable('parallelJoinOutputTable', 'parallelJoinOutputTable.txt', con);

        # Deleting parallelSortOutputTable and parallelJoinOutputTable
        deleteTables('parallelSortOutputTable', con);
        deleteTables('parallelJoinOutputTable', con);

        if con:
            con.close()

    except Exception as detail:
        print "Something bad has happened!!! This is the error ==> ", detail
