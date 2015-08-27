/**
  * inmdb.h IPC management
 **/

#ifndef LIB_INMEMORYDB_H_
#define LIB_INMEMORYDB_H_

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/sem.h>
#include <errno.h>
#include <stdint.h>

class CInmemoryDB{
private:
    static const int kMaxNumOfTable = 100;
    static const int kOffsetTableSize = 101; //should always be kMaxNumOfTable+1
    static const int kMaxNumOfSems  = 100;
    //datamember
    void *pShmData;
    int semID;
    int shmID;
    bool threadSafeFlag;
public:
    CInmemoryDB();
    ~CInmemoryDB();
    int create(const char *ipcPathName,int ipcid,int shmSize,int semNum, int operatorFlag = 0666);
    int connect(const char *ipcPathName,int ipcid,int accessFlag = 0666);
    int createTable(int tableid,int tableSize);
    void * getTablePData(int tableid);
    inline int getDBSize(void) {return *(int *)area_1();} // Get size for entire shared memory
    int getTableSize(int tableid);
    bool lock(int tableid);
    bool unLock(int tableid);
    int releaseInmemDB(void);
    bool exist(void);
    inline int getShmId() { return shmID; }
    int detatchShm(void);
    void threadSafe(bool v);
private:
    int releaseShm();
    int releaseSem();

    /* Point and length related issues */
    // memory struct: table offset table, shmSize, semNum, threadsafe flag, tables
    inline intptr_t startPoint(){return (intptr_t)pShmData;}

    inline int offset0(){return 0;} //integer array, store the offset for tables.
    inline int length0(){return sizeof(int)*kOffsetTableSize;}//length for tableOffset array
    inline intptr_t area_0(){return startPoint()+offset0();}

    inline int offset1(){return offset0()+length0();}//shmSize, integer
    inline int length1(){return sizeof(int);}//length for shmSize
    inline intptr_t area_1(){return startPoint()+offset1();}

    inline int offset2(){return offset1()+length1();}//semNum, integer
    inline int length2(){return sizeof(int);}//length for semNum
    inline intptr_t area_2(){return startPoint()+offset2();}

    inline int offset3(){return offset2()+length2();}//threadsafe flag, bool
    inline int length3(){return sizeof(bool);}//length for threadsafe flag
    inline intptr_t area_3(){return startPoint()+offset3();}

    inline int offset4(){return offset3()+length3();}//The start place for tables
    inline int length4(){return (*(int*)area_1())-offset4();}//length for tables
    inline intptr_t area_4(){return startPoint()+offset4();}
};
#endif  //LIB_INMEMORYDB_H_
