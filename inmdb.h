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

class CInmemoryDB{
private:
    static const int kMaxNumOfTable = 100;
    static const int kMaxNumOfSems  = 256;
    //datamember
    void *pShmData;
    int semID;
    int shmID;
public:
    CInmemoryDB();
    ~CInmemoryDB();
    int create(const char *ipcPathName,int ipcid,int shmSize,int semNum, int operatorFlag = 0600);
    int connect(const char *ipcPathName,int ipcid,int accessFlag = 0);
    int createTable(int tableid,int tableSize);
    void * getTablePData(int tableid);
    int getDBSize(void);
    int getTableSize(int tableid);
    int lock(int tableid);
    int unLock(int tableid);
    int releaseInmemDB(void);
    int isExist(void);
    inline int getShmId() { return shmID; };
    int detatchShm(void);
private:
    int releaseShm();
    int releaseSem();
};
#endif  //LIB_INMEMORYDB_H_
