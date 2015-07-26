/**
  * inmemorydb.h IPC management
 **/

#ifndef __INMEMORYDB_H__
#define __INMEMORYDB_H__

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/sem.h>
#include <errno.h>

#define MAXNUMOFTABLE	100
#define MAXNUMOFSEMS	256
class CInmemoryDB{
private:
	//datamember
	void *pShmData;
	int semID;
	int shmID;
public:	
        CInmemoryDB(){pShmData = NULL;semID = -1;shmID = -1;};
        ~CInmemoryDB(){
		if (pShmData != NULL) {
			shmdt((char *)pShmData);
			pShmData = NULL;
		}
	};
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
	int getShmId(){
		return	shmID;
	};
private:
	int releaseShm();
	int releaseSem();
public:
	int detatchShm(void);
};
#endif
