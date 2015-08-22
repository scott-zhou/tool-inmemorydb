#include "inmdb.h"
#include "inmdb_log.h"
#include <sys/types.h>
#include <unistd.h>
#include <time.h>

/**********************************************
 Title:create:ipc create
 parameter:
 Description:
 Returns:0 fail
         1 success
         2 already existed

 Error Number:
 *********************************************/

int CInmemoryDB::create(const char *ipcPathName, int ipcid, int shmSize, int semNum, int operatorFlag)
{
    //ipcid is limited between 1 and 255,and ipcPathName is not empty;
    if(ipcid < 1 || ipcid > 255){
        inmdb_log(LOGDEBUG,"ipcid (%d) out of range.",ipcid);
        return 0;
    }
    if(ipcPathName == NULL || ipcPathName[0] == 0){
        inmdb_log(LOGDEBUG,"ipcPathName(%p) is NULL or blank.", ipcPathName);
        return 0;
    }

    //shmSize and semNum ard limited
    if(shmSize <= 0 || semNum < 1 || semNum > MAXNUMOFSEMS){
        inmdb_log(LOGDEBUG,"shmSize(%d)or semNum(%d) error < 1.",shmSize,semNum);
        return 0;
    }

    key_t ipckey = ftok(ipcPathName,ipcid);
    if(ipckey == -1){
        inmdb_log(LOGDEBUG,"ftok(ipcPathName(%s),ipcid(%d)) error.",ipcPathName,ipcid);
        return 0;
    }

    shmID = shmget(ipckey,0,IPC_CREAT|operatorFlag);
    if(shmID > -1){
        //The shared memory is already existed.
        return 2;
    }

    shmID = shmget(ipckey,shmSize,IPC_CREAT|operatorFlag);
    if(shmID == -1){
        inmdb_log(LOGDEBUG,"shmget error: %d. ipckey = %d shmSize=%d", errno,ipckey,shmSize);
        return 0;
    }

    semID = semget(ipckey,semNum ,IPC_CREAT|operatorFlag);
    if(shmID == -1){
        inmdb_log(LOGDEBUG,"semget error: %d. ipckey = %d semNum=%d", errno, ipckey, semNum);
        releaseShm();
        return 0;
    }


    union semum {
        int val;
        struct semid_ds *buf;
        ushort array[MAXNUMOFSEMS+1];
    }arg;

    memset(&arg,0,sizeof(arg));
    arg.val = 1;
    for (int i=0; i<semNum; i++) {
        if (semctl(semID, i, SETVAL, arg) == -1) {
            inmdb_log(LOGDEBUG,"semctl at set value error: %d", errno);
            releaseShm();
            releaseSem();
        }
    }

    pShmData = shmat(shmID,NULL,0);

    if((int)pShmData == -1){
        inmdb_log(LOGDEBUG,"shmat error: %d.", errno);
        releaseShm();
        releaseSem();
        return 0;
    }

    memset(pShmData, 0, shmSize);
    for(int offsetindex = 0; offsetindex < MAXNUMOFTABLE; offsetindex++){
        int flag = -1;
        memcpy((void *)((int)pShmData + 4*offsetindex),&flag,4);
    }

    memcpy((void *)((int)pShmData + 4*MAXNUMOFTABLE),&shmSize,4);
    memcpy((void *)((int)pShmData + MAXNUMOFTABLE*4 + 4),&semNum,4);


    shmdt(pShmData);
    pShmData = NULL;
    return 1;
}

/**********************************************
 Title:connect:ipc connect
 Description:
 parameter:
 Returns:0 fail
         1 Success
 Error Number:
 *********************************************/

int CInmemoryDB::connect(const char *ipcPathName,int ipcid,int accessFlag)
{
    if(ipcid < 1 || ipcid > 255){
        inmdb_log(LOGDEBUG,"ipcid(%d) is wrong.",ipcid);
        return 0;
    }
    if(ipcPathName == NULL || ipcPathName[0] == 0){
        inmdb_log(LOGDEBUG,"ipcPathName(%p) is NULL or blank.", ipcPathName);
        return 0;
    }

    key_t ipckey = ftok(ipcPathName,ipcid);
    if(ipckey == -1){
        inmdb_log(LOGDEBUG,"ftok error: %d", errno);
        return 0;
    }

    shmID = shmget(ipckey,0,IPC_CREAT);
    if(shmID == -1){
        //The shared memory is not existed
        inmdb_log(LOGDEBUG,"shmget error: %d, ipcPathName=%s ipcid=%d ipckey = %d",
                  errno, ipcPathName, ipcid, ipckey);
        return 0;
    }

    semID = semget(ipckey,0 ,IPC_CREAT);
    if(semID == -1){
        inmdb_log(LOGDEBUG,"semget error: %d, ipcPathName=%s ipcid=%d ipckey = %d",
                  errno, ipcPathName, ipcid, ipckey);
        shmID = -1;
        return 0;
    }
    pShmData = shmat(shmID,NULL,0);

    if((int)pShmData == -1){
        inmdb_log(LOGDEBUG,"shmat error: %d, ipcPathName=%s ipcid=%d shmID = %d",
                  errno, ipcPathName, ipcid, shmID);
        semID = -1;
        shmID = -1;
        return 0;
    }
    inmdb_log(LOGDEBUG,"Succeed, ipcPathName=%s ipcid=%d shmID = %d",ipcPathName,ipcid,shmID);
    return 1;
}

/**********************************************
 Title:createTable:allocate ipc resource for table
 Description:
 parameter:
 Returns: 0 allocate table fail
          1 allocate table success
          2 already existed

 Error Number:
 *********************************************/
 
int CInmemoryDB::createTable(int tableid,int tableSize)
{
    if(tableid < 0 || tableid > MAXNUMOFTABLE - 1 || tableSize <= 0){
        inmdb_log(LOGDEBUG,"tableid(%d) or tableSize(%d) invalid", tableid, tableSize);
        return 0;
    }

    int offset;
    int *tableoffset = (int*)pShmData;
    if(tableoffset[tableid] > 0){
        return 2;
    }

    if(tableid == 0){
        tableoffset[0] = MAXNUMOFTABLE*4 + 8;
    }
    else if(tableoffset[tableid - 1] < 0){
        return 0;
    }
    else{
        tableoffset[tableid] = -1 *tableoffset[tableid];
    }

    //set next table offset
    offset = (int)tableoffset[tableid] + tableSize;
    if(offset > getDBSize()){
        inmdb_log(LOGDEBUG,"table offset(%d) beyond the db size.", offset);
        return 0;
    }
    if(tableid < MAXNUMOFTABLE - 1){
        tableoffset[tableid+1] = -1 * offset;
    }
    return 1;
}

/**********************************************
 Title:getTablePData:allocate ipc resource for table
 Description:
 parameter:
 Returns: NULL:error
 Error Number:
 *********************************************/

void * CInmemoryDB::getTablePData(int tableid)
{
    int *tableoffset = (int *)pShmData;
    if((int)tableoffset[tableid] == -1){
        return (void *) NULL;
    }
    return (void *)((int)pShmData + tableoffset[tableid]);
}

/**********************************************
 Title:getDBSize:get dbshm size
 Description:
 parameter:
 Returns:
 Error Number:
 *********************************************/

int CInmemoryDB::getDBSize(void)
{
    int *dbsize =(int *)((int) pShmData + 4*MAXNUMOFTABLE);
    return *dbsize;
}

/**********************************************
 Title:getTableSize:get table size
 Description:
 parameter:
 Returns:
 Error Number:
 *********************************************/
int CInmemoryDB::getTableSize(int tableid)
{
    if(tableid < 0||tableid >MAXNUMOFTABLE ){
        inmdb_log(LOGDEBUG,"tableid(%d) invalid",tableid);
        return 0;
    }
    int *tableoffset = (int *)pShmData;
    return (tableoffset[tableid + 1] - tableoffset[tableid])/(1024*1024);
}

/**********************************************
 Title:lock:lock
 Description:
 parameter:
 Returns:N/A
 Error Number:
 *********************************************/
 int CInmemoryDB::lock(int tableid)
{
    if(tableid < 0||tableid >MAXNUMOFTABLE ){
        inmdb_log(LOGDEBUG,"tableid(%d) invalid",tableid);
        return 0;
    }
    if ( semID < 0 ){
        inmdb_log(LOGDEBUG,"semID(%d) invalid",semID);
        return 0;
    }
    struct sembuf sbuf;
    sbuf.sem_num= tableid;
    sbuf.sem_op = -1;
    sbuf.sem_flg = SEM_UNDO;
    if(semop(semID, &sbuf, 1) == -1 ){
        inmdb_log(LOGDEBUG,"semop operate error: %d, tableid = %d semID = %d",
                  errno, tableid, semID);
        return 0;
    }
    return 1;
}

/**********************************************
 Title:unLock:unLock
 Description:
 parameter:
 Returns:0 error 1success
 Error Number:
 *********************************************/
int CInmemoryDB::unLock(int tableid)
{
    if ( semID < 0 ){
        inmdb_log(LOGDEBUG,"semID(%d) invalid.",semID);
        return 0;
    }

    struct sembuf sbuf;
    sbuf.sem_num = tableid;
    sbuf.sem_op = 1;
    sbuf.sem_flg = SEM_UNDO;
    if(semop(semID, &sbuf, 1) == -1 )
    {
        if ( errno != EINTR ){
            inmdb_log(LOGDEBUG,"semop error: %d, tableid = %d semID = %d",
                      errno, tableid, semID);
            return 0;
        }
    }
    return 1;
}

/**********************************************
 Title:unLock:releaseInmemDB
 Description:
 parameter:
 Returns:0 error 1success
 Error Number:
 *********************************************/
 
int CInmemoryDB::releaseInmemDB(void)
{
    detatchShm();
    releaseShm();
    releaseSem();
    return 1;
}

/**********************************************
 Title:unLock:isExist
 Description:
 parameter:
 Returns:0 error 1success
 Error Number:
 *********************************************/
 int CInmemoryDB::isExist(void)
{
    struct shmid_ds ds;
    memset(&ds, 0, sizeof(struct shmid_ds));
    if ( shmID >= 0 ){
        if ( shmctl (shmID, IPC_STAT, &ds) < 0 ) {
            return 0;
        }
        else{
            return 1;
        }
    }
    else{
        inmdb_log(LOGDEBUG,"shmID(%d) invalid", shmID);
        return 0;
    }
    return 1;
}

/**********************************************
 Title:releaseShm:releaseShm
 Description:
 parameter:
 Returns:0 error 1success
 Error Number:
 *********************************************/
int CInmemoryDB::releaseShm(void)
{
    if ( shmID >= 0 ){
        if (shmctl(shmID, IPC_RMID, 0) < 0){
            inmdb_log(LOGDEBUG,"semctl at set value error: %d", errno);
            return 0;
        }
        else{
            shmID = - 1;
            pShmData = NULL;
            return 1;
        }
    }
    else{
        return 0;
    }
    return 1;
}

/**********************************************
 Title:releaseSem:releaseSem
 Description:
 parameter:
 Returns:0 error 1success
 Error Number:
 *********************************************/
int CInmemoryDB::releaseSem(void)
{
    if (semctl(semID, 0, IPC_RMID) < 0) {
        return -1;
    }
    semID = -1;
    return 1;
}

int CInmemoryDB::detatchShm(void)
{
    if (!pShmData)
        return 1;

    return (shmdt(pShmData)==0);
}

