#include "inmdb.h"
#include "inmdb_log.h"
#include <sys/types.h>
#include <unistd.h>
#include <time.h>
#include <assert.h>

CInmemoryDB::CInmemoryDB() :
        pShmData(NULL),
        semID(-1),
        shmID(-1),
        threadSafeFlag(true)
{
}

CInmemoryDB::~CInmemoryDB()
{
    if (pShmData != NULL) {
        shmdt((char *)pShmData);
        pShmData = NULL;
    }
}

/**********************************************
 Function: create:ipc create
 parameter:
 Description:
 Returns:0 fail
         1 success
         2 already existed

 Error Number:
 *********************************************/
int CInmemoryDB::create(const char *ipcPathName, int ipcid, int shmSize, int semNum, int operatorFlag)
{
    assert(ipcid >=0);
    assert(ipcid <= 255);
    assert(ipcPathName != NULL);
    assert(strlen(ipcPathName) > 0);
    assert(shmSize > 0);
    assert(semNum >= 0);
    assert(semNum <= kMaxNumOfSems);

    threadSafeFlag = (semNum>0);

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
        ushort *array;
    }arg;
    ushort ar[kMaxNumOfSems];
    memset(&arg,0,sizeof(arg));
    memset(ar, 0, kMaxNumOfSems*sizeof(ushort));
    for (int i=0; i<semNum; i++) {ar[i] = 1;};
    arg.array = ar;

    if (semctl(semID, 0, SETALL, arg) == -1) {
        inmdb_log(LOGDEBUG,"semctl SETALL fail, errno: %d", errno);
        releaseShm();
        releaseSem();
        return 0;
    }

    pShmData = shmat(shmID,NULL,0);

    if((intptr_t)pShmData == -1){
        inmdb_log(LOGDEBUG,"shmat fail, errno: %d.", errno);
        releaseShm();
        releaseSem();
        return 0;
    }

    memset(pShmData, 0, shmSize);
    for(int offsetindex = 0; offsetindex < kOffsetTableSize; offsetindex++){
        // Default tableoffset value is -1, means not assigned
        int flag = -1;
        memcpy((void *)(area_0() + sizeof(int)*offsetindex),
               &flag,
               sizeof(int));
    }

    //memcpy((void *)((intptr_t)pShmData + sizeof(int)*kMaxNumOfTable),&shmSize,sizeof(int));
    memcpy((void*)area_1(), &shmSize, length1());
    //memcpy((void *)((intptr_t)pShmData + sizeof(int)*kMaxNumOfTable + sizeof(int)),&semNum,sizeof(int));
    memcpy((void*)area_2(), &semNum, length2());
    memcpy((void*)area_3(), &threadSafeFlag, length3());

    //detach, a connect must be called obviously for use shared memory.
    shmdt(pShmData);
    pShmData = NULL;
    return 1;
}

/**********************************************
 Function: connect:ipc connect
 Description:
 parameter:
 Returns:0 fail
         1 Success
 Error Number:
 *********************************************/
int CInmemoryDB::connect(const char *ipcPathName,int ipcid,int accessFlag)
{
    assert(ipcid >=0);
    assert(ipcid <= 255);
    assert(ipcPathName != NULL);
    assert(strlen(ipcPathName) > 0);

    key_t ipckey = ftok(ipcPathName,ipcid);
    if(ipckey == -1){
        inmdb_log(LOGDEBUG,"ftok error: %d", errno);
        return 0;
    }

    shmID = shmget(ipckey,0,IPC_CREAT|accessFlag);
    if(shmID == -1){
        //The shared memory is not existed
        inmdb_log(LOGDEBUG,"shmget error: %d, ipcPathName=%s ipcid=%d ipckey = %d",
                  errno, ipcPathName, ipcid, ipckey);
        return 0;
    }

    semID = semget(ipckey,0 ,IPC_CREAT|accessFlag);
    if(semID == -1){
        inmdb_log(LOGDEBUG,"semget error: %d, ipcPathName=%s ipcid=%d ipckey = %d",
                  errno, ipcPathName, ipcid, ipckey);
        shmID = -1;
        return 0;
    }
    pShmData = shmat(shmID,NULL,0);

    if((intptr_t)pShmData == -1){
        inmdb_log(LOGDEBUG,"shmat error: %d, ipcPathName=%s ipcid=%d shmID = %d",
                  errno, ipcPathName, ipcid, shmID);
        semID = -1;
        shmID = -1;
        return 0;
    }
    threadSafeFlag = *(bool*)area_3();
    inmdb_log(LOGDEBUG,"Succeed, ipcPathName=%s ipcid=%d shmID=%d semID=%d",ipcPathName,ipcid,shmID,semID);
    return 1;
}

/**********************************************
 Function: createTable:allocate ipc resource for table
 Description:
 parameter:
 Returns: 0 allocate table fail
          1 allocate table success
          2 already existed
 Error Number:
 *********************************************/
int CInmemoryDB::createTable(int tableid,int tableSize)
{
    assert(tableid >= 0);
    assert(tableid < kMaxNumOfTable);
    assert(tableSize > 0);

    int *tableoffset = (int*)pShmData;
    if(tableoffset[tableid] > 0){
        return 2;
    }

    int tableOffsetStart = offset4();

    if((tableid > 0) && (tableoffset[tableid - 1] < 0)){
        // tableid must be used sequentially in create
        // From 0 to kMaxNumOfTable-1, could not jump.
        inmdb_log(LOGDEBUG,
                  "Table ID must be used sequentially in create phase. ID %d can not be use before previous tables are created.",
                  tableid);
        return 0;
    }
    else if (tableid >0){
        tableOffsetStart = -1 *tableoffset[tableid];
    }

    int nextTableOffset = tableOffsetStart + tableSize;
    if(nextTableOffset > getDBSize()){
        inmdb_log(LOGDEBUG,"Table end address offset(%d) beyond the db size. Can not create table.", nextTableOffset);
        return 0;
    }

    tableoffset[tableid] = tableOffsetStart;
    tableoffset[tableid+1] = -1 * nextTableOffset;
    return 1;
}

/**********************************************
 Function: getTablePData:allocate ipc resource for table
 Description:
 parameter:
 Returns: NULL:error
 Error Number:
 *********************************************/
void * CInmemoryDB::getTablePData(int tableid)
{
    int *tableoffset = (int *)pShmData;
    if(tableoffset[tableid] < 0){
        return (void *) NULL;
    }
    return (void *)((intptr_t)pShmData + tableoffset[tableid]);
}

/**********************************************
 Function: getTableSize
 Description: Return the size for specified table in MB
 *********************************************/
int CInmemoryDB::getTableSize(int tableid)
{
    int *tableoffset = (int *)pShmData;
    if(tableid < 0 ||
       tableid >= kMaxNumOfTable ||
       tableoffset[tableid] <0){
        inmdb_log(LOGDEBUG,"tableid(%d) invalid",tableid);
        return 0;
    }
    return (tableoffset[tableid + 1] - tableoffset[tableid])/(1000*1000);
}

void CInmemoryDB::threadSafe(bool v)
{
    if((void*)startPoint() == NULL) return;
    threadSafeFlag = v;
    memcpy((void*)area_3(), &threadSafeFlag, length3());
}

/**********************************************
 Function: lock
 Description:
 parameter:
 Returns:N/A
 Error Number:
 *********************************************/
 bool CInmemoryDB::lock(int tableid)
{
    if(!threadSafeFlag) return true;
    assert(tableid>=0);
    assert(tableid<kMaxNumOfTable);
    if ( semID < 0 ){
        inmdb_log(LOGDEBUG,"semID(%d) invalid",semID);
        return false;
    }
    struct sembuf sbuf;
    sbuf.sem_num= tableid;
    sbuf.sem_op = -1;
    sbuf.sem_flg = SEM_UNDO;
    if(semop(semID, &sbuf, 1) == -1 ){
        inmdb_log(LOGDEBUG,"semop operate error: %d, tableid = %d semID = %d",
                  errno, tableid, semID);
        return false;
    }
    inmdb_log(LOGDEBUG,"lock succeed, semval for table %d is %d after lock.",tableid, semctl(semID, tableid, GETVAL));
    return true;
}

/**********************************************
 Function: unLock:unLock
 Description:
 parameter:
 Returns:0 error 1success
 Error Number:
 *********************************************/
bool CInmemoryDB::unLock(int tableid)
{
    if(!threadSafeFlag) return true;
    assert(tableid>=0);
    assert(tableid<kMaxNumOfTable);
    if ( semID < 0 ){
        inmdb_log(LOGDEBUG,"semID(%d) invalid.",semID);
        return false;
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
            return false;
        }
    }
    inmdb_log(LOGDEBUG,"unlock succeed, semval for table %d is %d after unlock.",tableid, semctl(semID, tableid, GETVAL));
    return true;
}

/**********************************************
 Function: unLock:releaseInmemDB
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
 Function: exist
 Description: return if the shared memory is created or not
 *********************************************/
 bool CInmemoryDB::exist(void)
{
    struct shmid_ds ds;
    memset(&ds, 0, sizeof(struct shmid_ds));
    if ( shmID >= 0 && shmctl (shmID, IPC_STAT, &ds) >= 0){
        return true;
    }
    inmdb_log(LOGDEBUG,"shmID(%d) invalid", shmID);
    return false;
}

/**********************************************
 Function: releaseShm:releaseShm
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
 Function: releaseSem:releaseSem
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
