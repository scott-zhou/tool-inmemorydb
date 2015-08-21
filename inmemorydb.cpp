#include "inmemorydb.h"
#include <sys/types.h>
#include <unistd.h>
#include <time.h>

/* Comment it for not output log
#define WRITE_LOGS_TO_FILE
*/
#ifdef WRITE_LOGS_TO_FILE
void writelog(const char *str,const char *ipcPathName,int ipcid,int shmID = -1);
void writelog(const char *str,const char *ipcPathName,int ipcid,int shmID)
{
    char fname[] = "/tmp/conn.log";
    FILE *fFd = fopen(fname, "a");

    char cmd[64],line[128];
    memset(cmd,0,sizeof(cmd));
    memset(line,0,sizeof(line));
    sprintf(cmd,"ps -e|grep %d|grep -v grep",getpid());

    FILE *f = popen(cmd,"r");
    char *s;
    while((s = fgets(line,128,f))){
        int id;
        sscanf(line,"%d",&id);
        //char tmp[128];
        //sprintf(tmp,"id=%d\n",id);
        //fwrite(line,1,strlen(line),fFd);
        //fwrite(tmp,1,strlen(tmp),fFd);

        if(id == getpid()){
            break;
        }
    }
    if(!s){
        memset(line,0,sizeof(line));
        sprintf(line,"WTF, can not find the process(%d) name.",getpid());
    }
    fclose(f);

    char haha[256*4];
    memset(haha,0,sizeof(haha));
    char tStr[128];
    time_t t = time(NULL);
    ctime_r(&t,tStr);
    if(tStr[strlen(tStr)-1] == '\n'){
        tStr[strlen(tStr)-1] = 0;
    }
    sprintf(haha,"time(%s):conn %s:%d(shmID:%d) %s -- (%d):%s",tStr,ipcPathName,ipcid,shmID,str,getpid(),line);
    fwrite(haha,1,strlen(haha),fFd);
    fclose(fFd);
}
#else
#define writelog(...) {;}
#endif

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
        printf("CInmemoryDB::create:ipcid(%d) is wrong.\n",ipcid);
        return 0;
    }
    if(ipcPathName == NULL || ipcPathName[0] == 0){
        printf("CInmemoryDB::create:ipcPathName is NULL or blank.\n");
        return 0;
    }

    //shmSize and semNum ard limited
    if(shmSize <= 0 || semNum < 1 || semNum > MAXNUMOFSEMS){
        printf("CInmemoryDB::create:shmSize(%d)or semNum(%d) < 1.\n",shmSize,semNum);
        return 0;
    }

    key_t ipckey = ftok(ipcPathName,ipcid);
    if(ipckey == -1){
        perror("CInmemoryDB::create:ftok error:.\n");
        printf("ipcPathName(%s),ipcid(%d)\n",ipcPathName,ipcid);
        return 0;
    }

    shmID = shmget(ipckey,0,IPC_CREAT|operatorFlag);
    if(shmID > -1){
        //The shared memory is already existed.
        return 2;
    }

    shmID = shmget(ipckey,shmSize,IPC_CREAT|operatorFlag);
    if(shmID == -1){
        perror("CInmemoryDB::create:shmget error:.\n");
        printf("ipckey = %d shmSize=%d\n",ipckey,shmSize);
        return 0;
    }

    /*
    semID = semget(ipckey,0 ,IPC_CREAT|IPC_EXCL|operatorFlag);
    if(shmID > -1){
        return 2;
    }
    */

    semID = semget(ipckey,semNum ,IPC_CREAT|operatorFlag);
    if(shmID == -1){
                perror("CInmemoryDB::create:semget error:.\n");
        printf("ipckey = %d semNum=%d flag(%d)\n",ipckey,semNum,IPC_CREAT|operatorFlag);
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
                        perror("CInmemoryDB::create:semctl at set value error: ");
            releaseShm();
            releaseSem();
        }
    }

    pShmData = shmat(shmID,NULL,0);

    if((int)pShmData == -1){
                perror("CInmemoryDB::create:shmat error:.\n");
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
                printf("CInmemoryDB::connect:ipcid(%d) is wrong.\n",ipcid);
        writelog("error 1",ipcPathName,ipcid);
        return 0;
    }
    if(ipcPathName == NULL || ipcPathName[0] == 0){
                printf("CInmemoryDB::connect:ipcPathName is NULL or blank.\n");
        writelog("error 2",ipcPathName,ipcid);
        return 0;
    }

    key_t ipckey = ftok(ipcPathName,ipcid);
    if(ipckey == -1){
                perror("CInmemoryDB::connect:ftok error:.\n");
        return 0;
        writelog("error 3",ipcPathName,ipcid);
    }

    shmID = shmget(ipckey,0,IPC_CREAT);
    if(shmID == -1){
                //The shared memory is not existed
                //perror("CInmemoryDB::connect:shmget error:.\n");
        //printf("ipcPathName=%s ipckey = %d ipcid=%d\n",ipcPathName,ipckey,ipcid);
        writelog("error 4",ipcPathName,ipcid);
        return 0;
    }

    semID = semget(ipckey,0 ,IPC_CREAT);
    if(semID == -1){
                perror("CInmemoryDB::connect:semget error:.\n");
        printf("ipckey = %d\n",ipckey);
        shmID = -1;
        writelog("error 5",ipcPathName,ipcid);
        return 0;
    }
    pShmData = shmat(shmID,NULL,0);

    if((int)pShmData == -1){
                perror("CInmemoryDB::connect:shmat error:.\n");
        semID = -1;
        shmID = -1;
        writelog("error 6",ipcPathName,ipcid);
        return 0;
    }
    writelog("OK",ipcPathName,ipcid,shmID);
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
        printf("CInmemoryDB::createTable:tableid(%d) or tableSize(%d) is wrong.\n", tableid, tableSize);
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
        printf("CInmemoryDB::createTable:table beyond the db.\n");
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
        printf("PippinInmemDB::getTableSize:tableid(%d).\n",tableid);
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
        //all need tell tableid < semNum
        printf("PippinInmemDB::lock:tableid(%d).\n",tableid);
        return 0;
    }
    if ( semID < 0 ){
        printf("PippinInmemDB::lock:semop osemID = %d.\n",semID);
        return 0;
    }
    struct sembuf sbuf;
    sbuf.sem_num= tableid;
    sbuf.sem_op = -1;
    sbuf.sem_flg = SEM_UNDO;
    if(semop(semID, &sbuf, 1) == -1 ){
        perror("PippinInmemDB::lock:semop operate error:");
        printf("tableid = %d semID = %d\n",tableid,semID);
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
        printf("PippinInmemDB::unLock:semop osemID = %d.\n",semID);
        return 0;
    }

    struct sembuf sbuf;
    sbuf.sem_num = tableid;
    sbuf.sem_op = 1;
    sbuf.sem_flg = SEM_UNDO;
    if(semop(semID, &sbuf, 1) == -1 )
    {
        if ( errno != EINTR ){
            perror("PippinInmemDB::unLock:semop error:");
            printf("tableid = %d semID = %d\n",tableid,semID);
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
    bzero(&ds, sizeof(struct shmid_ds));
    if ( shmID >= 0 ){
        if ( shmctl (shmID, IPC_STAT, &ds) < 0 ) {
            return 0;
        }
        else{
            return 1;
        }
    }
    else{
        perror("PippinInmemDB::IsExist error:");
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
                                perror("CInmemoryDB::releaseShm:semctl at set value error: ");
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

