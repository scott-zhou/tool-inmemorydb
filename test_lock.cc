/*test.cxx for inmemdb*/
#include "inmdb.h"
#include "inmdb_table.h"
typedef struct gateway{
    char tt[16];
    int  port;
    int  gw_type;
    int  ss7gw_id;
}GATEWAY;
CInmemoryDB myInmemDB;
CInmemoryTable<gateway>gw;


int main(int argc,char *argv[])
{
    int shmSize =10*1024*1024;
    int semNum = 10;
    int createflag;
    int ipcid = 23;
    createflag = myInmemDB.create("/tmp/inmdb",ipcid,shmSize,semNum);
    if(createflag == 0){
        printf("create error.\n");
        return 0;
    }
    else if(createflag == 2){
        printf("exist.\n");
    }
    else{
        printf("create success.\n");
    }
    if(!myInmemDB.connect("/tmp/inmdb",ipcid)){
        printf("connect error.\n");
        return 1;
    }
    int tableid = 0;
    int tableSize = 1*1024*1024;
    if(!myInmemDB.createTable(tableid,tableSize)){
        printf("createTable error.\n");
        return 1;
    }
    myInmemDB.getTablePData(tableid);
    printf("createTable myInmemDB.getDBSize()%d.\n",myInmemDB.getDBSize());

    char line[1024];
    while(1){
        memset(line,0,sizeof(line));
        printf("lock/unLock:"); fflush(NULL);
        gets(line);
        printf("line = <%s>\n",line);
        if(!strncmp(line,"lock",strlen("lock"))){
            myInmemDB.lock(tableid);
            printf("lock ok.\n"); fflush(NULL);
        }
        else if(!strncmp(line,"unLock",strlen("unLock"))){
            myInmemDB.unLock(tableid);
            printf("unLock ok.\n"); fflush(NULL);
        }
    }
    myInmemDB.releaseInmemDB();
    return 1;
}




