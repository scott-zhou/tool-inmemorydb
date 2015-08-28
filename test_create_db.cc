#include "inmdb.h"
#include "inmdb_table.h"
#include "test.h"
#include <unistd.h>

CInmemoryDB myInmemDB;
CInmemoryTable<gateway>gw;

int main(int argc,char *argv[])
{
    int createflag;
    createflag = myInmemDB.create(IPC_PATH,IPC_ID,SHM_SIZE,SEM_NUM);
    if(createflag == 0){
        printf("create error.\n");
        return 1;
    }
    else if(createflag == 2){
        printf("Already exist, can not create again.\n");
    }
    else{
        printf("create success.\n");
    }
    if(!myInmemDB.connect(IPC_PATH,IPC_ID)){
        printf("connect error.\n");
        return 1;
    }
    //myInmemDB.threadSafe(false);
    printf("Create and connect success.\n");
    return 0;
}
