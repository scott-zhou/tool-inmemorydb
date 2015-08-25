#include "inmdb.h"
#include "inmdb_table.h"
#include "test.h"
#include <unistd.h>

CInmemoryDB myInmemDB;
CInmemoryTable<gateway>gw;

int main(int argc,char *argv[])
{
    if(!myInmemDB.connect(IPC_PATH,IPC_ID)){
        printf("Connect error. Create DB first.\n");
        return 1;
    }
    printf("Connect DB success.\n");
    printf("Trying to create table in DB, max record num %d.\n", RECORD_NUM);
    printf("DB size %dMB, table size %dMB.\n",
           SHM_SIZE/(1000*1000),
           gw.countTableSize(RECORD_NUM,0,0)/(1000*1000));
    int createflag = gw.create(&myInmemDB,0,RECORD_NUM,2,2);
    if(createflag == 0){
        printf("GW table create error.\n");
        return 1;
    }
    else if(createflag == 2){
        printf("GW talbe already exist.\n");
    }
    else{
        printf("GW table create success.\n");
    }
    if(!gw.connect(&myInmemDB,0)){
        printf("GW connect error.\n");
        return 1;
    }
    gw.addLookUpKey(0,OFFSET(GATEWAY,tt1),64,-1,-1,HASHSEARCH,STRKEYFROMONLYSTR, "%s");
    printf("GW table create and connect success.\n");
    return 0;
}
