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
    if(!gw.connect(&myInmemDB,0)){
        printf("GW connect error.\n");
        return 1;
    }
    printf("GW table connect success. Create 10 records in table\n");
    GATEWAY r;
    for(int i = 0; i < 10; i++){
        memset((void *)&r,0,sizeof(GATEWAY));
        sprintf(r.tt1,"p123%d",i);
        sprintf(r.tt2,"p123%d",i);
        r.port = i;
        r.gw_type = i;
        r.ss7gw_id = i;
        r.pp = i;
        r.qq = i;
        TABLEINDEX index = gw.insert(r);
        if(!gw.ASSERTTABLEINDEX(index)){
            printf("gw insert error i(%d).\n",i);
            break;
        }
        else{
            printf("gw insert success, index %d\n", int(index));
        }
    }
    return 0;
}

