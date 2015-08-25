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
    printf("GW table connect success. Print records in table. record count %d\n", gw.GetTableLoadCount());
    int index = gw.traversalPre(USEDHEAD);
    while(gw.ASSERTTABLEINDEX(index)) {
        const GATEWAY* r = gw(index);
        printf("Data in index %d is %d\n", index, r->port);
        index = gw.traversalPre(index);
    }
    return 0;
}
