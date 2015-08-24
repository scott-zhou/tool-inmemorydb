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
    myInmemDB.releaseInmemDB();
    printf("DB released.\n");
    return 0;
}
