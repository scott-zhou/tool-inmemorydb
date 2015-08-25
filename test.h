#ifndef TEST_H
#define TEST_H

typedef struct gateway{
    char tt1[64];
    char tt2[64];
    int  port;
    int  gw_type;
    int  ss7gw_id;
    int pp;
    int qq;
}GATEWAY;

#define IPC_PATH   "/tmp/inmdb_test_ipc"
#define IPC_ID     160
#define SHM_SIZE   20*1000*1000
#define RECORD_NUM 100*1000
#define SEM_NUM    10

#endif // TEST_H
