#include "inmdb.h"
#include "inmdb_table.h"
#include <sys/time.h>
#include <unistd.h>

typedef struct gateway{
    char tt1[64];
    char tt2[64];
    int  port;
    int  gw_type;
    int  ss7gw_id;
    int pp;
    int qq;
}GATEWAY;
CInmemoryDB myInmemDB;
CInmemoryTable<gateway>gw;


int main(int argc,char *argv[])
{
    if(argc != 2){
        printf("command line input error .\n");
        return 1;
    }

    TABLEINDEX tmp;
    TABLEINDEX tableIndex;
    int shmSize = 200*1024*1024;
    int semNum = 10;
    int createflag;
    int ipcid = atoi(argv[1]);
    createflag = myInmemDB.create("/home/scott/codes/tool-inmemorydb/bin/IPC",ipcid,shmSize,semNum);
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
    if(!myInmemDB.connect("/home/scott/codes/tool-inmemorydb/bin/IPC",ipcid)){
        printf("connect error.\n");
        return 1;
    }
    printf("connect success.\n");
    /*
       int tableid = 0;
       int tableSize = 1*1024*1024;
       if(!myInmemDB.createTable(tableid,tableSize)){
       printf("createTable error.\n");
       return 1;
       }
       myInmemDB.getTablePData(tableid);
       printf("createTable myInmemDB.getDBSize()%d.\n",myInmemDB.getDBSize());

       printf("createTable myInmemDB.getTableSize(tableid = %d) %d.\n",tableid,myInmemDB.getTableSize(tableid));
     */
    //createflag = gw.create(&myInmemDB,0,100001,1,3);
    createflag = gw.create(&myInmemDB,0,1000 *1000,0,0);
    printf("dbsize %d tablesize %d.\n", shmSize/(1024*1024),gw.countTableSize(100000 *10,0,0)/(1024*1024));
    if(createflag == 0){
        printf("gw create error.\n");
        return 0;
    }
    else if(createflag == 2){
        printf("gw exist.\n");
    }
    else{
        //    gw.addLookUpKey(0,OFFSET(GATEWAY,tt1),64,-1,-1,HASHSEARCH,STRKEYFROMONLYSTR, "%s");
        //    gw.addLookUpKey(1,OFFSET(GATEWAY,tt2),64,-1,-1,HASHSEARCH,STRKEYFROMONLYSTR, "%s");
        //    gw.addLookUpKey(2,OFFSET(GATEWAY,port),4,-1,-1,SORTSEARCH,INTKEYFROMONLYINT, "%s");
        //    gw.addLookUpKey(2,OFFSET(GATEWAY,gw_type),4,-1,-1,SORTSEARCH,INTKEYFROMONLYINT, "%s");
        //    gw.addLookUpKey(3,OFFSET(GATEWAY,ss7gw_id),4,-1,-1,SORTSEARCH,INTKEYFROMONLYINT, "%s");
        //    gw.addLookUpKey(4,OFFSET(GATEWAY,pp),4,-1,-1,SORTSEARCH,INTKEYFROMONLYINT, "%s");
        ///    gw.addLookUpKey(5,OFFSET(GATEWAY,qq),4,-1,-1,SORTSEARCH,INTKEYFROMONLYINT, "%s");
        printf("gw create success.\n");
    }
    if(!gw.connect(&myInmemDB,0)){
        printf("gw connect error.\n");
        return 1;
    }
    //getchar();
    //goto releasetag;
    printf("gw connect success.\n");
    //    printf("gw addLookUpKey success.\n");
    //typedef struct gateway{
    GATEWAY xy;
    int jjj = 0;

    timeval now1,now2;
    gettimeofday(&now1, NULL);
    //for(int i = 0; i < 1;i ++)
    while(1)
    {
        for(int i = 0; i < 300;i ++){
            memset((void *)&xy,0,sizeof(xy));
            sprintf(xy.tt1,"p123%d",i);
            sprintf(xy.tt2,"p123%d",i);
            xy.port = i;
            xy.gw_type = i;
            xy.ss7gw_id = i;
            xy.pp = i;
            xy.qq = i;
            //        printf("insert i %d.\n",i);
            tmp = gw.insert(xy);
            //        printf("after insert i %d.\n",i);
            if(!gw.ASSERTTABLEINDEX(tmp)){
                printf("gw insert error i(%d).\n",i);
            }
            else{
                //printf("gw insert success.\n");
            }

            if(!gw.deletex(tmp)){
                printf("gw delete error i(%d).\n",i);
            }
            //        printf("before search i %d.\n",i);
            /*            tmp = gw.search(xy,0);
                        if(!gw.ASSERTTABLEINDEX(tmp)){
                        printf("gw search error i(%d).\n",i);
                        }
                        tmp = gw.search(xy,1);
                        if(!gw.ASSERTTABLEINDEX(tmp)){
                        printf("gw search error i(%d).\n",i);
                        }
                        tmp = gw.search(xy,2);
                        if(!gw.ASSERTTABLEINDEX(tmp)){
                        printf("gw search error i(%d).\n",i);
                        }
             */    //        printf("after search i %d.\n",i);
            //        printf("before gw updateData i(%d).\n",i);
            //        memset((void *)&xy,0,sizeof(xy));
            //        sprintf(xy.tt,"p999%d",i);
            //        if(!gw.updateData(OFFSET(GATEWAY,tt),strlen(xy.tt),tmp,xy.tt)){
            //            printf("gw updateData error i(%d).\n",i);
            //        }
            //        printf("after gw updateData i(%d).\n",i);
            //
            /*        memset((void *)&xy,0,sizeof(xy));
                    sprintf(xy.tt,"p123%d",i);
                    tmp = gw.search(xy,0);
                    if(!gw.ASSERTTABLEINDEX(tmp)){
            //printf("gw search.......... error i(%d).\n",i);
            }


            memset((void *)&xy,0,sizeof(xy));
            sprintf(xy.tt,"p999%d",i);
            tmp = gw.search(xy,0);
            if(!gw.ASSERTTABLEINDEX(tmp)){
            printf("gw search!!!!!!!!!!!!!!!!! error i(%d).\n",i);
            }
             */        //printf("search tt(%s).\n",gw(tmp)->tt);
            /*
             */
        }
        usleep(30*1000);
    }
    tmp = 2;
    memset((void *)&xy,0,sizeof(xy));
    jjj = 1;
    xy.port = jjj;
    if(!gw.updateData(OFFSET(GATEWAY,port),4,tmp,&(xy.port))){
        printf("gw updateData error jjj(%d).\n",jjj);
    }

    tmp = 2;
    jjj = 0;
    xy.port = jjj;
    if(!gw.updateData(OFFSET(GATEWAY,port),4,tmp,&(xy.port))){
        printf("gw updateData error jjj(%d).\n",jjj);
    }

    //    gw.printSort();
    //    gw.printHash();
    //    int xIndex;
    //    xIndex = 10;
    //    gw.printKeys(xIndex);
    //    gw.printHashTail();
    /*
       sprintf(xy.tt,"p123");
       tmp = gw.searchPrefix(xy.tt,0);
       if(!gw.ASSERTTABLEINDEX(tmp)){
       printf("search errori.\n");
       }
       printf("search tt(%s)",gw(tmp)->tt);
       while(1){
       tmp = gw.searchPrefixNext(xy.tt,0,tmp);
       if(!gw.ASSERTTABLEINDEX(tmp)){
       printf("searchNext errori.\n");
       break;
       }
       printf("searchNext tt(%s).\n",gw(tmp)->tt);
       }
     */

    gettimeofday(&now2, NULL);
    printf("now2 - now1 = %ld .\n",(now2.tv_sec - now1.tv_sec) * 1000 +(now2.tv_usec - now1.tv_usec) / 1000 );
    getchar();
    myInmemDB.releaseInmemDB();
    return 1;
}
