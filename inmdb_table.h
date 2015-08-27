/**
 * inmemtable.h Data store and management
 **/

#ifndef LIB_INMDB_TABLE_H__
#define LIB_INMDB_TABLE_H__

#include <time.h>
#include <assert.h>
#include "inmdb.h"
#include "inmdb_table_index.h"
#include "inmdb_const_values.h"
#include "inmdb_log.h"

//define key struct
typedef struct tablekey{
    SEARCHMETHOD searchMethod;
    KEYMETHOD keyMethod;
    int field1Offset;
    int field1Length;
    int field2Offset;
    int field2Length;
    char keyFormat[MAXKEYFORMAT];
    int loadCount;
}TABLEKEY;

typedef struct tabledescriptor{
    int numOfHashKey;
    int numOfSortKey;
    int sizeOfRecord;
    int sizeOfTable;
    int capacity;
    int loadCount;
    int hashPrimeNumber;
} TABLEDESCRIPTOR;

template<class T>
class CInmemoryTable{
    private:
        //datamember

        T *pData;
        TABLEKEY *pKey;
        TABLEDESCRIPTOR *pTableDescriptor;
        int *dataNext;
        int *dataPre;
        int *pSort[MAXNUMOFSORTKEY];
        int *pHash[MAXNUMOFHASHKEY];
        int *pHashNext[MAXNUMOFHASHKEY];
        CInmemoryDB *pInmemDB;
        int tableID;
        time_t *timeStamp;
        int bzeroFlag;
    public:
        CInmemoryTable()
        {
            pData = NULL;
            pKey = NULL;
            pTableDescriptor = NULL;
            dataNext = NULL;
            dataPre = NULL;
            timeStamp = NULL;
            for(int i = 0;i < MAXNUMOFSORTKEY;i++){
                pSort[i] = NULL;
            }

            for(int i = 0;i < MAXNUMOFHASHKEY;i++){
                pHash[i] = NULL;
                pHashNext[i] = NULL;
            }
            pInmemDB = NULL;
            tableID = -1;
            bzeroFlag = 1;
        };
        virtual ~CInmemoryTable(){};
        int countTableSize(int tablecapacity,int numofhashkey,int numofsortkey);
        int create(CInmemoryDB *pInmemDB, int id,int tablecapacity,int numofhashkey = 0,int numofsortkey = 0);
        int connect(CInmemoryDB *pInmemDB,int id);
        int clear(void);
        void addLookUpKey(int keyid,int field1offset,int field1Length, int field2offset,int field2Length,SEARCHMETHOD sm,KEYMETHOD km, const char *keyFormat="");
        int ASSERTTABLEINDEX(int tableindex);    //not thread-safe
        TABLEINDEX search(T& tempdata,int keyid = 0);
        TABLEINDEX searchNext(T& tempdata,int keyid, TABLEINDEX tableindex);
        TABLEINDEX searchPrefix(void *key,int keyid = 0);
        TABLEINDEX searchPrefixNext(void *key,int keyid, TABLEINDEX tableindex);
        //TABLEINDEX insert(T& tempdata);
        int insert(T& tempdata);
        int deletex(int tableindex);
        int getTableUseRate();
        int GetTableLoadCount();
        int begin();
        int traversalNext(int tableindex);
        int end();
        int  traversalPre(int tableindex);
        int updateData(int offset,int dataLenth,int tableindex,const void *pNewData);
        const T* operator()(int index);    //not thread-safe
        time_t getTimeStamp(int index){return timeStamp[index];};
        int getCapability(){return (pTableDescriptor->capacity);};
        int printKeys(int dataIndex);
        int printSort();
        int printHashTail();
        int printHash();
        int dataalloc();
        int dataallocc();         //not thread-safe
        int deletexx(int index);    //not thread-safe
        void disableBzero(int flag = 0) {
            bzeroFlag = flag;
        }
    private:
        int buildkey(void *key,int dataIndex,int keyid = 0);
        int buildkey(void *tmpstrkey,T& tempData,int keyid = 0);
        size_t hashfunc(const char *key);
        int sortsearch(T& tempdata,int keyid, int &dataIndex, int &sortIndex);
        int sortsearch(void *key,int keyid, int &dataIndex, int &internalIndex);
        int sortdeletesearch(void *key,int keyid, int keyDataIndex, int &sortIndex);
        int sortsearchprefix(void *key,int keyid, int &dataIndex, int &internalIndex);
        bool sortinsert(int keyid, int dataIndex);
        bool hashinsert(int keyid, int dataIndex);
        bool sortdelete(int keyid,int dataIndex);
        bool hashdelete(int keyid,int dataIndex);
        int datainsert(T& tempdata,int &dataIndex);
        int datadelete(int dataIndex);
        int getHashPrimeNumber(int hashCapacity);
        TABLEINDEX search(void *key,int keyid = 0);
        TABLEINDEX searchNext(void *key,int keyid, TABLEINDEX tableindex);
};

template <class T>
int CInmemoryTable<T>::printKeys(int dataIndex)
{
    for(int hashNum = 0; hashNum < pTableDescriptor->numOfHashKey;hashNum ++){
        char tmpstrkey[MAXSTROFKEY];
        memset(tmpstrkey,0,sizeof(tmpstrkey));
        if(!buildkey(tmpstrkey,dataIndex,hashNum)){
            printf("buildkey return 0:tmpstrkey(%s) dataIndex(%d)keyid (%d).\n",tmpstrkey,dataIndex,hashNum);
            return 0;
        }
        printf("hash key[%d] = %32s ",hashNum,tmpstrkey);
        printf("\n");
    }
    for(int sortNum = 0; sortNum < pTableDescriptor->numOfSortKey;sortNum ++){
        int tmpintkey;
        tmpintkey = 0;
        if(!buildkey(&tmpintkey,dataIndex,pTableDescriptor->numOfHashKey + sortNum)){
            printf("buildkey return 0:tmpintkey(%d) dataIndex(%d)keyid (%d).\n",tmpintkey,dataIndex,pTableDescriptor->numOfHashKey + sortNum);
            return 0;
        }
        printf("sort key[%d] = %32d ",pTableDescriptor->numOfHashKey + sortNum,tmpintkey);
        printf("\n");
    }
    return 1;
}

template <class T>
int CInmemoryTable<T>::printHash()
{
    //get pointer to each sort begin
    for(int hashNum = 0; hashNum < pTableDescriptor->numOfHashKey;hashNum ++){
        printf("hash[%d]:.\n",hashNum);
        for(int count = 0; count < pTableDescriptor->hashPrimeNumber; count ++){
            printf("%5d ",pHash[hashNum][count]);
        }
        printf("\n");
        printf("hashNext[%5d]:.\n",hashNum);
        for(int count = 0; count < (pTableDescriptor->capacity + 2); count ++){
            printf("%5d ",pHashNext[hashNum][count]);
        }
        printf("\n");
    }
    return 1;
}

template <class T>
int CInmemoryTable<T>::printHashTail()
{
    int hashNextIndex;
    hashNextIndex = 0;
    //get pointer to each sort begin
    for(int hashNum = 0; hashNum < pTableDescriptor->numOfHashKey;hashNum ++){
        printf("hash[%d]:.\n",hashNum);
        for(int count = 0; count < pTableDescriptor->hashPrimeNumber; count ++){
            printf("%5d ",pHash[hashNum][count]);
            hashNextIndex = pHash[hashNum][count];
            if(hashNextIndex){
                printf("(hashnext: ");
            }
            while(hashNextIndex){
                printf(" %5d",pHashNext[hashNum][hashNextIndex]);
                hashNextIndex = pHashNext[hashNum][hashNextIndex];
            }
            printf(" )");
        }
        printf("\n");
    }
    return 1;
};

template <class T>
int CInmemoryTable<T>::printSort()
{
    //get pointer to each sort begin
    for(int sortNum = 0; sortNum < pTableDescriptor->numOfSortKey;sortNum ++){
        printf("sort[%d]:.\n",sortNum);
        for(int count = 0; count < pKey[pTableDescriptor->numOfHashKey + sortNum].loadCount; count ++){
            printf("%03d ",pSort[sortNum][count]);
        }
        printf("\n");
    }
    return 1;
};

/**********************************************
Function: countTableSize: count the table size(bytes)
parameter: '
tablecapacity: table max record num
numofhashkey: num of hashkey
numofsortkey: num of sortkey
Description:
Returns:    units:bytes
Error Number:
*********************************************/

template <class T>
int CInmemoryTable<T>::countTableSize(int tablecapacity ,int numofhashkey,int numofsortkey)
{
    // Memory struct is: TABLEDESCRIPTOR, keys, previous link table, next link table, data
    return (sizeof(TABLEDESCRIPTOR) +
            sizeof(TABLEKEY)*(numofhashkey + numofsortkey) +
            2*sizeof(int)*getHashPrimeNumber(tablecapacity + 2)*numofhashkey +
            sizeof(int)*(tablecapacity + 2)*numofsortkey +
            sizeof(int)*(tablecapacity + 2)*3 +
            (sizeof(T)*(tablecapacity + 2))/4*4+4);
}

/**********************************************
Function: create:
parameter:
Description: Create table in shared memory.
    If the keys are need, they must be inited before insert any record into table.
    Call addLookUpKey to init keys.
Returns:    0 fail
1 success
2 already existed
Error Number:
*********************************************/
template <class T>
int CInmemoryTable<T>::create(
        CInmemoryDB *pInmemoryDB,
        int id,
        int tablecapacity,
        int numofhashkey,
        int numofsortkey)
{
    //tablesize is the totalsize(bytes) of the table
    //createflag is for returnvalue from  pInmemoryDB->createTable(),0 error,1 success,2 exist
    //pTable is pointer to the beginning of the table
    int tablesize;
    int createflag;
    void *pTable = NULL;

    //Create and init everything here.
    //tablekey,tabledesriptor,timestamp,datapre,datanext
    if(pInmemoryDB == NULL){
        inmdb_log(LOGCRITICAL, "pInmemoryDB == %p.", pInmemoryDB);
        return 0;
    }
    //count the table size(bytes)
    tablesize = countTableSize(tablecapacity,numofhashkey,numofsortkey);
    createflag = pInmemoryDB->createTable(id,tablesize);

    if(createflag == 0){
        inmdb_log(LOGCRITICAL, "FATAL ERROR::CInmemoryTable::create tableid(%d)error\n",id);
        return 0;
    }
    else if(createflag == 2){
        inmdb_log(LOGWARN, "INFO::CInmemoryTable::create tableid(%d)exist",id);
        return 2;
    }
    //get the pointer to table begin
    pTable = pInmemoryDB->getTablePData(id);
    if(pTable == NULL){
        inmdb_log(LOGCRITICAL, "FATAL ERROR::CInmemoryTable::create getTablePData tableid(%d).\n",id);
        return 0;
    }
    inmdb_log(LOGDEBUG, "Table start address in create is(%p).\n",pTable);
    // Memory struct is: TABLEDESCRIPTOR, keys, previous link table, next link table, data

    tableID = id;
    pInmemDB = pInmemoryDB;

    //make the table descriptor information
    TABLEDESCRIPTOR TDescriptor;
    TDescriptor.numOfHashKey = numofhashkey;
    TDescriptor.numOfSortKey = numofsortkey;
    TDescriptor.sizeOfRecord = sizeof(T);
    TDescriptor.sizeOfTable = tablesize;
    TDescriptor.capacity = tablecapacity ;
    TDescriptor.loadCount = 0;
    TDescriptor.hashPrimeNumber = getHashPrimeNumber(tablecapacity + 2);
    memcpy(pTable,&TDescriptor,sizeof(TABLEDESCRIPTOR));

    //initial the pKey[keyid].loadCount = 0;
    pKey = (TABLEKEY*)((intptr_t)pTable + sizeof(TABLEDESCRIPTOR));
    //initiate the datapre/datanext/timestamp
    //dataPre = (int*)((int)pTable + sizeof(TABLEDESCRIPTOR) + (numofhashkey + numofsortkey) * sizeof(TABLEKEY) + numofhashkey * ((tablecapacity + 2) + getHashPrimeNumber((tablecapacity + 2))) * sizeof(int) + numofsortkey * (tablecapacity + 2) * sizeof(int));
    dataPre = (int*)((intptr_t)pTable + sizeof(TABLEDESCRIPTOR) + (numofhashkey + numofsortkey) * sizeof(TABLEKEY) + numofhashkey * (2*getHashPrimeNumber((tablecapacity + 2))) * sizeof(int) + numofsortkey * (tablecapacity + 2) * sizeof(int));
    dataNext = (int *)((intptr_t)dataPre + (tablecapacity + 2) * sizeof(int));
    timeStamp = (time_t *)((intptr_t)dataNext + sizeof(int) * (tablecapacity + 2));
    dataNext[USEDHEAD] = USEDHEAD;
    dataPre[USEDHEAD] = USEDHEAD;
    dataNext[UNUSEDHEAD] = 2;
    dataPre[2] = UNUSEDHEAD;
    dataNext[2] = 3;
    for(int i = 3;i<(tablecapacity + 2);i++){
        dataNext[i] = i+1;
        dataPre[i] = i-1;
    }
    dataNext[(tablecapacity + 2) - 1] = UNUSEDHEAD;
    dataPre[UNUSEDHEAD] = (tablecapacity + 2) - 1;
    pTable = NULL;
    tableID = id;
    pInmemDB = pInmemoryDB;
    inmdb_log(LOGDEBUG, "INFO::CInmemoryTable::create tableid(%d)success.\n",id);
    return 1;
}

/**********************************************
Function: connect:
parameter:
Description:
Returns:    0 fail
1 success
Error Number:
*********************************************/
template <class T> 
int CInmemoryTable<T>::connect(CInmemoryDB *pInmemoryDB,int id){

    // indexListSize is size of hashNext/sort/timeStamp list
    // hashListSize is size of hash list
    // pTable is pointer to the beginning of the table
    void *pTable = NULL;
    int indexListSize;
    int hashListSize;
    if(pInmemoryDB == NULL){
        inmdb_log(LOGCRITICAL, "pInmemoryDB == %p.", pInmemoryDB);
        return 0;
    }
    //get pointer to table begin
    pInmemDB = pInmemoryDB;
    tableID = id;
    pTable = pInmemDB->getTablePData(id);
    if(pTable == NULL){
        inmdb_log(LOGCRITICAL, "FATAL ERROR::CInmemoryTable::connect getTablePData tableid(%d).\n",id);
        return 0;
    }
    inmdb_log(LOGDEBUG, "Table start address in connect is(%p).\n",pTable);
    //get pointer to tabledescriptor begin
    pTableDescriptor = (TABLEDESCRIPTOR *)pTable;

    //get pointer to keyescriptor begin
    indexListSize = (pTableDescriptor->capacity + 2) * sizeof(int);
    hashListSize = (pTableDescriptor->hashPrimeNumber) * sizeof(int);
    pKey = (TABLEKEY*)((intptr_t)pTable + sizeof(TABLEDESCRIPTOR));

    //get pointer to each hash begin
    for(int hashNum = 0; hashNum < pTableDescriptor->numOfHashKey;hashNum ++){
        pHash[hashNum] =(int *) ((hashNum == 0)? ((intptr_t)pKey + sizeof(TABLEKEY) * (pTableDescriptor->numOfSortKey + pTableDescriptor->numOfHashKey)):((intptr_t)pHash[hashNum -1] + hashListSize));
    }

    //get pointer to each hashnext begin
    for(int hashNum = 0; hashNum < pTableDescriptor->numOfHashKey;hashNum ++){
        pHashNext[hashNum] = (int *)((hashNum == 0)?((intptr_t)pHash[pTableDescriptor->numOfHashKey - 1] + hashListSize):((intptr_t)pHashNext[hashNum - 1] + hashListSize));
    }

    //get pointer to each sort begin
    for(int sortNum = 0; sortNum < pTableDescriptor->numOfSortKey;sortNum ++){
        pSort[sortNum] = (int *)((sortNum == 0)?((intptr_t)pKey + sizeof(TABLEKEY) * (pTableDescriptor->numOfSortKey + pTableDescriptor->numOfHashKey) + 2*hashListSize * pTableDescriptor->numOfHashKey):((intptr_t)pSort[sortNum - 1] + indexListSize));
    }

    //get pointer to dataPre/dataNext/timeStamp begin
    //dataPre = (int *)((int)pKey + sizeof(TABLEKEY) * (pTableDescriptor->numOfSortKey + pTableDescriptor->numOfHashKey) + (hashListSize + indexListSize) * pTableDescriptor->numOfHashKey + indexListSize * pTableDescriptor->numOfSortKey);
    dataPre = (int *)((intptr_t)pKey + sizeof(TABLEKEY) * (pTableDescriptor->numOfSortKey + pTableDescriptor->numOfHashKey) + (2*hashListSize) * pTableDescriptor->numOfHashKey + indexListSize * pTableDescriptor->numOfSortKey);

    dataNext = (int *)((intptr_t)dataPre + indexListSize);
    timeStamp = (time_t *)((intptr_t)dataNext + indexListSize);
    //get pointer to data begin
    pData = (T *)((intptr_t)timeStamp + indexListSize);
    inmdb_log(LOGDEBUG, "INFO::CInmemoryTable::connect tableid(%d)success.\n",id);
    return 1;
}

/**********************************************
Function: clear:
parameter:
Description:
Returns:    0 fail
1 success
Error Number:
*********************************************/

template <class T> 
int CInmemoryTable<T>::clear(void){
    //reset tabledescriptor
    pTableDescriptor->loadCount = 0;
    //reset keydescriptor
    for(int keyid = 0;keyid < (pTableDescriptor->numOfHashKey + pTableDescriptor->numOfSortKey);keyid ++){
        pKey[keyid].loadCount = 0;
    }

    //reset pHash/pHashNext/pSort
    for(int hashNum = 0; hashNum < pTableDescriptor->numOfHashKey;hashNum ++){
        memset(pHash[hashNum],0,sizeof(int)*pTableDescriptor->hashPrimeNumber);
    }

    //get pointer to each hashnext begin
    for(int hashNum = 0; hashNum < pTableDescriptor->numOfHashKey;hashNum ++){
        memset(pHashNext[hashNum],0,sizeof(int)*(pTableDescriptor->capacity + 2));
    }

    //get pointer to each sort begin
    for(int sortNum = 0; sortNum < pTableDescriptor->numOfSortKey;sortNum ++){
        memset(pSort[sortNum],0,sizeof(int)*(pTableDescriptor->capacity + 2)*pTableDescriptor->numOfSortKey);
    }

    //reset datapre/datanext/timestamp
    for(int j = 0; j< (pTableDescriptor->capacity + 2); j++){
        timeStamp[j] = 0;
    }
    dataNext[USEDHEAD] = USEDHEAD;
    dataPre[USEDHEAD] = USEDHEAD;
    dataNext[UNUSEDHEAD] = 2;
    dataPre[2] = UNUSEDHEAD;
    dataNext[2] = 3;
    for(int i = 3;i<(pTableDescriptor->capacity + 2);i++){
        dataNext[i] = i+1;
        dataPre[i] = i-1;
    }
    dataNext[(pTableDescriptor->capacity + 2) - 1] = UNUSEDHEAD;
    dataPre[UNUSEDHEAD] = (pTableDescriptor->capacity + 2) - 1;
    //clear data
    memset(pData,0,((sizeof(T) * (pTableDescriptor->capacity + 2))/4*4 + 4));
    inmdb_log(LOGWARN, "INFO::CInmemoryTable::connect clear(%d)success.\n",tableID);
    return 1;
}

/**********************************************
Function: addLookUpKey:
parameter:
Description:
Returns: 
Error Number:
*********************************************/
template <class T> 
void CInmemoryTable<T>::addLookUpKey(
        int keyid,
        int field1offset,
        int field1Length,
        int field2offset,
        int field2Length,
        SEARCHMETHOD sm,
        KEYMETHOD km,
        const char *keyFormat){
    void *pTable = NULL;
    if(tableID < 0){
        inmdb_log(LOGCRITICAL, "tableID(%d) < 0.\n", tableID);
        return;
    }
    pTable = pInmemDB->getTablePData(tableID);
    if(pTable == NULL){
        inmdb_log(LOGCRITICAL, "pTable(%p) is NULL.\n", pTable);
        return;
    }
    inmdb_log(LOGDEBUG, "Table start address in add lookup key is(%p).\n",pTable);
    assert(keyid>=0);
    if(sm == HASHSEARCH) {
        inmdb_log(LOGDEBUG, "keyid(%d) numOfHashKey(%d).\n", keyid, pTableDescriptor->numOfHashKey);
        assert(keyid < pTableDescriptor->numOfHashKey);
    }
    else if(sm == SORTSEARCH) {
        inmdb_log(LOGDEBUG, "keyid(%d) numOfSortKey(%d).\n", keyid, pTableDescriptor->numOfSortKey);
        assert(keyid < pTableDescriptor->numOfSortKey);
    }

    pKey = (TABLEKEY*)((intptr_t)pTable + sizeof(TABLEDESCRIPTOR));

    pKey[keyid].searchMethod = sm;
    pKey[keyid].keyMethod = km;
    pKey[keyid].field1Offset = field1offset;
    pKey[keyid].field1Length = field1Length;
    pKey[keyid].field2Offset = field2offset;
    pKey[keyid].field2Length = field2Length;
    if(km != INTKEYFROMONLYINT){
        strncpy(pKey[keyid].keyFormat,keyFormat,sizeof(pKey[keyid].keyFormat) - 1);
    }
    //pKey[keyid].loadCount = 0;
    pTable = NULL;
    inmdb_log(LOGWARN, "INFO::CInmemoryTable::addLookUpKey tableid(%d)keyid(%d)success.\n",tableID,keyid);
}

/*********************************************
Function: ASSERTTABLEINDEX: This function is NOT thread-safe!
parameter:
Description:
Returns: 0 error;1 success
Error Number:
*********************************************/
template <class T> 
int CInmemoryTable<T>::ASSERTTABLEINDEX(int tableindex){
    if(NULL == pTableDescriptor) {
        return 0;
    }
    if(tableindex < 2 || tableindex > ((pTableDescriptor->capacity + 2) - 1)){
        inmdb_log(LOGDEBUG, "tableindex invalid value(%d) tableID(%d) .\n",tableindex,tableID);
        return 0;
    }

    if(timeStamp == NULL){
        inmdb_log(LOGCRITICAL, "error timeStamp == %p", timeStamp);
        return 0;
    }
    if(timeStamp[tableindex] == 0){
        return 0;
    }
    return 1;
}


/*********************************************
Function: buildkey:
parameter:
Description:
Returns: 0 error;1 success
Error Number:
*********************************************/
template <class T> 
int CInmemoryTable<T>::buildkey(void *tmpstrkey,T& tempData,int keyid){
    if(pKey[keyid].keyMethod == STRKEYFROMSTRPLUSINT){
        char field1[MAXFIELDSTRING];
        int field2;
        memset(field1,0,sizeof(field1));
        field2 = 0;
        strncpy(field1,(const char*)((intptr_t)&tempData + pKey[keyid].field1Offset),pKey[keyid].field1Length);
        field2 = *(int *)((intptr_t)&tempData + pKey[keyid].field2Offset);
        sprintf((char *)tmpstrkey,pKey[keyid].keyFormat,field1,field2);
    }
    else if(pKey[keyid].keyMethod == STRKEYFROMINTPLUSSTR){
        int field1;
        char field2[MAXFIELDSTRING];
        memset(field2,0,sizeof(field2));
        field1 = 0;
        field1 = *(int *)((intptr_t)&tempData + pKey[keyid].field1Offset);
        strncpy(field2,(const char*)((intptr_t)&tempData + pKey[keyid].field2Offset),pKey[keyid].field2Length);
        sprintf((char *)tmpstrkey,pKey[keyid].keyFormat,field1,field2);
    }
    else if(pKey[keyid].keyMethod == STRKEYFROMSTRPLUSSTR){
        char field1[MAXFIELDSTRING];
        char field2[MAXFIELDSTRING];
        memset(field1,0,sizeof(field1));
        memset(field2,0,sizeof(field2));
        strncpy(field1,(const char*)((intptr_t)&tempData + pKey[keyid].field1Offset),pKey[keyid].field1Length);
        strncpy(field2,(const char*)((intptr_t)&tempData + pKey[keyid].field2Offset),pKey[keyid].field2Length);
        sprintf((char *)tmpstrkey,pKey[keyid].keyFormat,field1,field2);
    }
    else if(pKey[keyid].keyMethod == STRKEYFROMINTPLUSINT){
        int field1;
        int field2;
        field1 = 0;
        field2 = 0;
        field1 = *(int *)((intptr_t)&tempData + pKey[keyid].field1Offset);
        field2 = *(int *)((intptr_t)&tempData + pKey[keyid].field2Offset);
        sprintf((char *)tmpstrkey,pKey[keyid].keyFormat,field1,field2);
    }
    else if(pKey[keyid].keyMethod == STRKEYFROMONLYSTR){
        sprintf((char *)tmpstrkey,pKey[keyid].keyFormat,(const char*)((intptr_t)&tempData + pKey[keyid].field1Offset));
    }
    else if(pKey[keyid].keyMethod == INTKEYFROMONLYINT){
        *(int *)tmpstrkey = *(int *)((intptr_t)&tempData + pKey[keyid].field1Offset);
    }
    else if(pKey[keyid].keyMethod == STRKEYFROMONLYINT){
        int key_v = *(int *)((intptr_t)&tempData + pKey[keyid].field1Offset);
        sprintf((char *)tmpstrkey, pKey[keyid].keyFormat, key_v);
    }
    else{
        inmdb_log(LOGCRITICAL, "FATAL ERROR::CInmemoryTable:buildkey:pKey[%d].keyMethod(%d) error.\n",keyid,pKey[keyid].keyMethod);
        return 0;
    }
    return 1;
}

/*********************************************
Function: buildkey:
parameter:
Description:
Returns: 0 error;1 success
Error Number:
*********************************************/
template <class T> 
int CInmemoryTable<T>::buildkey(void *tmpstrkey,int dataIndex,int keyid){
    return buildkey(tmpstrkey,pData[dataIndex],keyid);
}

/*********************************************
Function: sortsearch:
parameter:sortIndex and dataIndex is -1 when it is not founded
Description:
Returns: 0 error;1 success
Error Number:
*********************************************/

template <class T> 
int CInmemoryTable<T>::sortsearch(T& tempdata,int keyid, int &dataIndex, int &sortIndex){
    if(pKey[keyid].keyMethod == STRKEYFROMONLYSTR){
        char strkey[MAXSTROFKEY];
        memset(strkey,0,sizeof(strkey));
        if(!buildkey(strkey,tempdata,keyid)){
            inmdb_log(LOGCRITICAL, "FATAL ERROR::CInmemoryTable:sortsearch:Key[%s]keyid(%d)buildkey return 0.\n",strkey,keyid);
        }
        return sortsearch(strkey,keyid,dataIndex,sortIndex);
    }
    else{
        int intkey;
        intkey = 0;
        if(!buildkey(&intkey,tempdata,keyid)){
            inmdb_log(LOGCRITICAL, "FATAL ERROR::CInmemoryTable:sortsearch:Key[%d]keyid(%d)buildkey return 0.\n",intkey,keyid);
        }
        return sortsearch(&intkey,keyid,dataIndex,sortIndex);
    }
}

/*********************************************
Function: sortsearch:
parameter:sortIndex and dataIndex should be -1 if could not found
Description:
Returns: 0 error;1 success
Error Number:
*********************************************/

template <class T> 
int CInmemoryTable<T>::sortsearch(void *key,int keyid, int &dataIndex, int &sortIndex){
    char tmpstrkey[MAXSTROFKEY];
    int beginIndex;
    int endIndex;
    int tmpIndex;
    int tmpintkey;

    beginIndex = 0;
    sortIndex = -1;
    dataIndex = -1;
    endIndex = pKey[keyid].loadCount - 1;
    memset(tmpstrkey,0,sizeof(tmpstrkey));

    if(endIndex == -1){
        //empty array
        inmdb_log(LOGDEBUG, "LOGDEBUG:CInmemoryTable:sortsearch: pSort[keyid %d] is empty.\n",keyid);
        return 0;
    }
    tmpIndex = endIndex/2;
    while(1){
        dataIndex = pSort[keyid - pTableDescriptor->numOfHashKey][tmpIndex];
        if(pKey[keyid].keyMethod == STRKEYFROMONLYSTR){
            if(!buildkey(tmpstrkey,dataIndex,keyid)){
                inmdb_log(LOGCRITICAL, "FATAL ERROR::CInmemoryTable:sortsearch:Key[%s].tmpstrkey(%s) dataIndex(%d)keyid (%d)buildkey return 0.\n",key,tmpstrkey,dataIndex,keyid);
                return 0;
            }
            if(strcmp((char *)key,tmpstrkey) > 0){
                if((tmpIndex == endIndex) || (beginIndex == endIndex)){
                    inmdb_log(LOGDEBUG, "LOGDEBUG:CInmemoryTable:sortsearch:can not find Key[%s] tableID(%d) keyid(%d).\n",*(int *)key,tableID,keyid);
                    return 0;
                }
                beginIndex = tmpIndex + 1;
                tmpIndex = beginIndex + (endIndex - beginIndex)/2;
            }
            else if(strcmp((char *)key,tmpstrkey) < 0){
                if((tmpIndex == beginIndex) || (beginIndex == endIndex)){
                    inmdb_log(LOGDEBUG, "LOGDEBUG:CInmemoryTable:sortsearch:can not find Key[%s] tableID(%d) keyid(%d).\n",*(int *)key,tableID,keyid);
                    return 0;
                }
                endIndex = tmpIndex - 1;
                tmpIndex = beginIndex + (endIndex - beginIndex)/2;
            }
            else{
                while(1)
                {
                    if(tmpIndex == 0){
                        break;
                    }
                    tmpIndex--;
                    int tmpDataIndex = dataIndex;
                    dataIndex = pSort[keyid - pTableDescriptor->numOfHashKey][tmpIndex];
                    if(!buildkey(tmpstrkey,dataIndex,keyid)){
                        inmdb_log(LOGCRITICAL, "FATAL ERROR::CInmemoryTable:sortsearch:Key[%s].tmpstrkey(%s) dataIndex(%d)keyid (%d)buildkey return 0.\n",key,tmpstrkey,dataIndex,keyid);
                        return 0;
                    }
                    if(strcmp((char *)key,tmpstrkey) != 0){
                        tmpIndex ++;
                        dataIndex = tmpDataIndex;
                        break;
                    }
                }
                sortIndex = tmpIndex;
                return 1;
            }
        }
        else{
            if(!buildkey(&tmpintkey,dataIndex,keyid)){
                inmdb_log(LOGCRITICAL, "FATAL ERROR::CInmemoryTable:sortsearch:Key[%d].tmpintkey(%d)dataIndex(%d)keyid(%d) buildkey return 0.\n",*(int *)key,tmpintkey,dataIndex,keyid);
                return 0;
            }

            if((*(int *)key) > tmpintkey){
                if((tmpIndex == endIndex) || (beginIndex == endIndex)){
                    inmdb_log(LOGDEBUG, "LOGDEBUG:CInmemoryTable:sortsearch:can not find Key[%d].\n",*(int *)key);
                    return 0;
                }
                beginIndex = tmpIndex + 1;
                tmpIndex = beginIndex + (endIndex - beginIndex)/2;
            }
            else if(*(int *)key < tmpintkey){
                if((tmpIndex == beginIndex) || (beginIndex == endIndex)){
                    inmdb_log(LOGDEBUG, "LOGDEBUG:CInmemoryTable:sortsearch:can not find Key[%d]tmpstrkey(%d).\n",*(int *)key,tmpstrkey);
                    return 0;
                }
                endIndex = tmpIndex - 1;
                tmpIndex = beginIndex + (endIndex - beginIndex)/2;
            }
            else{
                while(1)
                {
                    if(tmpIndex == 0){
                        break;
                    }
                    tmpIndex--;
                    int tmpDataIndex = dataIndex;
                    dataIndex = pSort[keyid - pTableDescriptor->numOfHashKey][tmpIndex];
                    if(!buildkey(&tmpintkey,dataIndex,keyid)){
                        inmdb_log(LOGCRITICAL, "FATAL ERROR::CInmemoryTable:sortsearch:Key[%d].tmpintkey(%d)dataIndex(%d) keyid(%d)buildkey return 0.\n",*(int *)key,tmpintkey,dataIndex,keyid);
                        return 0;
                    }
                    if((*(int *)key) != tmpintkey){
                        tmpIndex ++;
                        dataIndex = tmpDataIndex;
                        break;
                    }
                }//end while
                sortIndex = tmpIndex;
                //inmdb_log(LOGCRITICAL, "RESULT::CInmemoryTable:sortsearch: buildkey return dataIndex(%d)sortIndex(%d).\n",dataIndex,sortIndex);
                return 1;
            }//end if
        }//end if
    }//end while
    return 1;
}

/*********************************************
Function: sortsearch:
parameter:sortIndex and dataIndex should be -1 if not found
Description:
Returns: 0 error;1 success
Error Number:
*********************************************/

template <class T> 
int CInmemoryTable<T>::sortdeletesearch(void *key,int keyid, int keyDataIndex, int &sortIndex){
    char tmpstrkey[MAXSTROFKEY];
    int beginIndex;
    int endIndex;
    int tmpIndex;
    int tmpintkey;
    int dataIndex;
    beginIndex = 0;
    sortIndex = -1;
    dataIndex = -1;

    endIndex = pKey[keyid].loadCount - 1;
    memset(tmpstrkey,0,sizeof(tmpstrkey));

    if(endIndex == -1){
        inmdb_log(LOGDEBUG, "LOGDEBUG:CInmemoryTable:sortdeletesearch: pSort[keyid %d] is empty.\n",keyid);
        return 0;
    }
    tmpIndex = endIndex/2;
    while(1){
        dataIndex = pSort[keyid - pTableDescriptor->numOfHashKey][tmpIndex];
        if(pKey[keyid].keyMethod == STRKEYFROMONLYSTR){
            if(!buildkey(tmpstrkey,dataIndex,keyid)){
                inmdb_log(LOGCRITICAL, "buildkey error, Key[%p].tmpstrkey(%s) dataIndex(%d)keyid (%d)buildkey return 0.\n",key,tmpstrkey,dataIndex,keyid);
                return 0;
            }
            if(strcmp((char *)key,tmpstrkey) > 0){
                if((tmpIndex == endIndex) || (beginIndex == endIndex)){
                    inmdb_log(LOGDEBUG, "Can not find Key[%d] tableID(%d) keyid(%d).\n",*(int *)key,tableID,keyid);
                    return 0;
                }
                beginIndex = tmpIndex + 1;
                tmpIndex = beginIndex + (endIndex - beginIndex)/2;
            }
            else if(strcmp((char *)key,tmpstrkey) < 0){
                if((tmpIndex == beginIndex) || (beginIndex == endIndex)){
                    inmdb_log(LOGDEBUG, "Can not find Key[%d] tableID(%d) keyid(%d).\n",*(int *)key,tableID,keyid);
                    return 0;
                }
                endIndex = tmpIndex - 1;
                tmpIndex = beginIndex + (endIndex - beginIndex)/2;
            }
            else{
                while(1)
                {
                    if(tmpIndex == 0){
                        break;
                    }
                    tmpIndex--;
                    int tmpDataIndex = dataIndex;
                    dataIndex = pSort[keyid - pTableDescriptor->numOfHashKey][tmpIndex];
                    if(!buildkey(tmpstrkey,dataIndex,keyid)){
                        inmdb_log(LOGCRITICAL, "buildkey error, Key[%p].tmpstrkey(%s) dataIndex(%d)keyid (%d)buildkey return 0.\n",key,tmpstrkey,dataIndex,keyid);
                        return 0;
                    }
                    if(strcmp((char *)key,tmpstrkey) != 0){
                        tmpIndex ++;
                        dataIndex = tmpDataIndex;
                        break;
                    }
                }
                while(1)
                {
                    if(keyDataIndex == dataIndex){
                        break;
                    }
                    if(tmpIndex == 0 ){
                        inmdb_log(LOGCRITICAL, "tmpIndex error, Key[%p].tmpstrkey(%s) keyDataIndex(%d)dataIndex(%d)keyid (%d)return 0.\n",key,tmpstrkey,keyDataIndex,dataIndex,keyid);
                        return 0;
                    }
                    tmpIndex++;
                    //int tmpDataIndex = dataIndex;
                    dataIndex = pSort[keyid - pTableDescriptor->numOfHashKey][tmpIndex];
                    if(tmpIndex > pKey[keyid].loadCount - 1){
                        inmdb_log(LOGCRITICAL, "tmpIndex error, Key[%p].tmpstrkey(%s) keyDataIndex(%d)dataIndex(%d)keyid (%d)can not find.\n",key,tmpstrkey,keyDataIndex,dataIndex,keyid);
                        return 0;
                    }
                    if(!buildkey(tmpstrkey,dataIndex,keyid)){
                        inmdb_log(LOGCRITICAL, "buildkey error, Key[%p].tmpstrkey(%s) dataIndex(%d)keyid (%d)buildkey return 0.\n",key,tmpstrkey,dataIndex,keyid);
                        return 0;
                    }
                    if(strcmp((char *)key,tmpstrkey) != 0){
                        inmdb_log(LOGCRITICAL, "Wrong key? Key[%p].tmpstrkey(%s) keyDataIndex(%d)dataIndex(%d)keyid (%d)return 0.\n",key,tmpstrkey,keyDataIndex,dataIndex,keyid);
                        return 0;
                    }
                }
                sortIndex = tmpIndex;
                return 1;
            }
        }
        else{
            if(!buildkey(&tmpintkey,dataIndex,keyid)){
                inmdb_log(LOGCRITICAL, "sortdeletesearch:Key[%d].tmpintkey(%d)dataIndex(%d)keyid(%d) buildkey return 0.\n",*(int *)key,tmpintkey,dataIndex,keyid);
                return 0;
            }

            if((*(int *)key) > tmpintkey){
                if((tmpIndex == endIndex) || (beginIndex == endIndex)){
                    inmdb_log(LOGDEBUG, "LOGDEBUG:CInmemoryTable:sortdeletesearch:can not find Key[%d].\n",*(int *)key);
                    return 0;
                }
                beginIndex = tmpIndex + 1;
                tmpIndex = beginIndex + (endIndex - beginIndex)/2;
            }
            else if(*(int *)key < tmpintkey){
                if((tmpIndex == beginIndex) || (beginIndex == endIndex)){
                    inmdb_log(LOGDEBUG, "LOGDEBUG:CInmemoryTable:sortdeletesearch:can not find Key[%d].\n",*(int *)key);
                    return 0;
                }
                endIndex = tmpIndex - 1;
                tmpIndex = beginIndex + (endIndex - beginIndex)/2;
            }
            else{
                while(1)
                {
                    if(tmpIndex == 0){
                        break;
                    }
                    tmpIndex--;
                    int tmpDataIndex = dataIndex;
                    dataIndex = pSort[keyid - pTableDescriptor->numOfHashKey][tmpIndex];
                    if(!buildkey(&tmpintkey,dataIndex,keyid)){
                        inmdb_log(LOGCRITICAL, "sortdeletesearch:Key[%d].tmpintkey(%d)dataIndex(%d) keyid(%d)buildkey return 0.\n",*(int *)key,tmpintkey,dataIndex,keyid);
                        return 0;
                    }
                    if((*(int *)key) != tmpintkey){
                        tmpIndex ++;
                        dataIndex = tmpDataIndex;
                        break;
                    }
                }
                //end while
                while(1)
                {
                    if(keyDataIndex == dataIndex){
                        break;
                    }
                    tmpIndex++;
                    //int tmpDataIndex = dataIndex;
                    dataIndex = pSort[keyid - pTableDescriptor->numOfHashKey][tmpIndex];
                    int loadCount = pKey[keyid].loadCount;
                    if(tmpIndex > (loadCount - 1)){
                        inmdb_log(LOGCRITICAL, "Key[%d].tmpintkey(%d)keyDataIndex(%d)dataIndex(%d) keyid(%d)can not find .\n",*(int *)key,tmpintkey,keyDataIndex,dataIndex,keyid);
                        return 0;
                    }
                    if(!buildkey(&tmpintkey,dataIndex,keyid)){
                        inmdb_log(LOGCRITICAL, "sortdeletesearch:Key[%d].tmpintkey(%d)dataIndex(%d) keyid(%d)buildkey return 0.\n",*(int *)key,tmpintkey,dataIndex,keyid);
                        return 0;
                    }
                    if((*(int *)key) != tmpintkey){
                        inmdb_log(LOGCRITICAL, "sortdeletesearch:Key[%d].tmpintkey(%d)keyDataIndex(%d)dataIndex(%d) keyid(%d)return 0.\n",*(int *)key,tmpintkey,keyDataIndex,dataIndex,keyid);
                        return 0;
                    }
                }
                //end while
                sortIndex = tmpIndex;
                //inmdb_log(LOGCRITICAL, "RESULT::CInmemoryTable:sortsearch: buildkey return dataIndex(%d)sortIndex(%d).\n",dataIndex,sortIndex);
                return 1;
            }//end if
        }//end if
    }//end while
    return 1;
}

/*********************************************
Function: sortsearchprefix:
parameter:
Description:
Returns: 0 error;1 success
Error Number:
*********************************************/
template <class T> 
int CInmemoryTable<T>::sortsearchprefix(void *key,int keyid, int &dataIndex, int &sortIndex){
    char tmpstrkey[MAXSTROFKEY];
    int beginIndex;
    int endIndex;
    int tmpIndex;

    endIndex = pKey[keyid].loadCount - 1;
    memset(tmpstrkey,0,sizeof(tmpstrkey));

    if(endIndex == -1){
        inmdb_log(LOGDEBUG, "LOGDEBUG:CInmemoryTable:sortsearchprefix: pSort[keyid] is empty.\n",keyid);
        return 0;
    }
    tmpIndex = endIndex/2;
    while(1){
        dataIndex = pSort[keyid - pTableDescriptor->numOfHashKey][tmpIndex];
        if(!buildkey(tmpstrkey,dataIndex,keyid)){
            inmdb_log(LOGCRITICAL, "FATAL ERROR::CInmemoryTable:sortsearchprefix:Key[%s].tmpstrkey(%s)dataIndex(%d) keyid(%d)buildkey return 0.\n",(char *)key,tmpstrkey,dataIndex,keyid);
            return 0;
        }
        if(strcmp((char *)key,tmpstrkey) > 0){
            if((tmpIndex == endIndex) || (beginIndex == endIndex)){
                if(endIndex < pKey[keyid].loadCount - 1){
                    tmpIndex = endIndex + 1;
                    dataIndex = pSort[keyid - pTableDescriptor->numOfHashKey][tmpIndex];
                    if(!buildkey(tmpstrkey,dataIndex,keyid)){
                        inmdb_log(LOGCRITICAL, "FATAL ERROR::CInmemoryTable:sortsearchprefix:Key[%s].tmpstrkey(%s)dataIndex(%d) keyid(%d)buildkey return 0",(char *)key,tmpstrkey,dataIndex,keyid);
                        return 0;
                    }
                    if(strncmp((char *)key,tmpstrkey,strlen((char *)key)) == 0){
                        sortIndex = tmpIndex ;
                        dataIndex = pSort[keyid - pTableDescriptor->numOfHashKey][tmpIndex];
                        return 1;
                    }
                }
                inmdb_log(LOGDEBUG, "LOGDEBUG:CInmemoryTable:sortsearchprefix:can not find Key[%s]tableID(%d) keyid(%d).\n",*(int *)key,tableID,keyid);
                return 0;
            }
            beginIndex = tmpIndex + 1;
            tmpIndex = beginIndex + (endIndex - beginIndex)/2;
        }
        else if(strcmp((char *)key,tmpstrkey) < 0){
            if((tmpIndex == beginIndex) || (beginIndex == endIndex)){
                if(strncmp((char *)key,tmpstrkey,strlen((char *)key)) == 0){
                    sortIndex = tmpIndex ;
                    dataIndex = pSort[keyid - pTableDescriptor->numOfHashKey][tmpIndex];
                    return 1;
                }
                inmdb_log(LOGDEBUG, "LOGDEBUG:CInmemoryTable:sortsearchprefix:can not find Key[%s]tableID(%d) keyid(%d).\n",*(int *)key,tableID,keyid);
                return 0;
            }
            endIndex = tmpIndex - 1;
            tmpIndex = beginIndex + (endIndex - beginIndex)/2;
        }
        else{
            while(1)
            {
                if(tmpIndex == 0){
                    break;
                }
                tmpIndex--;
                int tmpDataIndex = dataIndex;
                dataIndex = pSort[keyid - pTableDescriptor->numOfHashKey][tmpIndex];
                if(!buildkey(tmpstrkey,dataIndex,keyid)){
                    inmdb_log(LOGCRITICAL, "FATAL ERROR::CInmemoryTable:sortsearchprefix:Key[%s].tmpstrkey(%s)dataIndex(%d) keyid(%d)buildkey return 0.\n",(char *)key,tmpstrkey,dataIndex,keyid);
                    return 0;
                }
                if(strcmp((char *)key,tmpstrkey) != 0){
                    tmpIndex ++;
                    dataIndex = tmpDataIndex;
                    break;
                }
            }
            sortIndex = tmpIndex;
            return 1;
        }
    }
    return 1;
}

/*********************************************
Function: search:
parameter:
Description:
Returns: 0 error;1 success
Error Number:
*********************************************/
//
template <class T> 
TABLEINDEX CInmemoryTable<T>::search(T& tempdata,int keyid){
    char tmpstrkey[MAXSTROFKEY];
    TABLEINDEX tableindex;
    int dataIndex = 0;

    memset(tmpstrkey,0,sizeof(tmpstrkey));

    if(pTableDescriptor == NULL || keyid < 0 || keyid >(pTableDescriptor->numOfHashKey + pTableDescriptor->numOfSortKey - 1)){
        inmdb_log(LOGCRITICAL, "FATAL ERROR::CInmemoryTable::search pTableDescriptor == NULL || keyid < 0 || keyid >(pTableDescriptor->numOfHashKey + pTableDescriptor->numOfSortKey - 1).\n",0);
        return 0;
    }

    pInmemDB->lock(tableID);
    if(pKey[keyid].searchMethod == HASHSEARCH){
        memset(tmpstrkey,0,sizeof(tmpstrkey));
        if(!buildkey(tmpstrkey,tempdata,keyid)){
            tableindex.dataIndex = 0;
            pInmemDB->unLock(tableID);
            return tableindex;
        }
        int tmphashIndex = hashfunc(tmpstrkey);
        dataIndex = pHash[keyid][tmphashIndex];
        if(!dataIndex){
            tableindex.dataIndex = 0;
            pInmemDB->unLock(tableID);
            inmdb_log(LOGDEBUG, "dataIndex=%d tmphashIndex=%d\n", dataIndex,tmphashIndex);
            return tableindex;
        }

        char key[MAXSTROFKEY];
        memset(key,0,sizeof(key));
        if(!buildkey(key,dataIndex,keyid)){
            tableindex.dataIndex = 0;
            pInmemDB->unLock(tableID);
            return tableindex;
        }
        int hashIndex = hashfunc(key);

        if(hashIndex != tmphashIndex){
            tableindex.dataIndex = 0;
            pInmemDB->unLock(tableID);
            inmdb_log(LOGDEBUG, "key = '%s' tmpstrkey='%s' hashIndex=%d tmphashIndex=%d\n",key,tmpstrkey,hashIndex,tmphashIndex);
            return tableindex;
        }

        if(!strcmp(tmpstrkey,key)){
            //success
            tableindex.dataIndex = dataIndex;
            tableindex.internalIndex = hashIndex;
            pInmemDB->unLock(tableID);
            return tableindex;
        }

        if(pHashNext[keyid][hashIndex] == -1){
            tableindex.dataIndex = 0;
            inmdb_log(LOGDEBUG, "key = '%s' tmpstrkey='%s'\n",key,tmpstrkey);
            pInmemDB->unLock(tableID);
            return tableindex;
        }

        //go to hashnext
        int hashnextIndex = pHashNext[keyid][hashIndex];
        if(pHash[keyid][hashnextIndex] == 0){
            tableindex.dataIndex = 0;
            inmdb_log(LOGDEBUG, "key = '%s' tmpstrkey='%s'\n",key,tmpstrkey);
            pInmemDB->unLock(tableID);
            return tableindex;
        }

        while(hashnextIndex >= 0){
            dataIndex = pHash[keyid][hashnextIndex];
            memset(key,0,sizeof(key));
            if(!dataIndex || !buildkey(key,dataIndex,keyid)){
                inmdb_log(LOGDEBUG, "key = '%s' tmpstrkey='%s' dataIndex=%d hashnextIndex=%d\n",key,tmpstrkey,dataIndex,hashnextIndex);
                tableindex.dataIndex = 0;
                pInmemDB->unLock(tableID);
                return tableindex;
            }
            int tmphashnextIndex = hashfunc(key);
            if(tmphashnextIndex != tmphashIndex){
                inmdb_log(LOGDEBUG, "key = '%s' tmpstrkey='%s' dataIndex=%d hashnextIndex=%d\n",key,tmpstrkey,dataIndex,hashnextIndex);
                tableindex.dataIndex = 0;
                pInmemDB->unLock(tableID);
                return tableindex;
            }
            else if(!strcmp(tmpstrkey,key)){
                tableindex.dataIndex = dataIndex;
                tableindex.internalIndex = hashnextIndex;
                pInmemDB->unLock(tableID);
                return tableindex;
            }
            inmdb_log(LOGDEBUG, "key = '%s' tmpstrkey='%s' dataIndex=%d tmphashnextIndex=%d hashnextIndex=%d\n",key,tmpstrkey,dataIndex,tmphashnextIndex,hashnextIndex);
            hashnextIndex = pHashNext[keyid][hashnextIndex];
        }
    }
    else if(pKey[keyid].searchMethod == SORTSEARCH){
        //if(pTableDescriptor->numOfSortKey <= 0)
        //    return 1;
        int internalIndex;
        if(!sortsearch(tempdata,keyid,dataIndex,internalIndex)){
            inmdb_log(LOGDEBUG, "search keyid(%d)sortsearch return 0.\n",keyid);
            tableindex.dataIndex = 0;
            pInmemDB->unLock(tableID);
            return tableindex;
        }
        tableindex.dataIndex = dataIndex;
        if(internalIndex == pKey[keyid].loadCount - 1){
            tableindex.internalIndex = -1;
        }
        else{
            tableindex.internalIndex = internalIndex + 1;
        }
    }
    pInmemDB->unLock(tableID);
    return tableindex;
}

/*********************************************
Function: searchNext:
parameter:
Description:
Returns: 0 error;1 success
Error Number:
*********************************************/

template <class T>
TABLEINDEX CInmemoryTable<T>::searchNext(T& tempdata,int keyid, TABLEINDEX tableindex)
{
    char key[MAXSTROFKEY];
    char tmpstrkey[MAXSTROFKEY];
    int intkey;
    int tmpintkey;
    int hashnextIndex;
    int sortIndex;
    int dataIndex;

    if(!ASSERTTABLEINDEX(tableindex.dataIndex)){
        return 0;
    }

    if(pTableDescriptor == NULL || keyid < 0 || keyid >(pTableDescriptor->numOfHashKey + pTableDescriptor->numOfSortKey - 1)){
        inmdb_log(LOGCRITICAL, "::searchNext pTableDescriptor == NULL || keyid < 0 || keyid >(pTableDescriptor->numOfHashKey + pTableDescriptor->numOfSortKey - 1).\n",0);
        return 0;
    }
    pInmemDB->lock(tableID);
    if(pKey[keyid].searchMethod == HASHSEARCH){
        memset(key,0,sizeof(key));
        if(!buildkey(key,tempdata,keyid)){
            //wrong
            inmdb_log(LOGCRITICAL, "Key[%s].tmpstrkey(%s)keyid(%d)buildkey return 0.\n",(char *)key,tmpstrkey,keyid);
            tableindex.dataIndex = 0;
            pInmemDB->unLock(tableID);
            return tableindex;
        }
        int tmphashIndex = hashfunc(key);

        hashnextIndex = tableindex.internalIndex;
        while(hashnextIndex >= 0){
            hashnextIndex = pHashNext[keyid][hashnextIndex];
            dataIndex = pHash[keyid][hashnextIndex];
            memset(tmpstrkey,0,sizeof(tmpstrkey));
            if(!buildkey(tmpstrkey,dataIndex,keyid)){
                inmdb_log(LOGCRITICAL, "Key[%s].tmpstrkey(%s)dataIndex(%d) keyid(%d)buildkey return 0.\n",(char *)key,tmpstrkey,dataIndex,keyid);
                tableindex.dataIndex = 0;
                pInmemDB->unLock(tableID);
                return tableindex;
            }
            int tmphashnextIndex = hashfunc(tmpstrkey);
            if(tmphashnextIndex != tmphashIndex){
                inmdb_log(LOGDEBUG, "key = '%s' tmpstrkey='%s' dataIndex=%d hashnextIndex=%d\n",key,tmpstrkey,dataIndex,hashnextIndex);
                tableindex.dataIndex = 0;
                pInmemDB->unLock(tableID);
                return tableindex;
            }
            else if(!strcmp(tmpstrkey,(char *)key)){
                tableindex.dataIndex = dataIndex;
                tableindex.internalIndex = hashnextIndex;
                pInmemDB->unLock(tableID);
                return tableindex;
            }
            inmdb_log(LOGDEBUG, "key = '%s' tmpstrkey='%s' dataIndex=%d tmphashnextIndex=%d hashnextIndex=%d\n",key,tmpstrkey,dataIndex,tmphashnextIndex,hashnextIndex);
        }
        //cann't find next
        return 0;
    }
    else{
        sortIndex = tableindex.internalIndex;
        if(sortIndex < 0 || sortIndex > (pTableDescriptor->capacity + 2) - 1){
            tableindex.dataIndex = 0;
            pInmemDB->unLock(tableID);
            return tableindex;
        }
        dataIndex = pSort[keyid - pTableDescriptor->numOfHashKey][sortIndex];
        if(pKey[keyid].keyMethod == STRKEYFROMONLYSTR){
            if(!buildkey(key,tempdata,keyid)){
                //wrong
                inmdb_log(LOGCRITICAL, "Key[%s].tmpstrkey(%s)dataIndex(%d) keyid(%d)buildkey return 0.\n",(char *)key,tmpstrkey,dataIndex,keyid);
                tableindex.dataIndex = 0;
                pInmemDB->unLock(tableID);
                return tableindex;
            }
            if(!buildkey(tmpstrkey,dataIndex,keyid)){
                //wrong
                inmdb_log(LOGCRITICAL, "Key[%s].tmpstrkey(%s)dataIndex(%d) keyid(%d)buildkey return 0.\n",(char *)key,tmpstrkey,dataIndex,keyid);
                tableindex.dataIndex = 0;
                pInmemDB->unLock(tableID);
                return tableindex;
            }
            if(strcmp(tmpstrkey,(char *)key) == 0){
                tableindex.dataIndex = dataIndex;
                if(sortIndex < pKey[keyid].loadCount - 1){
                    tableindex.internalIndex = sortIndex + 1;
                }
                else{
                    tableindex.internalIndex = -1;
                }
                pInmemDB->unLock(tableID);
                return tableindex;
            }
            inmdb_log(LOGDEBUG, "searchNext:can not find Key[%s].tmpstrkey(%s)dataIndex(%d) keyid(%d).\n",(char *)key,tmpstrkey,dataIndex,keyid);
        }
        else{
            if(!buildkey(&intkey,tempdata,keyid)){
                //wrong
                inmdb_log(LOGCRITICAL, ":searchnext:Key[%d].tmpintkey(%d)dataIndex(%d)keyid(%d) buildkey return 0.\n",*(int *)key,tmpintkey,dataIndex,keyid);
                tableindex.dataIndex = 0;
                pInmemDB->unLock(tableID);
                return tableindex;
            }
            if(!buildkey(&tmpintkey,dataIndex,keyid)){
                //wrong
                inmdb_log(LOGCRITICAL, ":searchnext:Key[%d].tmpintkey(%d)dataIndex(%d)keyid(%d) buildkey return 0.\n",*(int *)key,tmpintkey,dataIndex,keyid);
                tableindex.dataIndex = 0;
                pInmemDB->unLock(tableID);
                return tableindex;
            }
            //            if(*(int *)key == tmpintkey){
            if(intkey == tmpintkey){
                tableindex.dataIndex = dataIndex;
                if(sortIndex < pKey[keyid].loadCount - 1){
                    tableindex.internalIndex = sortIndex + 1;
                }
                else{
                    tableindex.internalIndex = -1;
                }
                pInmemDB->unLock(tableID);
                return tableindex;
            }
            inmdb_log(LOGDEBUG, "searchnext:can not find intkey[%d].tmpintkey(%d)dataIndex(%d)keyid(%d).\n",intkey,tmpintkey,dataIndex,keyid);
        }
    }

    tableindex.dataIndex = 0;
    pInmemDB->unLock(tableID);
    return tableindex;
}

/*********************************************
Function: searchPrefix:
parameter:
Description:
Returns: 0 error;1 success
Error Number:
*********************************************/

template <class T>
TABLEINDEX CInmemoryTable<T>::searchPrefix(void *key,int keyid){
    TABLEINDEX tableindex;
    int dataIndex;
    int internalIndex;

    pInmemDB->lock(tableID);
    if(pTableDescriptor == NULL || keyid < 0 || keyid >(pTableDescriptor->numOfHashKey + pTableDescriptor->numOfSortKey - 1)){
        inmdb_log(LOGCRITICAL, "::searchPrefix pTableDescriptor == NULL || keyid < 0 || keyid >(pTableDescriptor->numOfHashKey + pTableDescriptor->numOfSortKey - 1).\n",0);
        return 0;
    }
    if(!sortsearchprefix(key,keyid,dataIndex,internalIndex)){
        inmdb_log(LOGDEBUG, "searchPrefix can not find key[%s]keyid(%d).\n",(char *)key,keyid);
        tableindex.dataIndex = 0;
        pInmemDB->unLock(tableID);
        return tableindex;
    }
    pInmemDB->unLock(tableID);
    tableindex.dataIndex = dataIndex;
    if(internalIndex < pKey[keyid].loadCount - 1){
        tableindex.internalIndex = internalIndex + 1;
    }
    else{
        tableindex.internalIndex = -1;
    }
    return tableindex;
}

/*********************************************
Function: searchPrefixNext
parameter:
Description:
Returns: 0 error;1 success
Error Number:
*********************************************/
template <class T>
TABLEINDEX CInmemoryTable<T>::searchPrefixNext(void *key,int keyid, TABLEINDEX tableindex){
    char tmpstrkey[MAXSTROFKEY];
    int dataIndex;
    int sortIndex;

    memset(tmpstrkey,0,sizeof(tmpstrkey));

    if(pTableDescriptor == NULL || keyid < 0 || keyid >(pTableDescriptor->numOfHashKey + pTableDescriptor->numOfSortKey - 1)){
        inmdb_log(LOGCRITICAL, "::searchPrefix pTableDescriptor == NULL || keyid < 0 || keyid >(pTableDescriptor->numOfHashKey + pTableDescriptor->numOfSortKey - 1).\n",0);
        return 0;
    }
    sortIndex = tableindex.internalIndex;
    if(sortIndex < 0 || sortIndex > (pTableDescriptor->capacity + 2)){
        tableindex.dataIndex = 0;
        pInmemDB->unLock(tableID);
        return tableindex;
    }
    dataIndex = pSort[keyid][sortIndex];
    pInmemDB->lock(tableID);
    if(!buildkey(tmpstrkey,dataIndex,keyid)){
        //wrong
        inmdb_log(LOGCRITICAL, ":searchPrefixNextKey[%s].tmpstrkey(%s)dataIndex(%d) keyid(%d)buildkey return 0.\n",(char *)key,tmpstrkey,dataIndex,keyid);
        tableindex.dataIndex = 0;
        pInmemDB->unLock(tableID);
        return tableindex;
    }
    pInmemDB->unLock(tableID);
    if(strncmp(tmpstrkey,(char *)key, strlen((char *)key)) == 0){
        tableindex.dataIndex = dataIndex;
        if(sortIndex < pKey[keyid].loadCount - 1){
            tableindex.internalIndex = sortIndex + 1;
        }
        else{
            tableindex.internalIndex = -1;
        }
        return tableindex;
    }
    inmdb_log(LOGDEBUG, "searchPrefixNext:can not find Key[%s].tmpstrkey(%s)dataIndex(%d) keyid(%d).\n",(char *)key,tmpstrkey,dataIndex,keyid);
    tableindex.dataIndex = 0;
    return tableindex;
}

/*********************************************
Function: datainsert
parameter:
Description:
Returns: 0 error;1 success
Error Number:
*********************************************/
template <class T>
int CInmemoryTable<T>::datainsert(T& tempdata,int &dataIndex){
    //idataNext point to the first data in unused list after datainsert
    int idataNext;

    dataIndex = dataNext[UNUSEDHEAD];
    if(dataIndex == UNUSEDHEAD){
        inmdb_log(LOGCRITICAL, "Result:CInmemoryTable:datainsert:shm(%d) table(%d) is full(%d).\n",pInmemDB->getShmId(),tableID,pTableDescriptor->capacity);
        return 0;
    }
    idataNext = dataNext[dataIndex];
    //the used link
    dataPre[dataNext[USEDHEAD]] = dataIndex;
    dataNext[dataIndex] = dataNext[USEDHEAD];
    dataPre[dataIndex] = USEDHEAD;
    dataNext[USEDHEAD] = dataIndex;

    //the null link
    dataPre[idataNext] = UNUSEDHEAD;
    dataNext[UNUSEDHEAD] = idataNext;
    memcpy(&pData[dataIndex], &tempdata, sizeof(T));
    pTableDescriptor->loadCount ++;
    timeStamp[dataIndex] = time(NULL);
    return 1;
}

/*********************************************
Function: datadelete
parameter:
Description:
Returns: 0 error;1 success
Error Number:
*********************************************/
template <class T>
int CInmemoryTable<T>::datadelete(int dataIndex){
    //idataNext point to next dataof the deleted data
    //idataPre point to pre dataof the deleted data
    int idataNext;
    int idataPre;

    idataNext = dataNext[dataIndex];
    idataPre = dataPre[dataIndex];

    dataPre[idataNext] = idataPre;
    dataNext[idataPre] = idataNext;

    dataNext[dataPre[UNUSEDHEAD]] = dataIndex;
    dataNext[dataIndex] = UNUSEDHEAD;
    dataPre[dataIndex] = dataPre[UNUSEDHEAD];
    dataPre[UNUSEDHEAD] = dataIndex;
    if (bzeroFlag)
        memset((void*)&pData[dataIndex], 0, sizeof(T));

    timeStamp[dataIndex] = 0;
    pTableDescriptor->loadCount --;
    return 1;
}

/*********************************************
Function: hashinsert
parameter:
Description:
Returns: false - error;true - success
Error Number:
*********************************************/
template <class T>
bool CInmemoryTable<T>::hashinsert(int keyid, int dataIndex){
    // tmpStrKey is key of data
    // hashIndex is index in the hash list
    char tmpStrKey[MAXSTROFKEY];
    int hashIndex;

    if(!dataIndex){
        inmdb_log(LOGDEBUG, "dataIndex=%d.\n",dataIndex);
    }

    memset(tmpStrKey,0,sizeof(tmpStrKey));
    int newhashindex;
    newhashindex = 0;

    if(!buildkey(tmpStrKey,dataIndex,keyid)){
        inmdb_log(LOGCRITICAL, ":hashinsert.tmpstrkey(%s)dataIndex(%d) keyid(%d)buildkey return 0.\n",tmpStrKey,dataIndex,keyid);
        return false;
    }
    hashIndex = hashfunc(tmpStrKey);
    inmdb_log(LOGDEBUG, "tmpStrKey='%s' hashIndex = %d dataIndex = %d",tmpStrKey,hashIndex,dataIndex);
    if (pHash[keyid][hashIndex] == 0){
        pHash[keyid][hashIndex] = dataIndex;
        pHashNext[keyid][hashIndex] = -1;
    }
    else{
        int oldDataIndex = pHash[keyid][hashIndex];
        memset(tmpStrKey,0,sizeof(tmpStrKey));
        if(!buildkey(tmpStrKey,oldDataIndex,keyid)){
            inmdb_log(LOGCRITICAL, ":hashinsert.tmpstrkey(%s)dataIndex(%d) keyid(%d)buildkey return 0.",tmpStrKey,dataIndex,keyid);
            return false;
        }
        int oldhashindexhead = hashfunc(tmpStrKey);
        inmdb_log(LOGDEBUG, "oldtmpStrKey='%s' oldhashindexhead = %d oldDataIndex=%d hashIndex=%d",tmpStrKey,oldhashindexhead,oldDataIndex,hashIndex);
        if(oldhashindexhead != hashIndex){
            int preoldhashindex = -1;
            int oldhashindex = oldhashindexhead;
            while(oldhashindex >= 0 && oldhashindex != hashIndex){
                preoldhashindex = oldhashindex;
                oldhashindex = pHashNext[keyid][oldhashindex];
            }

            if(!oldhashindex){
                inmdb_log(LOGDEBUG, "get the hashIndex(%d) tmpstrkey(%s)dataIndex(%d) keyid(%d) error .",hashIndex,tmpStrKey,dataIndex,keyid);
                return false;
            }

            //find the empty pHash unit
            for(newhashindex = dataIndex;newhashindex<pTableDescriptor->hashPrimeNumber;newhashindex++){
                if(pHash[keyid][newhashindex] == 0) break;
            }
            if(newhashindex == pTableDescriptor->hashPrimeNumber){
                for(newhashindex = dataIndex; newhashindex >= 2; newhashindex--){
                    if(pHash[keyid][newhashindex] == 0) break;
                }
                if(newhashindex == 1){
                    inmdb_log(LOGDEBUG, "cann't find the pHash[%d]\n",keyid);
                    return false;
                }
            }

            inmdb_log(LOGDEBUG, "dataIndex=%d hashIndex=%d newhashindex = %d [%d->%d->%d]\n",dataIndex,hashIndex,newhashindex,preoldhashindex,hashIndex,pHashNext[keyid][hashIndex]);
            pHash[keyid][newhashindex] = oldDataIndex;
            pHashNext[keyid][newhashindex] = pHashNext[keyid][hashIndex];
            pHashNext[keyid][preoldhashindex] = newhashindex;

            pHash[keyid][hashIndex] = dataIndex;
            pHashNext[keyid][hashIndex] = -1;
            inmdb_log(LOGDEBUG, "dataIndex=%d hashIndex=%d newhashindex = %d [%d->%d->%d]\n",dataIndex,hashIndex,newhashindex,preoldhashindex,pHashNext[keyid][preoldhashindex],pHashNext[keyid][newhashindex]);
        }
        else{
            //find the empty pHash unit
            for(newhashindex = dataIndex;newhashindex<pTableDescriptor->hashPrimeNumber;newhashindex++){
                if(pHash[keyid][newhashindex] == 0) break;
            }
            if(newhashindex == pTableDescriptor->hashPrimeNumber){
                for(newhashindex = dataIndex; newhashindex >= 2; newhashindex--){
                    if(pHash[keyid][newhashindex] == 0) break;
                }
                if(newhashindex == 1){
                    inmdb_log(LOGDEBUG, "cann't find the pHash[%d]\n",keyid);
                    return false;
                }
            }

            pHash[keyid][newhashindex] = dataIndex;
            pHashNext[keyid][newhashindex] = pHashNext[keyid][hashIndex];
            pHashNext[keyid][hashIndex] = newhashindex;
        }
    }
    return true;
}
/*********************************************
Function: sortinsert
parameter:
Description:
Returns: false - error;true - success
Error Number:
*********************************************/
template <class T>
bool CInmemoryTable<T>::sortinsert(int keyid,int dataIndex){
    char tmpstrkey[MAXSTROFKEY];
    char strkey[MAXSTROFKEY];
    int beginIndex;
    int endIndex;
    int tmpIndex;
    int tmpintkey;
    int tmpDataIndex;
    int intkey;

    memset(tmpstrkey,0,sizeof(tmpstrkey));
    memset(strkey,0,sizeof(strkey));
    beginIndex = 0;
    endIndex = pKey[keyid].loadCount - 1;
    tmpIndex = endIndex/2;
    tmpDataIndex = 0;

    if(endIndex == -1){
        pSort[keyid - pTableDescriptor->numOfHashKey][0] = dataIndex;
        pKey[keyid].loadCount ++;
        return true;
    }
    if(pKey[keyid].keyMethod == STRKEYFROMONLYSTR){
        buildkey(strkey,dataIndex,keyid);
    }
    else{
        buildkey(&intkey,dataIndex,keyid);
    }
    while(1){
        tmpDataIndex = pSort[keyid - pTableDescriptor->numOfHashKey][tmpIndex];
        inmdb_log(LOGDEBUG, "ERROR::CInmemoryTable:sortinsert.tmpstrkey(%s)tmpDataIndex(%d) keyid(%d) keyMethod %d.\n",tmpstrkey,tmpDataIndex,keyid,pKey[keyid].keyMethod);
        if(pKey[keyid].keyMethod == STRKEYFROMONLYSTR){
            if(!buildkey(tmpstrkey,tmpDataIndex,keyid)){
                inmdb_log(LOGCRITICAL, ":sortinsert.tmpstrkey(%s)tmpDataIndex(%d) keyid(%d)buildkey return false.\n",tmpstrkey,tmpDataIndex,keyid);
                return false;
            }

            if(strcmp(strkey,tmpstrkey) > 0){
                if((tmpIndex == endIndex) || (beginIndex == endIndex)){
                    for(int sortIndex = pKey[keyid].loadCount - 1; sortIndex > tmpIndex; sortIndex--){
                        pSort[keyid - pTableDescriptor->numOfHashKey][sortIndex + 1] = pSort[keyid - pTableDescriptor->numOfHashKey][sortIndex];
                    }

                    pKey[keyid].loadCount ++;
                    pSort[keyid - pTableDescriptor->numOfHashKey][tmpIndex + 1] = dataIndex;
                    //inmdb_log(LOGCRITICAL, "RESULT::CInmemoryTable:sortinsert: success dataIndex(%d).\n",dataIndex);
                    return true;
                }
                beginIndex = tmpIndex + 1;
                tmpIndex = beginIndex + (endIndex - beginIndex)/2;
            }
            else if(strcmp(strkey,tmpstrkey) < 0){
                if((tmpIndex == beginIndex) || (beginIndex == endIndex)){
                    for(int sortIndex = pKey[keyid].loadCount - 1; sortIndex > tmpIndex - 1; sortIndex--){
                        pSort[keyid - pTableDescriptor->numOfHashKey][sortIndex + 1] = pSort[keyid - pTableDescriptor->numOfHashKey][sortIndex];
                    }
                    pKey[keyid].loadCount ++;
                    pSort[keyid - pTableDescriptor->numOfHashKey][tmpIndex] = dataIndex;
                    return true;
                }
                endIndex = tmpIndex - 1;
                tmpIndex = beginIndex + (endIndex - beginIndex)/2;
            }
            else{
                for(int sortIndex = pKey[keyid].loadCount - 1; sortIndex > tmpIndex; sortIndex--){
                    pSort[keyid - pTableDescriptor->numOfHashKey][sortIndex + 1] = pSort[keyid - pTableDescriptor->numOfHashKey][sortIndex];
                }
                pSort[keyid - pTableDescriptor->numOfHashKey][tmpIndex + 1] = dataIndex;
                pKey[keyid].loadCount ++;
                return true;
            }
        }
        else if(pKey[keyid].keyMethod == INTKEYFROMONLYINT){
            if(!buildkey(&tmpintkey,tmpDataIndex,keyid)){
                inmdb_log(LOGCRITICAL, ":sortinsert.tmpintkey(%d)tmpDataIndex(%d) keyid(%d)buildkey return false.\n",tmpintkey,tmpDataIndex,keyid);
                return false;
            }
            if(intkey > tmpintkey){
                if((tmpIndex == endIndex) || (beginIndex == endIndex)){
                    for(int sortIndex = pKey[keyid].loadCount - 1; sortIndex > tmpIndex; sortIndex--){
                        pSort[keyid - pTableDescriptor->numOfHashKey][sortIndex + 1] = pSort[keyid - pTableDescriptor->numOfHashKey][sortIndex];
                    }
                    pKey[keyid].loadCount ++;
                    pSort[keyid - pTableDescriptor->numOfHashKey][tmpIndex + 1] = dataIndex;
                    //inmdb_log(LOGCRITICAL, "RESULT::CInmemoryTable:sortinsert: success dataIndex(%d).\n",dataIndex);
                    return true;
                }
                beginIndex = tmpIndex + 1;
                tmpIndex = beginIndex + (endIndex - beginIndex)/2;
            }
            else if(intkey < tmpintkey){
                if((tmpIndex == beginIndex) || (beginIndex == endIndex)){
                    for(int sortIndex = pKey[keyid].loadCount - 1; sortIndex > tmpIndex - 1; sortIndex--){
                        pSort[keyid - pTableDescriptor->numOfHashKey][sortIndex + 1] = pSort[keyid - pTableDescriptor->numOfHashKey][sortIndex];
                    }
                    pKey[keyid].loadCount ++;
                    pSort[keyid - pTableDescriptor->numOfHashKey][tmpIndex ] = dataIndex;
                    //inmdb_log(LOGCRITICAL, "RESULT::CInmemoryTable:sortinsert: success dataIndex(%d).\n",dataIndex);
                    return true;
                }
                endIndex = tmpIndex - 1;
                tmpIndex = beginIndex + (endIndex - beginIndex)/2;
            }
            else{
                for(int sortIndex = pKey[keyid].loadCount; sortIndex > tmpIndex; sortIndex--){
                    pSort[keyid - pTableDescriptor->numOfHashKey][sortIndex + 1] = pSort[keyid - pTableDescriptor->numOfHashKey][sortIndex];
                }
                pSort[keyid - pTableDescriptor->numOfHashKey][tmpIndex + 1] = dataIndex;
                pKey[keyid].loadCount ++;
                return true;
            }
        }
        else{
            inmdb_log(LOGCRITICAL, ":sortinsert:keyid(%d) keyMethod (%d) error.\n",keyid,pKey[keyid].keyMethod);
            return false;
        }
    }
    return true;
}

/*********************************************
Function: hashdelete
parameter:
Description:
Returns: false - error;true - success
Error Number:
*********************************************/

template <class T>
bool CInmemoryTable<T>::hashdelete(int keyid,int dataIndex)
{
    char strkey[MAXSTROFKEY];
    int hashIndex;

    memset(strkey,0,sizeof(strkey));
    if(!buildkey(strkey,dataIndex,keyid)){
        inmdb_log(LOGCRITICAL, ":hashdelete.strkey(%s)dataIndex(%d) keyid(%d)buildkey return 0.\n",strkey,dataIndex,keyid);
        return false;
    }
    hashIndex = hashfunc(strkey);

    inmdb_log(LOGDEBUG, "pHashNext[keyid][hashIndex]= %d pHash[keyid][hashIndex]=%d hashIndex = %d dataIndex=%d\n",pHashNext[keyid][hashIndex],pHash[keyid][hashIndex],hashIndex,dataIndex);
    if(pHash[keyid][hashIndex] == dataIndex){
        int secondHashIndex = pHashNext[keyid][hashIndex];
        if(secondHashIndex < 0){
            pHash[keyid][hashIndex] = 0;
            pHashNext[keyid][hashIndex] = -1;
        }
        else{
            pHash[keyid][hashIndex] = pHash[keyid][secondHashIndex];
            pHashNext[keyid][hashIndex] = pHashNext[keyid][secondHashIndex];

            pHash[keyid][secondHashIndex] = 0;
            pHashNext[keyid][secondHashIndex] = -1;
        }
    }
    else {
        int prehashIndex = -1;
        while(hashIndex >= 0 && pHash[keyid][hashIndex] != dataIndex){
            prehashIndex = hashIndex;
            hashIndex = pHashNext[keyid][hashIndex];
        }
        if(pHash[keyid][hashIndex] != dataIndex){
            hashIndex = hashfunc(strkey);
            inmdb_log(LOGCRITICAL, "hashIndex=%d dataIndex=%d strkey='%s'\n",hashIndex,dataIndex,strkey);
            while(hashIndex >= 0 && pHash[keyid][hashIndex] != dataIndex){
                prehashIndex = hashIndex;
                hashIndex = pHashNext[keyid][hashIndex];
                inmdb_log(LOGDEBUG, "hashIndex=%d\n",hashIndex);
            }
            return false;
        }
        int secondHashIndex = pHashNext[keyid][hashIndex];
        if(secondHashIndex == -1){
            pHash[keyid][hashIndex] = 0;
            pHashNext[keyid][hashIndex] = -1;
            if(prehashIndex >=0) pHashNext[keyid][prehashIndex] = -1;
        }
        else{
            pHashNext[keyid][prehashIndex] = pHashNext[keyid][hashIndex];

            pHash[keyid][hashIndex] = 0;
            pHashNext[keyid][hashIndex] = -1;
        }
    }
    return true;
}

/*********************************************
Function: sortdelete
parameter:
Description:
Returns: false - error;true - success
Error Number:
*********************************************/
template <class T>
bool CInmemoryTable<T>::sortdelete(int keyid,int dataIndex)
{
    char strkey[MAXSTROFKEY];
    int intkey;
    int sortIndex;

    memset(strkey,0,sizeof(strkey));

    if(pKey[keyid].keyMethod == STRKEYFROMONLYSTR){
        if(!buildkey(strkey,dataIndex,keyid)){
            inmdb_log(LOGCRITICAL, ":sortdelete:strkey(%s) dataIndex(%d) keyid(%d) buildkey return f.\n",strkey,dataIndex,keyid);
            return false;
        }
        if(!sortdeletesearch(strkey,keyid, dataIndex, sortIndex)){
            inmdb_log(LOGCRITICAL, ":sortdelete:sortdeletesearch fail strkey(%s) dataIndex(%d) keyid(%d) buildkey return f.\n",strkey,dataIndex,keyid);
            return false;
        }
    }
    else if(pKey[keyid].keyMethod == INTKEYFROMONLYINT){
        if(!buildkey(&intkey,dataIndex,keyid)){
            inmdb_log(LOGCRITICAL, ":sortdelete:intkey(%d) dataIndex(%d) keyid(%d) buildkey return f.\n",intkey,dataIndex,keyid);
            return false;
        }
        if(!sortdeletesearch(&intkey,keyid, dataIndex,sortIndex)){
            inmdb_log(LOGCRITICAL, ":sortdelete:sortdeletesearch return 0intkey(%d) dataIndex(%d) keyid(%d).\n",intkey,dataIndex,keyid);
            return false;
        }
    }
    //internalIndex = -1 shows this is the end
    for(int i = sortIndex; i <= pKey[keyid].loadCount - 1; i++)
        pSort[keyid - pTableDescriptor->numOfHashKey][i] = pSort[keyid - pTableDescriptor->numOfHashKey][i + 1];
    pKey[keyid].loadCount--;
    return true;
}

/*********************************************
Function: insert
parameter:
Description:
Returns: index in table.
Error Number:
*********************************************/
template <class T>
//TABLEINDEX CInmemoryTable<T>::insert(T& tempdata){
int CInmemoryTable<T>::insert(T& tempdata){
    int dataIndex;
    //insert data
    pInmemDB->lock(tableID);
    if(!datainsert(tempdata,dataIndex)){
        dataIndex = 0;
        pInmemDB->unLock(tableID);
        return dataIndex;
    }
    if(!dataIndex){
        return dataIndex;
    }
    //insert hash data
    for(int keyid = 0;keyid < pTableDescriptor->numOfHashKey;keyid ++){
        if(!hashinsert(keyid, dataIndex)){
            inmdb_log(LOGCRITICAL, "hashinsert error, keyid %d", keyid);
            if(keyid > 0){
                keyid --;
                for(; keyid >= 0; keyid --){
                    hashdelete(keyid,dataIndex);
                }
            }
            datadelete(dataIndex);
            dataIndex = 0;
            pInmemDB->unLock(tableID);
            return dataIndex;
        }
    }

    //insert sort data
    for(int keyid = pTableDescriptor->numOfHashKey;keyid < (pTableDescriptor->numOfHashKey + pTableDescriptor->numOfSortKey);keyid ++){
        if(!sortinsert(keyid,dataIndex)){
            inmdb_log(LOGCRITICAL, "sortinsert error, keyid %d", keyid);
            if(keyid > pTableDescriptor->numOfHashKey){
                keyid --;
                for(; keyid >= pTableDescriptor->numOfHashKey; keyid --){
                    sortdelete(keyid,dataIndex);
                }
            }
            if(keyid > 0){
                keyid --;
                for(; keyid >= 0; keyid --){
                    hashdelete(keyid,dataIndex);
                }
            }
            datadelete(dataIndex);
            dataIndex = 0;
            pInmemDB->unLock(tableID);
            return dataIndex;
        }
    }

    pInmemDB->unLock(tableID);
    return dataIndex;
}

/*********************************************
Function: deletex
parameter:
Description:
Returns: 0 error;1 success
Error Number:
*********************************************/
template <class T>
int CInmemoryTable<T>::deletex(int dataIndex){
    pInmemDB->lock(tableID);

    if(!ASSERTTABLEINDEX(dataIndex)){
        inmdb_log(LOGCRITICAL, "deletex dataIndex = (%d) dataIndex not exist x(%d)\n",dataIndex,tableID);
        pInmemDB->unLock(tableID);
        return 0;
    }

    if(pTableDescriptor == NULL || dataIndex > (pTableDescriptor->capacity + 2) - 1 || dataIndex == 0 || dataIndex == 1 ){
        inmdb_log(LOGCRITICAL, "deletex pTableDescriptor == NULL || dataIndex = (%d) > (pTableDescriptor->capacity + 2) - 1 tableID(%d)\n",dataIndex,tableID);
        pInmemDB->unLock(tableID);
        return 0;
    }

    if (!bzeroFlag) {
        datadelete(dataIndex);
        pInmemDB->unLock(tableID);
        return 1;
    }

    for(int keyid = 0;keyid < pTableDescriptor->numOfHashKey;keyid ++){
        if(!hashdelete(keyid,dataIndex)){
            inmdb_log(LOGCRITICAL, "hashdelete error. keyid %d", keyid);
        }
    }
    for(int keyid = pTableDescriptor->numOfHashKey;keyid < (pTableDescriptor->numOfHashKey + pTableDescriptor->numOfSortKey);keyid ++){
        if(!sortdelete(keyid,dataIndex)){
            inmdb_log(LOGCRITICAL, "sortdelete error. keyid %d", keyid);
        }
    }
    datadelete(dataIndex);
    pInmemDB->unLock(tableID);
    return 1;
}

/*********************************************
Function: getTableUseRate
parameter:
Description:
Returns: 0 error;1 success
Error Number:
*********************************************/
template <class T>
int CInmemoryTable<T>::getTableUseRate(){
    return (pTableDescriptor->loadCount * 100)/(pTableDescriptor->capacity + 2);
}

template <class T>
int CInmemoryTable<T>::GetTableLoadCount(){
    return pTableDescriptor->loadCount;
}

/*********************************************
Function: end
Description: return the index for last element
Returns: element index
*********************************************/
template <class T>
int  CInmemoryTable<T>::end(){
    return dataPre[USEDHEAD];
}

/*********************************************
Function: traversalPre
parameter: index for current element
Description: return the previous index of current element
Returns: element index
*********************************************/
template <class T>
int  CInmemoryTable<T>::traversalPre(int tableindex){
    tableindex = dataPre[tableindex];
    return tableindex;
}

/*********************************************
Function: begin
Description: return the index for first element
Returns: element index
*********************************************/
template <class T>
int  CInmemoryTable<T>::begin(){
    return dataNext[USEDHEAD];
}

/*********************************************
Function: traversalNext
parameter: index for current element
Description: return the next index of current element
Returns: element index
*********************************************/
template <class T>
int  CInmemoryTable<T>::traversalNext(int tableindex){
    tableindex = dataNext[tableindex];
    return tableindex;
}


/*********************************************
Function: updateData
parameter:
Description:
Returns: 0 error;1 success
Error Number:
*********************************************/
template <class T>
int CInmemoryTable<T>::updateData(int offset,int dataLenth,int dataIndex,const void *pNewData){
    if(!pNewData){
        inmdb_log(LOGCRITICAL, "invalid pNewData %p", pNewData);
        return 0;
    }

    if((pTableDescriptor == NULL) || (dataIndex > (pTableDescriptor->capacity +2-1)) || (dataIndex == 0) || (dataIndex == 1)){
        inmdb_log(LOGCRITICAL, "Result:CInmemoryTable:updateData pTableDescriptor == NULL || dataIndex = (%d) > (pTableDescriptor->capacity + 2) - 1 || dataIndex == 0 || dataIndex == 1.",dataIndex);
        return 0;
    }
    pInmemDB->lock(tableID);
    for(int keyid = 0;keyid < pTableDescriptor->numOfHashKey;keyid ++){
        //inmdb_log(LOGCRITICAL, "pKey[%d].field1Offset = %d,pKey[keyid].field2Offset = %d\n",keyid,pKey[keyid].field1Offset,pKey[keyid].field2Offset);
        if(pKey[keyid].field1Offset == offset || pKey[keyid].field2Offset == offset ){
            inmdb_log(LOGDEBUG, "pKey[%d].field1Offset = %d,pKey[keyid].field2Offset = %d\n",keyid,pKey[keyid].field1Offset,pKey[keyid].field2Offset);
            hashdelete(keyid,dataIndex);
        }
    }
    for(int keyid = pTableDescriptor->numOfHashKey;keyid < (pTableDescriptor->numOfHashKey + pTableDescriptor->numOfSortKey);keyid ++){
        inmdb_log(LOGCRITICAL, "pKey[%d].field1Offset = %d,pKey[keyid].field2Offset = %d\n",keyid,pKey[keyid].field1Offset,pKey[keyid].field2Offset);
        if(pKey[keyid].field1Offset == offset || pKey[keyid].field2Offset == offset ){
            sortdelete(keyid,dataIndex);
        }
    }

    memcpy((void *)((intptr_t)&pData[dataIndex] + offset),pNewData,dataLenth);

    for(int keyid = 0;keyid < pTableDescriptor->numOfHashKey;keyid ++){
        //inmdb_log(LOGCRITICAL, "pKey[%d].field1Offset = %d,pKey[keyid].field2Offset = %d\n",keyid,pKey[keyid].field1Offset,pKey[keyid].field2Offset);
        if(pKey[keyid].field1Offset == offset || pKey[keyid].field2Offset == offset ){
            inmdb_log(LOGDEBUG, "pKey[%d].field1Offset = %d,pKey[keyid].field2Offset = %d\n",keyid,pKey[keyid].field1Offset,pKey[keyid].field2Offset);
            if(!hashinsert(keyid,dataIndex)){
                inmdb_log(LOGCRITICAL, ":updateData: keyid[%d]hashinsert error.\n",keyid);
                if(keyid > 0){
                    keyid --;
                    for(; keyid >= 0; keyid --){
                        hashdelete(keyid,dataIndex);
                    }
                }
                datadelete(dataIndex);
                pInmemDB->unLock(tableID);
                return 0;
            }
        }
    }
    for(int keyid = pTableDescriptor->numOfHashKey;keyid < (pTableDescriptor->numOfHashKey + pTableDescriptor->numOfSortKey);keyid ++){
        inmdb_log(LOGCRITICAL, "pKey[%d].field1Offset = %d,pKey[keyid].field2Offset = %d\n",keyid,pKey[keyid].field1Offset,pKey[keyid].field2Offset);
        if(pKey[keyid].field1Offset == offset || pKey[keyid].field2Offset == offset ){
            if(!sortinsert(keyid,dataIndex)){
                inmdb_log(LOGCRITICAL, "sortinsert error, keyid %d", keyid);
                if(keyid > pTableDescriptor->numOfHashKey){
                    keyid --;
                    for(; keyid >= pTableDescriptor->numOfHashKey; keyid --){
                        sortdelete(keyid,dataIndex);
                    }
                }
                if(keyid > 0){
                    keyid --;
                    for(; keyid >= 0; keyid --){
                        hashdelete(keyid,dataIndex);
                    }
                }
                datadelete(dataIndex);
                pInmemDB->unLock(tableID);
                return 0;
            }
        }
    }
    pInmemDB->unLock(tableID);
    return 1;
}

/*********************************************
Function: operator()
parameter:
Description:
Returns:
Error Number:
*********************************************/
template <class T>
const T* CInmemoryTable<T>::operator()(int index){
    if(!ASSERTTABLEINDEX(index)){
        return (T*)NULL;
    }
    return (T*)(&pData[index]);
}

/*********************************************
Function: hashfunc
parameter:
Description:
Returns: index should >=0; -1:error
Error Number:
*********************************************/
template <class T>
size_t CInmemoryTable<T>::hashfunc(const char * keyStart){
    //if(!keyStart || keyStart[0] == 0){
    //    inmdb_log(LOGCRITICAL, ":hashfunc:key error.\n",0);
    //    return -1;
    //}
    size_t h;
    h = 0;
    for (int i = 0;keyStart[i]; i++){
        h = 37 * h +  keyStart[i];
    }
    return (h % (pTableDescriptor->hashPrimeNumber));
}


/*********************************************
Function: getHashPrimeNumber
parameter:
Description:
Returns: hashPrimeNumber
Error Number:
*********************************************/

template <class T>
int CInmemoryTable<T>::getHashPrimeNumber(int hashCapacity){
    int primeNumber[28] = {2,11,53,97,193,389,769,1543,3079,6151,12289,24593,49157,98317,
        196613,393241,786433,1572869,3145739,6291469,12582917,25165843,50331653,
        100663319,201326611,402653189,805306457,1610612741};
    for(int i = 27; i >= 0; i--){
        //if the hashCapacity is less than 11, then the function will return 0 as the hash
        //prime number, which will cause core dump when calling insert later.
        //for(int i = 27; i > 0; i--){
        if(hashCapacity >= primeNumber[i]){
            if(i == 27){
                return primeNumber[i];
            }
            return primeNumber[i + 1];
        }
    }
    return 0;
}


/*********************************************
Function: dataalloc : thread-safe
parameter:
Description:
Returns: 0 error;> 0 dataIndex
Error Number:
*********************************************/
template <class T>
int CInmemoryTable<T>::dataalloc(){
    //idataNext point to the first data in unused list after dataalloc
    int idataNext;
    int dataIndex;

    pInmemDB->lock(tableID);

    dataIndex = dataNext[UNUSEDHEAD];
    if(dataIndex == UNUSEDHEAD){
        inmdb_log(LOGCRITICAL, "Result:CInmemoryTable:dataalloc:table is full.\n",0);
        pInmemDB->unLock(tableID);
        return 0;
    }
    idataNext = dataNext[dataIndex];
    //the used link
    dataPre[dataNext[USEDHEAD]] = dataIndex;
    dataNext[dataIndex] = dataNext[USEDHEAD];
    dataPre[dataIndex] = USEDHEAD;
    dataNext[USEDHEAD] = dataIndex;

    //the null link
    dataPre[idataNext] = UNUSEDHEAD;
    dataNext[UNUSEDHEAD] = idataNext;
    pTableDescriptor->loadCount ++;
    timeStamp[dataIndex] = time(NULL);

    pInmemDB->unLock(tableID);
    return dataIndex;
}

/*********************************************
Function: dataallocc: This function is NOT thread-safe!
parameter:
Description:
Returns: 0: error, 1: success
Error Number:
*********************************************/
template <class T>
int CInmemoryTable<T>::dataallocc()
{
    //idataNext point to the first data in unused list after dataalloc
    int idataNext;
    int dataIndex;

    dataIndex = dataNext[UNUSEDHEAD];
    if(dataIndex == UNUSEDHEAD){
        inmdb_log(LOGCRITICAL, "Result:CInmemoryTable:dataalloc:table is full.\n",0);
        return 0;
    }
    idataNext = dataNext[dataIndex];
    //the used link
    dataPre[dataNext[USEDHEAD]] = dataIndex;
    dataNext[dataIndex] = dataNext[USEDHEAD];
    dataPre[dataIndex] = USEDHEAD;
    dataNext[USEDHEAD] = dataIndex;

    //the null link
    dataPre[idataNext] = UNUSEDHEAD;
    dataNext[UNUSEDHEAD] = idataNext;
    pTableDescriptor->loadCount ++;
    timeStamp[dataIndex] = time(NULL);

    return dataIndex;
}

/*********************************************
Function: deletexx: This function is NOT thread-safe!
parameter:
Description:
Returns: always return 1
Error Number:
*********************************************/
template <class T>
int CInmemoryTable<T>::deletexx(int index)
{
    //add by Scott, 20050630:if(ASSERTTABLEINDEX(index))
    if(ASSERTTABLEINDEX(index)) {
        datadelete(index);
    }
    return 1;
}

#endif // LIB_INMDB_TABLE_H__
