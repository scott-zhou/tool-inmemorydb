#ifndef LIB_INMDB_CONST_VALUES_H
#define LIB_INMDB_CONST_VALUES_H

#include <stdint.h>

#define MAXNUMOFHASHKEY    8
#define MAXNUMOFSORTKEY    8

#define MAXKEYFORMAT       32 //max length of the keyformat
#define MAXSTROFKEY        256 //max lenth of key string

#define MAXFIELDSTRING    128 // max lenth fo the field as key in the struct

//define for canculate the field offset from beginning of the struct and the filed length.
#define OFFSET(s, m) ((int)((intptr_t)&(((s*)NULL)->m)))
#define SIZEOF(s, m) sizeof(((s*)NULL)->m)

//head of the datalist
enum HeadType {
    USEDHEAD   = 0,
    UNUSEDHEAD = 1
};

#define LISTHEADTABLEINDEX TABLEINDEX(USEDHEAD)

enum SEARCHMETHOD{
    HASHSEARCH = 0,
    SORTSEARCH,
};

enum KEYMETHOD{
    STRKEYFROMSTRPLUSINT = 0,//(str+int)
    STRKEYFROMINTPLUSSTR,//(int+str)
    STRKEYFROMSTRPLUSSTR,//(str+str)
    STRKEYFROMINTPLUSINT,//(int+int)
    STRKEYFROMONLYSTR,//(str)
    INTKEYFROMONLYINT,//(int)
    STRKEYFROMONLYINT//(int)
};


#endif // LIB_INMDB_CONST_VALUES_H
