#ifndef LIB_INMDB_TABLE_INDEX_H
#define LIB_INMDB_TABLE_INDEX_H

#include "inmdb_const_values.h"

class TABLEINDEX{
    private:
        int dataIndex;
        int internalIndex;
        template<typename T> friend class CInmemoryTable;
    public:
        TABLEINDEX(int index,int internal){
            dataIndex = index;
            internalIndex = internal;
        }
        TABLEINDEX(int index){
            dataIndex = index;
            internalIndex = 0;
        }

        TABLEINDEX(){
            dataIndex = USEDHEAD;
        }

        ~TABLEINDEX() {}

        int operator == (TABLEINDEX& tableindex) {
            return (dataIndex == tableindex.dataIndex);
        }

        TABLEINDEX& operator = (TABLEINDEX tableindex) {
            dataIndex = tableindex.dataIndex;
            internalIndex = tableindex.internalIndex;
            return (*this);
        }

        int operator != (TABLEINDEX& tableindex) {
            return (dataIndex != tableindex.dataIndex);
        }

        operator int() const{
            return dataIndex;
        }

        int getInternalIndex(){
            return internalIndex;
        }
};

#endif // LIB_INMDB_TABLE_INDEX_H
