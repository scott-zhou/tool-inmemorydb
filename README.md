# Inmenory Database and Table

Shared Memory Object:
An object that represents memory that can be mapped concurrently into the address space of more than one process.
For details about shared memory please refer to the Base Definitions volume of IEEE Std 1003.1-2001, [Section 3.340, Shared Memory Object](http://pubs.opengroup.org/onlinepubs/009695299/basedefs/xbd_chap03.html#tag_03_340)

inmdb.h, inmdb.cc:
CInmemoryDB encapsulate the POSIX Shared Memory Object, create shared memory segment and semaphores on that.

inmdb_const_values.h:
All the const values and macros should be putted here.

inmdb_table_index.h:
Declare the TABLEINDEX class which will be used by inmdb_table.

inmdb_table.h:
Declare the template class CInmemoryTable.
Use CInmemoryTable to create table in shared memory segment.
The table can use hash key or sort key.
The table can working on thread-safe mode or not. But of course, it will have cost when working on thread-safe.

inmdb_log.h:
Which is a very simple implement for log function. You have to replace it with you own if you want to integrate my lib into you system.

Other test related files:
For test usage.

Created by [Scott Zhou](http://www.scottzhou.me)
