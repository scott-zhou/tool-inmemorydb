# Inmenory Database and Table

## First of all
The function is basicly finished and when I plan to write a better document here, I realized that the interface is not easy to use and the whole function that I want to implement is not easy to understand.
Basicly, in this project, I create a peace of memory in POSIX standard shared memory, and on that shared memory, create several tables. The table element can any datastruct that definded by user (well, there is a limitation that the element must be a continuously memory and there is no point in the element), and user can create several keys for search. The Key can be hash key or sort key (sort key means the elelment will be sorted by the key on insert).
But now, I realized that maybe the interface is not user friendly. Why I want to create several table on same peace of shared memory? Why I use semaphores like now? I make it too complicated which cause that difficult for me to describe it clearly in document about all the interfaces, and difficult for users understand what I'm doing.
I decidec to not continue coding on this project, and create another more simple lib which will provide more clear and simple interface. I will name it lib-shm-table. And in the new lib I will aiming to create interface NOT only on C++.

## Shared Memory Object:
An object that represents memory that can be mapped concurrently into the address space of more than one process.
For details about shared memory please refer to the Base Definitions volume of IEEE Std 1003.1-2001, [Section 3.340, Shared Memory Object](http://pubs.opengroup.org/onlinepubs/009695299/basedefs/xbd_chap03.html#tag_03_340)

## inmdb.h, inmdb.cc:
CInmemoryDB encapsulate the POSIX Shared Memory Object, create shared memory segment and semaphores on that.

## inmdb_const_values.h:
All the const values and macros should be putted here.

## inmdb_table_index.h:
Declare the TABLEINDEX class which will be used by inmdb_table.

## inmdb_table.h:
Declare the template class CInmemoryTable.
Use CInmemoryTable to create table in shared memory segment.
The table can use hash key or sort key.
The table can working on thread-safe mode or not. But of course, it will have cost when working on thread-safe.

## inmdb_log.h:
Which is a very simple implement for log function. You have to replace it with you own if you want to integrate my lib into you system.

## Other test related files:
For test usage.

Created by [Scott Zhou](http://www.scottzhou.me)
