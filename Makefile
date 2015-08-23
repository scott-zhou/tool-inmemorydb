all:inmdb.o test test_lock 

inmdb.o:inmdb.cc
	g++ -c -O2 -Wall inmdb.cc  
	mv inmdb.o libs/.

test:test.cc inmdb.cc
	g++ -I. -lpthread -Wall test.cc -g  -O2 -o test libs/inmdb.o 
	mv test bin/.
test_lock:test_lock.cc inmdb.cc
	g++ -I. test_lock.cc -g  -o test_lock libs/inmdb.o
	mv test_lock bin/.
