all:inmdb.o \
	test \
	test_lock \
	test_create_db \
	test_destroy_db \
	test_create_table \
	test_insert_record

inmdb.o:inmdb.cc
	g++ -Wall -c -O2 -Wall inmdb.cc  
	mv inmdb.o libs/.

test:test.cc inmdb.cc
	g++ -Wall test.cc -g  -O2 -o test libs/inmdb.o 
	mv test bin/.
test_lock:test_lock.cc inmdb.cc
	g++ -Wall test_lock.cc -g  -o test_lock libs/inmdb.o
	mv test_lock bin/.
test_create_db:test_create_db.cc inmdb.cc
	g++ -Wall test_create_db.cc -g  -o create_db libs/inmdb.o
	mv create_db testbin/.
test_destroy_db:test_destroy_db.cc inmdb.cc
	g++ -Wall test_destroy_db.cc -g  -o destroy_db libs/inmdb.o
	mv destroy_db testbin/.
test_create_table:test_create_table.cc inmdb.cc
	g++ -Wall test_create_table.cc -g  -o create_table libs/inmdb.o
	mv create_table testbin/.
test_insert_record:test_insert_record.cc inmdb.cc
	g++ -Wall test_insert_record.cc -g  -o insert_record libs/inmdb.o
	mv insert_record testbin/.
