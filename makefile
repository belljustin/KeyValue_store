default: main.c kv_store.o
	gcc -o main.out main.c kv_store.o -g -lrt -lpthread

test1: test1.c test.h kv_store.o
	gcc -o test1.out test1.c kv_store.o -g -lrt -lpthread

test2: test2.c test.h kv_store.o
	gcc -o test2.out test2.c kv_store.o -g -lrt -lpthread

kv_store.o: kv_store.c kv_store.h
	gcc -c kv_store.c -g -lrt

clean:
	rm -f *.o main.out test1.out test2.out /dev/shm/*
	pkill -f test2.out
