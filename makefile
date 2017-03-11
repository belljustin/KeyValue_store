default: main.c kv_store.o
	gcc -o main.out main.c kv_store.o -g -lrt -lpthread

kv_store.o: kv_store.c kv_store.h
	gcc -c kv_store.c -g -lrt

clean:
	rm *.o main.out
