read_eval: read_eval.c kv_store.o
	gcc -o kvshell read_eval.c kv_store.o -g -lrt -lpthread

kv_store.o: ../kv_store.c ../kv_store.h
	gcc -c ../kv_store.c -g -lrt

clean:
	rm -f *.o main.out test1.out
