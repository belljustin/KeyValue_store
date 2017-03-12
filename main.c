#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <stdio.h>

#include "kv_store.h"


void massert (int assertion, char *msg) {
    if (!assertion)
        printf("\e[1;32mSuccess:\e[0m %s\n", msg);
    else
        printf("\e[1;31mError:\e[0m %s\n", msg);
}

void sig_handler(int signo)
{
    if (signo == SIGINT) {
        kv_delete_db();
        printf("SIGINT\n");
        exit(-1);
    }
    kv_delete_db();
    perror("Error");
}


int main(int argc, char *argv[]) {
    massert(kv_store_create("/jbell30") == -1, "Store creation");

    char *k1 = "foo";
    char *v1 = "bar";
    char *result = kv_store_read(k1);
    massert((result != NULL), "Read on non-existant key");
    free(result);

    kv_store_write(k1, v1);
    result = kv_store_read(k1);
    massert(strncmp(result, v1, VALUE_LEN), "Simple write-read");
    free(result);

    char *k2 = "Bob Dylan";
    char *vs[3] = {"Boots of Spanish Leather", "It Ain't Me Babe",
                 "Don't Think Twice It's Alright"};
    kv_store_write(k2, vs[0]);
    kv_store_write(k2, vs[1]);
    kv_store_write(k2, vs[2]);
    char **values = kv_store_read_all(k2);
    int check = 0;
    for (int i=0; i<3; i++) {
        if((check = strncmp(values[i], vs[i], VALUE_LEN)) != 0) {
            free(values[i]);
            break;
        }
        free(values[i]);
    }
    free(values);
    massert(check, "Read all");

    kv_delete_db();
    return 0;
}
