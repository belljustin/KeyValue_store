#include <semaphore.h>

#define NAME_SIZE 256
#define NUM_PODS 256
#define POD_DEPTH 256
#define KEY_LEN 32
#define VALUE_LEN 256

// TODO: make these consts so we don't have to recompute everytime vars are used
#define KV_SIZE (KEY_LEN * sizeof(char) + VALUE_LEN * sizeof(char))
#define POD_SIZE (POD_DEPTH * KV_SIZE + 2*sizeof(int) + 2*sizeof(sem_t))
#define STORE_SIZE (NUM_PODS * POD_SIZE)

typedef struct {
    char *key;
    int index;
} index_pair;

typedef struct {
    char *key;
    char *value;
} kv_pair;

typedef struct {
    int *write_counter;
    int *num_readers;
    sem_t *OKtoRead;
    sem_t *OKtoWrite;
    kv_pair **kv_pairs;
} kv_pod;

typedef struct {
    char *name;
    kv_pod **kv_pods;
    index_pair ***index;
} kv_store;


int kv_store_create(char *name);
int kv_store_write(char *key, char *value);
char *kv_store_read(char *key);
char **kv_store_read_all(char *key);
void kv_delete_db();
