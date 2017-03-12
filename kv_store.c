/********************************************************************************
 * KV_STORE                                                                     *
 *                                                                              *
 * Justin Bell                                                                  *
 * 260561261                                                                    *
 *                                                                              *
 * An API for a Key Value store that uses a hash table to store data in a shared*
 * memory object.                                                               *
 *                                                                              *
 * It uses semaphores to protect the database against writes on pod that are    *
 * currently being read, yet allows multiple readers to read a pod at the same  *
 * time.                                                                        *
 *******************************************************************************/

#include <sys/mman.h>
#include <sys/stat.h>
#include <limits.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <fcntl.h>

#include "kv_store.h"


/********************************************************************************
 * Constructors for the KV store structs                                        *
 *                                                                              *
 * Refer to the header for the struct definitions                               *
 *                                                                              *
 *******************************************************************************/

index_pair *newIndexPair() {
    index_pair *pair = malloc(sizeof(index_pair));
    pair->key = malloc(KEY_LEN * sizeof(char));
    memset(pair->key, '\0', KEY_LEN);

    return pair;
}


void delIndexPair(index_pair *pair) {
    free(pair->key);
    free(pair);
}


index_pair ***newIndex() {
    index_pair ***index = calloc(NUM_PODS, sizeof(void *));
    for (int i=0; i<NUM_PODS; i++) {
        index[i] = calloc(POD_DEPTH, sizeof(void *));
        for (int j=0; j<POD_DEPTH; j++) {
            index[i][j] = newIndexPair();
        }
    }

    return index;
}


void delIndex(index_pair ***index) {
    for (int i=0; i<NUM_PODS; i++) {
        for (int j=0; j<POD_DEPTH; j++) {
            delIndexPair(index[i][j]);
        }
        free(index[i]);
    }
    free(index);
}


kv_pair *newKVpair(void *addr, int empty) {
    kv_pair *pair = malloc(sizeof(kv_pair));

    pair->key = addr;
    addr += KEY_LEN * sizeof(char);
    if (empty == 1)
        memset(pair->key, '\0', KEY_LEN);

    pair->value = addr;
    if (empty == 1)
        memset(pair->value, '\0', VALUE_LEN);

    return pair;
}


void delKVpair(kv_pair *pair) {
    free(pair);
}


kv_pod *newKVpod(void *addr, int empty) {
    kv_pod *pod = malloc(sizeof(kv_pod));
    pod->write_counter = addr;
    if (empty == 1)
        *(pod->write_counter) = 0;
    addr += sizeof(int);

    pod->num_readers = addr;
    if (empty == 1)
        *(pod->num_readers) = 0;
    addr += sizeof(int);

    pod->OKtoRead = addr;
    if (empty == 1)
        sem_init(pod->OKtoRead, 1, 1);
    addr += sizeof(sem_t);

    pod->OKtoWrite = addr;
    if (empty == 1)
	    sem_init(pod->OKtoWrite, 1, 1);
    addr += sizeof(sem_t);

    pod->kv_pairs = calloc(POD_DEPTH, sizeof(void *));
    for (int i=0; i<POD_DEPTH; i++) {
        pod->kv_pairs[i] = newKVpair(addr, empty);
        addr += KV_SIZE;
    }

    return pod;
}


void delKVpod(kv_pod *pod) {
    for (int i=0; i<POD_DEPTH; i++) {
        delKVpair(pod->kv_pairs[i]);
    }
    sem_destroy(pod->OKtoRead);
    sem_destroy(pod->OKtoWrite);
    free(pod->kv_pairs);
    free(pod);
}


kv_store *newStore(char *name, void *addr, int empty) {
    kv_store *store = malloc(sizeof(kv_store));
    store->name = malloc(NAME_SIZE * sizeof(char));
    
    memset(store->name, '\0', NAME_SIZE);
    strncpy(store->name, name, strlen(name));

    store->kv_pods = calloc(NUM_PODS, sizeof(void *));
    for (int i=0; i<NUM_PODS; i++) {
        store->kv_pods[i] = newKVpod(addr, empty);
        addr += POD_SIZE;
    }

    store->index = newIndex();

    return store;
}


void delStore(kv_store *store) {
    free(store->name);
    for (int i=0; i<NUM_PODS; i++) {
        delKVpod(store->kv_pods[i]);
    }
    free(store->kv_pods);
    delIndex(store->index);
    free(store);
}


/********************************************************************************
 * Helpers functions                                                            *
 *                                                                              *
 * Miscellaneous functions for helping with the store                           *
 *                                                                              *
 *******************************************************************************/


/* 
 * Returns a hash for a given key. This serves as the index for the key's pod 
 *
 * @param key The key to be hashed
 * @returns The hash of the key
 */
int hash(char *key) {
    int h = 7;
    const int key_len = strlen(key);
    for (int i=0; i<key_len; i++) {
        h = (h*31 + key[i]) % NUM_PODS;
    }
    return h;
}


/*
 * Used to clean the index when a kv pair is evicted from a pod. If no other kv 
 * pair exists in the pod with the same key, that entry is wiped from the index.
 * Otherwise nothing is done.
 *
 * @param pod The pod from which the kv pair has been evicted
 * @param ipod The index pod related to pod
 * @param key The key of the evicted kv pair
 */
void clean_index(kv_pod *pod, index_pair **ipod, char *key) {
    for (int i=0; i<POD_DEPTH; i++) {
        if (strncmp(pod->kv_pairs[i]->key, key, KEY_LEN) == 0)
            return;
    }
    for (int i=0; i<POD_DEPTH; i++) {
        if (strncmp(ipod[i]->key, key, KEY_LEN) == 0) {
            memset(ipod[i]->key, '\0', KEY_LEN);
            return;
        }
    }
}


/*
 * Adds a key and its equivalent index to the index pod. If the key does not
 * exist, it is added. Otherwise, nothing is done.
 *
 * @param ipod The index pod to update
 * @param key The key to add
 * @param index The index of the newly added key
 */
void add_index(index_pair **ipod, char *key, int index) {
    int free_space = -1;
    for (int i=0; i<POD_DEPTH; i++) {
        if (ipod[i]->key[0] == '\0') {
            free_space = i;     
        }
        if (strncmp(ipod[i]->key, key, KEY_LEN) == 0)
            return;
    }
    if (free_space == -1)
        perror("Index has not been cleaned");
    strncpy(ipod[free_space]->key, key, KEY_LEN);
    ipod[free_space]->index = index;
}


/*
 * Retrieves the index of the key from the pod. If no key is found, 0 is
 * returned.
 *
 * @param ipod The index pod to be searched
 * @param key The key to be searched for
 * @return The index of the key if it is found. 0 otherwise.
 */
int get_index(index_pair **ipod, char *key) {
    for (int i=0; i<POD_DEPTH; i++) {
        if (strncmp(ipod[i]->key, key, KEY_LEN) == 0) {
            return ipod[i]->index;
        }
    }
    return 0;
}


/*
 * Updates the key's index
 *
 * If the key is found in the index pod, it's index is changed to the new one.
 * Otherwise, we add the key to the index pod and set its index.
 *
 * @param ipod The index pod to update
 * @param key The key to update
 * @param index The new index
 */
void update_index(index_pair **ipod, char *key, int index) {
    for (int i=0; i<POD_DEPTH; i++) {
        if (strncmp(ipod[i]->key, key, KEY_LEN) == 0) {
            ipod[i]->index = index;
            return;
        }
    }
    add_index(ipod, key, index);
}


/********************************************************************************
 * Read/Write Locks                                                             *
 *                                                                              *
 * The functions required for managing the read and write semaphores            *
 *                                                                              *
 *******************************************************************************/

/*
 * Acquires the write lock
 *
 * @param pod The key-value pod to get a write lock for
 */
void acquire_wlock(kv_pod *pod) {
    sem_wait(pod->OKtoWrite);
}


/*
 * Releases the write lock
 *
 * @param pod The key-value pod to release the lock on
 */
void release_wlock(kv_pod *pod) {
    sem_post(pod->OKtoWrite);
}


/*
 * Acquires the read lock
 *
 * If this is the only current reader to ask for a lock, then the write lock will
 * also be acquired to prevent writing while others are reading
 *
 * @param pod The key-value pod to get a read lock for
 */
void acquire_rlock(kv_pod *pod) {
    sem_wait(pod->OKtoRead);
    if ((*pod->num_readers)++ == 0)
        sem_wait(pod->OKtoWrite);
    sem_post(pod->OKtoRead);
}


/*
 * Releases the read lock
 *
 * If this is the last current reader to release the read lock, then the write
 * lock will also be released
 *
 * @param pod The key-value pod to release the lock on
 */
void release_rlock(kv_pod *pod) {
    sem_wait(pod->OKtoRead);
    if (--(*pod->num_readers) == 0)
        sem_post(pod->OKtoWrite);
    sem_post(pod->OKtoRead);
}


/********************************************************************************
 * Key-Value Store API                                                          *
 *                                                                              *
 * Implements the functionality specified for the assignment. These functions   *
 * use a pass-by reference design to avoid the use of a global variable.        *
 * However, wrapper functions are provided below to implement the API specified *
 * in "2. Suggested Interface"                                                  *
 *                                                                              *
 *******************************************************************************/


/*
 * Creates a store. A successful call will cause the provided kv_store pointer  
 * to point to: 
 *  1) a newly created store with the provided name, or
 *  2) a previously created store if the name already exists
 * The function may fail due to insufficient user permissions or memory. On
 * failure, store will be a NULL pointer.
 *
 * @param name The name of the Key-Value Store
 * @param store The pointer for the store
 */
kv_store *_kv_store_create(char *name, kv_store *store) {
    int new_store = 1;
    int fd = shm_open(name, O_RDWR|O_CREAT|O_EXCL, S_IRWXU);
    if (fd == -1) {
        new_store = 0;
        fd = shm_open(name, O_RDWR|O_CREAT, S_IRWXU);
        if (fd == -1)
            return NULL;
    }

    ftruncate(fd, STORE_SIZE);
    void *addr = mmap(NULL, STORE_SIZE, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
    if (addr == MAP_FAILED)
        return NULL;

    store = newStore(name, addr, new_store);
    
    close(fd);

    return store;
}


/*
 * Deletes the shared memory object and cleans up all the memory allocated for
 * the store
 *
 * @param store The store to delete
 */
void _kv_delete_db(kv_store *store) {
    munmap(store->kv_pods[0]->write_counter, STORE_SIZE);
    shm_unlink(store->name);
    delStore(store);
}


/*
 * Writes a key value pair to the store.
 *
 * If there is no space left in the pod to which the key is hashed, the earliest
 * inserted key-value pair will be overwritten by the new pair.
 *
 * On key removal, the index will also be cleaned from the index to make sure
 * there's room for the new index. See clean_index()
 *
 * @param store The store to write to
 * @param key The key to write with
 * @param value The value to write
 */
int _kv_store_write(kv_store *store, char *key, char *value) {
    int h = hash(key);
    kv_pod *pod = store->kv_pods[h];

    acquire_wlock(pod);

    int write_counter = *(pod->write_counter);
    *(pod->write_counter) = (*(pod->write_counter) + 1) % POD_DEPTH;

    kv_pair *pair = pod->kv_pairs[write_counter];
    char *removed_key = malloc(KEY_LEN * sizeof(char));
    strncpy(removed_key, pair->key, KEY_LEN);

    strncpy(pair->key, key, KEY_LEN);
    strncpy(pair->value, value, VALUE_LEN);

    clean_index(pod, store->index[h], removed_key);
    free(removed_key);
    add_index(store->index[h], key, write_counter);

    release_wlock(pod);
}


/*
 * Reads the value associated with the provided key from the store. Each read
 * updates the index such that subsequent reads provide the next value. If the
 * key is not found, a NULL value is returned.
 *
 * This function returns a pointer to a newly allocated string. It is the
 * responsibility of the calling process to free the associated memory.
 *
 * The index is maintained on the heap so read orders are process independent.
 *
 * @param store The store to read from
 * @param key The key to be used in the lookup
 * @return A pointer a duplicate string representing the value in the store
 */
char *_kv_store_read(kv_store *store, char *key) {
    int h = hash(key);
    kv_pod *pod = store->kv_pods[h];

    acquire_rlock(pod);

    index_pair **ipod = store->index[h];
    int index = get_index(ipod, key); 
    
    char *value = NULL;
    int i=0;
    for (;i<POD_DEPTH; i++) {
        int offset = (index + i) % POD_DEPTH;
        if (strncmp(pod->kv_pairs[offset]->key, key, KEY_LEN) == 0) {
            value = malloc(VALUE_LEN * sizeof(char));
            strncpy(value, pod->kv_pairs[offset]->value, VALUE_LEN);
            i++;
            break;
        }
    }
    for (;i<POD_DEPTH; i++) {
        int offset = (index + i) % POD_DEPTH;
        if (strncmp(pod->kv_pairs[offset]->key, key, KEY_LEN) == 0) {
            update_index(ipod, key, offset);
            break;
        }
    }
    release_rlock(pod);

    return value;
}


/*
 * Reads all values associated with the provided key from the store.
 *
 * This function returns an array of pointers to a newly allocated strings. It is
 * the responsibility of the calling process to free the associated memory.
 *
 * @param store The store to read from
 * @param key The key to be used in the lookup
 * @return  A pointer to an array of  duplicate strings representing the values
 *          in the store
 */
char **_kv_store_read_all(kv_store *store, char *key) {
    int h = hash(key);
    kv_pod *pod = store->kv_pods[h];
    acquire_rlock(pod);

    int start = get_index(store->index[h], key);

    char **values = NULL;
    char *value = _kv_store_read(store, key);
    if (value == NULL) {
        release_rlock(pod);
        return NULL;
    } 
    values = calloc(POD_DEPTH, sizeof(char *));

    int i=0;
    values[i++] = value;
    while (get_index(store->index[h], key) != start) {
        values[i++] = _kv_store_read(store, key);
    }
    while (i<POD_DEPTH) {
        values[i++] = NULL;
    }
    release_rlock(pod);
    return values;
}


/********************************************************************************
 * Wrappers for the Key-Value Store API                                         *
 *                                                                              *
 * Provides wrappers for the Key-Value Store so they adhere to the specs in "2. *
 * Suggested Implementation". Please refer to the mini_pa2 doc for further      *
 * documentation.                                                               *
 *                                                                              *
 *******************************************************************************/

kv_store *global_store;

int kv_store_create(char *name) {
    global_store = _kv_store_create(name, global_store);
    if (global_store == NULL)
        return -1;
    return 0;
}


int kv_store_write(char *key, char *value) {
    _kv_store_write(global_store, key, value);
    return 0;
}


char *kv_store_read(char *key) {
    return _kv_store_read(global_store, key);
}


char **kv_store_read_all(char *key) {
    return _kv_store_read_all(global_store, key);
}


void kv_delete_db() {
    _kv_delete_db(global_store);
}
