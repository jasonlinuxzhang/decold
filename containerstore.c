#include "containerstore.h"
#include "common.h"
#include "queue.h"
#include "sync_queue.h"

static SyncQueue* container_buffer;
static FILE *fp;
static pthread_t append_t;
containerid container_count  = 0;

gboolean g_fingerprint_equal(fingerprint* fp1, fingerprint* fp2) {
    return !memcmp(fp1, fp2, sizeof(fingerprint));
}

gint g_fingerprint_cmp(fingerprint* fp1, fingerprint* fp2, gpointer user_data) {
    return memcmp(fp1, fp2, sizeof(fingerprint));
}

static void init_container_meta(struct containerMeta *meta) {
    meta->chunk_num = 0;
    meta->data_size = 0;
    meta->id = -1;
    meta->map = g_hash_table_new_full(g_int_hash, g_fingerprint_equal, NULL,
	    free);
}

struct container* create_container() {
    struct container *c = (struct container*) malloc(sizeof(struct container));
    c->data = calloc(1, CONTAINER_SIZE);

    init_container_meta(&c->meta);
    c->meta.id = container_count++;
    return c;
}



int container_overflow(struct container* c, int32_t size) {
    if (c->meta.data_size + size > CONTAINER_SIZE - CONTAINER_META_SIZE)
	return 1;
    /*
 *   * 28 is the size of metaEntry.
 *	 */
    if ((c->meta.chunk_num + 1) * 28 + 16 > CONTAINER_META_SIZE)
	return 1;
    return 0;
}

/*
 *  * For backup.
 *   * return 1 indicates success.
 *    * return 0 indicates fail.
 *     */
int add_chunk_to_container(struct container* c, struct chunk* ck) {
    assert(!container_overflow(c, ck->size));
    if (g_hash_table_contains(c->meta.map, &ck->fp)) {
	NOTICE("Writing a chunk already in the container buffer!");
	ck->id = c->meta.id;
	return 0;
    }

    struct metaEntry* me = (struct metaEntry*) malloc(sizeof(struct metaEntry));
    memcpy(&me->fp, &ck->fp, sizeof(fingerprint));
    me->len = ck->size;
    me->off = c->meta.data_size;

    g_hash_table_insert(c->meta.map, &me->fp, me);
    c->meta.chunk_num++;

    if (destor.simulation_level < SIMULATION_APPEND)
	memcpy(c->data + c->meta.data_size, ck->data, ck->size);

    c->meta.data_size += ck->size;

    ck->id = c->meta.id;

    return 1;
}

void free_container_meta(struct containerMeta* cm) {
    g_hash_table_destroy(cm->map);
    free(cm);
}

void free_container(struct container* c) {
    g_hash_table_destroy(c->meta.map);
    if (c->data)
	free(c->data);
    free(c);
}

int container_empty(struct container* c) {
    return c->meta.chunk_num == 0 ? 1 : 0;
}

void write_container_async(struct container* c) {
    assert(c->meta.chunk_num == g_hash_table_size(c->meta.map));
    if (container_empty(c)) {
	container_count--;
	VERBOSE("Append phase: Deny writing an empty container %lld",
		c->meta.id);
	return;
    }

    sync_queue_push(container_buffer, c);
}

void write_container(struct container* c) {

    assert(c->meta.chunk_num == g_hash_table_size(c->meta.map));

    if (container_empty(c)) {
	/* An empty container
 *	 * It possibly occurs in the end of backup */
	container_count--;
	VERBOSE("Append phase: Deny writing an empty container %lld",
		c->meta.id);
	return;
    }

    VERBOSE("Append phase: Writing container %lld of %d chunks", c->meta.id,
	    c->meta.chunk_num);

	unsigned char * cur = &c->data[CONTAINER_SIZE - CONTAINER_META_SIZE];
	ser_declare;
	ser_begin(cur, CONTAINER_META_SIZE);
	ser_int64(c->meta.id);
	ser_int32(c->meta.chunk_num);
	ser_int32(c->meta.data_size);

	GHashTableIter iter;
	gpointer key, value;
	g_hash_table_iter_init(&iter, c->meta.map);
	while (g_hash_table_iter_next(&iter, &key, &value)) {
	    struct metaEntry *me = (struct metaEntry *) value;
	    ser_bytes(&me->fp, sizeof(fingerprint));
	    ser_bytes(&me->len, sizeof(int32_t));
	    ser_bytes(&me->off, sizeof(int32_t));
	}

	ser_end(cur, CONTAINER_META_SIZE);


	if (fseek(fp, c->meta.id * CONTAINER_SIZE + 8, SEEK_SET) != 0) {
	    perror("Fail seek in container store.");
	    exit(1);
	}
	if(fwrite(c->data, CONTAINER_SIZE, 1, fp) != 1){
	    perror("Fail to write a container in container store.");
	    exit(1);
	}

}

static void* append_thread(void *arg) {

    while (1) {
	struct container *c = sync_queue_get_top(container_buffer);
	if (c == NULL)
	    break;

	write_container(c);

	sync_queue_pop(container_buffer);

    }

    return NULL;
}

void init_container_store(char *pool_path) {

    if ((fp = fopen(pool_path, "r+"))) {
	fread(&container_count, 8, 1, fp);
    } else if (!(fp = fopen(pool_path, "w+"))) {
	perror(
		"Can not create container.pool for read and write because");
	exit(1);
    }

    container_buffer = sync_queue_new(25);

    pthread_create(&append_t, NULL, append_thread, NULL);

}
