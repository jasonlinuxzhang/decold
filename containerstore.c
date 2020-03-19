#include "containerstore.h"
#include "common.h"
#include "queue.h"
#include "sync_queue.h"

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

