#include "recipe.h"
#include "cal.h"
#include "common.h"
#include "queue.h"
#include "sync_queue.h"
#include "decold.h"
#include "containerstore.h"

char *g1 = "g1", *g2 = "g2";
int target_group = 0;

int enable_migration = 1;
int enable_refs = 0;
int enable_topk = 0;
long int big_file = 0;
float migration_threshold = 0.8;

char g1_temp_path[128] = "/root/destor_test/g1/";
char g2_temp_path[128] = "/root/destor_test/g2/";

char g1_path[32] = "/root/destor_test/g1/";
char g2_path[32] = "/root/destor_test/g2/";

SyncQueue *write_g1_identified_file_temp_queue;
SyncQueue *write_g2_identified_file_temp_queue;

SyncQueue *write_g1_identified_file_to_destor_queue;
SyncQueue *write_g2_identified_file_to_destor_queue;

SyncQueue *remained_files_queue;


SyncQueue *write_g1_remained_files_queue;
SyncQueue *write_g2_remained_files_queue;


pthread_t tid1;
pthread_t tid3;

containerid container_count;

void free_chunk(struct chunk* ck) {
    if (ck->data) {
	free(ck->data);
	ck->data = NULL;
    }
    free(ck);
}

static int comp_fp_by_fid(const void *s1, const void *s2)
{
    return ((struct fp_info *)s1)->fid - ((struct fp_info *)s2)->fid;
}

static int64_t find_first_fp_by_fid(struct fp_info *fps, uint64_t fp_count, uint64_t fid, uint64_t left_start, uint64_t right_start)
{
    uint64_t middle = 0;
    uint64_t left = left_start, right = right_start;
    while (left <= right) {
	middle = (left + right) / 2;
	if (fps[middle].fid == fid)
	    break;
	else if (fps[middle].fid < fid)
	    left = middle + 1;
	else
	    right = middle - 1;
    }

    if (left > right) {
	return -1;
    }

    return middle -  fps[middle].order;
}

void push_identified_files(struct identified_file_info *identified_files, uint64_t identified_file_count, SyncQueue *queue) {
	int i = 0;
	for(i = 0; i < identified_file_count; i++) {
	    sync_queue_push(queue, identified_files + i);
	}
	sync_queue_term(queue);
}

void update_remained_files(int group, struct file_info *files, uint64_t file_count, struct fp_info *s1_ord, uint64_t s1_count, struct identified_file_info *identified_files, uint64_t identified_file_count, struct migrated_file_info *migrated_files, uint64_t migrated_file_count)
{
	uint64_t remained_file_count = 0;
	int low = 0, high = s1_count - 1;
	qsort(s1_ord, s1_count, sizeof(struct fp_info), comp_fp_by_fid);
	int left = 0, right = 0;
	int i = 0;
	for(i = 0; i < file_count; i++) {
	    uint64_t fid = files[i].fid; 
	    left = 0;
	    right = identified_file_count - 1;
	    
	    while(left <= right) {
		int middle = (left + right)/2;
		if (identified_files[middle].fid == fid) 
		    break;
		else if (identified_files[middle].fid < fid)
		    left = middle + 1;
		else if (identified_files[middle].fid > fid)
		    right = middle - 1; 
	    }

	    // the file don't need to remain
	    if (left <= right) {
		continue;
	    }

	    left = 0;
	    right = migrated_file_count - 1;
	    
	    while(left <= right) {
		int middle = (left + right)/2;
		if (migrated_files[middle].fid == fid) 
		    break;
		else if (migrated_files[middle].fid < fid) {
		    left = middle + 1;
		} else if (migrated_files[middle].fid > fid) {
		   right = middle - 1; 
		}
	    }

	    // the file don't need to remain
	    if (left <= right) {
		continue;
	    }
	
	    struct remained_file_info *one_file = (struct remained_file_info *)malloc(sizeof(struct remained_file_info));
	    one_file->fid = fid;
	    one_file->chunknum = files[i].chunknum;
	    one_file->fps = (fingerprint *)malloc(sizeof(fingerprint) * one_file->chunknum);
	    one_file->fps_cid = (uint64_t *)malloc(sizeof(uint64_t) * one_file->chunknum);

	    int start = find_first_fp_by_fid(s1_ord, s1_count, one_file->fid, low, high);	
	    uint32_t i = 0;
	    for (i = 0; i < one_file->chunknum; i++) {
		memcpy(one_file->fps[i], s1_ord[start + i].fp,  sizeof(fingerprint));	
		one_file->fps_cid[i] = s1_ord[start + i].cid;
	    } 

	    remained_file_count++;
	    sync_queue_push(remained_files_queue, one_file);
	     
	}

	printf("remained %lu files\n", remained_file_count);
	sync_queue_term(remained_files_queue);
}

void intersection(const char *path1, const char *path2)
{
	struct fp_info *s1, *s2;
	struct file_info *file1, *file2;
	int64_t s1_count = 0, s2_count = 0;
	int64_t file1_count = 0, file2_count = 0;
	int64_t empty1_count = 0, empty2_count = 0;
	int64_t i, j;

	read_recipe(path1, &s1, &s1_count, &file1, &file1_count, &empty1_count);
	read_recipe(path2, &s2, &s2_count, &file2, &file2_count, &empty2_count);

	struct fp_info *s1_ord = (struct fp_info *)malloc(s1_count * sizeof(struct fp_info));
	struct fp_info *s2_ord = (struct fp_info *)malloc(s2_count * sizeof(struct fp_info));
	for (i = 0; i < s1_count; i++)
	{
		s1_ord[i] = s1[i];
		memcpy(s1_ord[i].fp, s1[i].fp, sizeof(fingerprint));
		VERBOSE("s1_order   CHUNK:fid=[%8" PRId64 "], order=%" PRId64 ", size=%" PRId64 ", container_id=%" PRId64 "\n",s1_ord[i].fid, s1_ord[i].order, s1_ord[i].size, s1_ord[i].cid);
	}
	for (i = 0; i < s2_count; i++)
	{
		s2_ord[i] = s2[i];
		memcpy(s2_ord[i].fp, s2[i].fp, sizeof(fingerprint));
	}
	
	struct file_info *file1_ord = (struct file_info *)malloc(file1_count * sizeof(struct file_info));
	struct file_info *file2_ord = (struct file_info *)malloc(file2_count * sizeof(struct file_info));
	for (i = 0; i < file1_count; i++) {
	    file1_ord[i] = file1[i];
	}
	for (i = 0; i < file2_count; i++) {
	    file2_ord[i] = file2[i];
	}

	struct fp_info *scommon1, *scommon2;
	int64_t sc1_count = 0, sc2_count = 0;
	cal_inter(s1, s1_count, s2, s2_count, &scommon1, &sc1_count, &scommon2, &sc2_count);

	struct identified_file_info *identified_file1, *identified_file2;
	int64_t identified_file1_count = 0, identified_file2_count = 0;

	struct migrated_file_info *m1, *m2;
	int64_t m1_count = 0, m2_count = 0;

	int64_t mig1_count[8]={0,0,0,0,0,0,0,0}; //60,65,70,75,80,85,90,95%
	int64_t mig2_count[8]={0,0,0,0,0,0,0,0}; //60,65,70,75,80,85,90,95%

	file_find(file1, file1_count, scommon1, sc1_count, &identified_file1, &identified_file1_count, &m1, &m1_count, mig1_count);
	file_find(file2, file2_count, scommon2, sc2_count, &identified_file2, &identified_file2_count, &m2, &m2_count, mig2_count);

	printf("%s total file count:%ld fingerprint count:%ld identified file count:%ld similar file count:%ld\n", target_group?"g2":"g1", file1_count, sc1_count, identified_file1_count, m1_count);
	
	if (!target_group)
	    push_identified_files(identified_file1, identified_file1_count, write_g1_identified_file_temp_queue);
	else 
	    push_identified_files(identified_file1, identified_file1_count, write_g2_identified_file_temp_queue);

	if (!target_group)
	    init_container_store(g1);
	else
	    init_container_store(g2);
	update_remained_files(0, file1_ord, file1_count, s1_ord, s1_count, identified_file1, identified_file1_count, m1, m1_count);

	pthread_join(tid1, NULL);
	pthread_join(tid3, NULL);

	free(file1);
	free(file2);
	free(file1_ord);
	free(file2_ord);

	free(s1);
	free(s2);
	free(s1_ord);
	free(s2_ord);
	for (i = 0; i < identified_file1_count; i++) {
	    free(identified_file1[i].sizes);
	    free(identified_file1[i].fps);
	}
	for (i = 0; i < identified_file2_count; i++) {
	    free(identified_file2[i].sizes);
	    free(identified_file2[i].fps);
	}
	free(identified_file1);
	free(identified_file2);
	for (i = 0; i < m1_count; i++) {
	    free(m1[i].arr);
	    free(m1[i].fps);
	}
	for (i = 0; i < m2_count; i++) {
	    free(m2[i].arr);
	    free(m2[i].fps);
	}
	free(m1);
	free(m2);
	free(scommon1);
	free(scommon2);
}

void * read_from_destor_thread(void *arg)
{
    		
    return NULL;
}

void * write_identified_file_to_temp_thread(void *arg)
{

    char *group = arg;
    char temp_identified_file_path[128];
    SyncQueue *write_identified_file_temp_queue;
    if (!strcmp(group, "g1")) 
    {
	write_identified_file_temp_queue = write_g1_identified_file_temp_queue;
	sprintf(temp_identified_file_path, "%s/identified_file", g1_temp_path);
    }
    else
    {
	write_identified_file_temp_queue = write_g2_identified_file_temp_queue;
	sprintf(temp_identified_file_path, "%s/identified_file", g2_temp_path);
    }

    
    uint64_t identified_file_count = 0;
    struct identified_file_info *file;
    FILE *filep = NULL;
    while ((file = sync_queue_pop(write_identified_file_temp_queue))) {
	if (NULL == NULL) {
	    filep = fopen(temp_identified_file_path, "w+");
	    if (NULL == filep) {
		printf("fopen %s failed\n", temp_identified_file_path);
		break;
	    }

	    fwrite(&identified_file_count, sizeof(uint64_t), 1, filep);

	}
	fwrite(file, sizeof(struct identified_file_info), 1, filep); 
	uint64_t i = 0;
	for (i = 0; i < file->num; i++)
	    fwrite(file->fps[i], sizeof(fingerprint), 1, filep); 
	identified_file_count++;
    }

    fseek(filep, 0,SEEK_SET);
    fwrite(&identified_file_count, sizeof(uint64_t), 1, filep);

    if (NULL != filep)
	fclose(filep);
    		
    return NULL;
}

void restore_temp_thread(void *arg) {
    char *group = arg;
    char temp_identified_file_path[128];
    SyncQueue *write_identified_file_to_destor_queue;
    if (!strcmp(group, "g1")) 
    {
	sprintf(temp_identified_file_path, "%s/identified_file", g1_temp_path);
	write_identified_file_to_destor_queue = write_g1_identified_file_to_destor_queue;
    }
    else
    {
	sprintf(temp_identified_file_path, "%s/identified_file", g2_temp_path);
	write_identified_file_to_destor_queue = write_g2_identified_file_to_destor_queue;
    }
    FILE *filep = fopen(temp_identified_file_path, "r");
    if (NULL == filep)
	printf("fopen %s failed\n", temp_identified_file_path);
    
    uint64_t identified_file_count = 0;
    fread(&identified_file_count, sizeof(uint64_t), 1, filep);

    struct identified_file_info *identified_files = (struct identified_file_info *)malloc(identified_file_count * sizeof(struct identified_file_info));
    uint64_t i = 0;
    while(fread(identified_files + i, sizeof(struct identified_file_info), 1, filep)) {
	identified_files[i].fps = (fingerprint *)malloc(sizeof(fingerprint) * identified_files[i].num);		
	fingerprint temp_fp;
	uint64_t j = 0;
	while(j < identified_files[i].num) {
	    fread(temp_fp, sizeof(fingerprint), 1, filep);
	    memcpy(identified_files[i].fps[j], temp_fp, sizeof(fingerprint));
	    j++;
	}

	sync_queue_push(write_identified_file_to_destor_queue, identified_files+i);
	i++;
    }
    sync_queue_term(write_identified_file_to_destor_queue);
}

void write_identified_files_to_destor_thread(void *arg) {
    char *group = arg;
    SyncQueue *write_destor_queue;
    if (!strcmp(group, "g1")) 
    {
	write_destor_queue = write_g1_identified_file_to_destor_queue;
    }
    else
    {
	write_destor_queue = write_g2_identified_file_to_destor_queue;
    }

    struct identified_file_info *one_file;

    while ((one_file = sync_queue_pop(write_destor_queue))) {
		
    }

}

void *read_remained_files_data_thread(void *arg) {

    struct remained_file_info *one_file;
    char pool_path[128];
    char new_meta_path[128];
    char new_record_path[128];

    char *group = arg;
    if (!strcmp(group, "g1")) { 
	sprintf(pool_path, "%s/%s", g1_path, "container.pool");
	sprintf(new_meta_path, "%s/%s", g1_path, "new.meta");
	sprintf(new_record_path, "%s/%s", g1_path, "new.recipe");
    } else { 
	sprintf(pool_path, "%s/%s", g2_path, "container.pool");
	sprintf(new_meta_path, "%s/%s", g2_path, "new.meta");
	sprintf(new_record_path, "%s/%s", g2_path, "new.recipe");
    }

    FILE *new_metadata_fp = NULL;
    static int metabufsize = 64*1024;
    char *metabuf = malloc(metabufsize);
    int32_t metabufoff = 0;
    uint64_t recipe_offset = 0;
    int one_chunk_size = sizeof(fingerprint) + sizeof(containerid) + sizeof(int32_t);

    FILE *new_record_fp = NULL;
    static int recordbufsize = 64*1024;
    int32_t recordbufoff = 0;
    char *recordbuf = malloc(recordbufsize);
    recipe_offset = one_chunk_size;

    GHashTable *recently_unique_chunks = g_hash_table_new_full(g_int64_hash, g_fingerprint_equal, NULL, free_chunk);

    uint64_t containerid = 0;

    new_metadata_fp = fopen(new_meta_path, "w+");
    if (NULL == new_metadata_fp) {
	printf("fopen %s failed\n", new_meta_path);
    }
    new_record_fp = fopen(new_record_path, "w+");
    if (NULL == new_record_fp) {
	printf("fopen %s failed\n", new_record_path);
    }
    FILE *old_pool_fp = fopen(pool_path, "r");
    if (NULL == old_pool_fp) {
	printf("fopen %s failed\n", pool_path);
    }


    int32_t bv_num = 1;
    int deleted = 0;
    int64_t number_of_files = 0;
    int64_t number_of_chunks = 0;

    memcpy(metabuf + metabufoff, &bv_num, sizeof(bv_num));
    metabufoff += sizeof(bv_num);
    memcpy(metabuf + metabufoff, &deleted, sizeof(deleted));
    metabufoff += sizeof(deleted);
    memcpy(metabuf + metabufoff, &number_of_files, sizeof(number_of_files));
    metabufoff += sizeof(number_of_files);
    memcpy(metabuf + metabufoff, &number_of_chunks, sizeof(number_of_chunks));
    metabufoff += sizeof(number_of_chunks);

    char *data = NULL;
    while ((one_file = sync_queue_pop(remained_files_queue))) {

	number_of_files++;
	uint64_t i = 0;
	
        memcpy(metabuf + metabufoff, &(one_file->fid), sizeof(one_file->fid));
        metabufoff += sizeof(one_file->fid);
        memcpy(metabuf + metabufoff, &recipe_offset, sizeof(recipe_offset));
        metabufoff += sizeof(recipe_offset);

        memcpy(metabuf + metabufoff, &one_file->chunknum, sizeof(one_file->chunknum));
        metabufoff += sizeof(one_file->chunknum);
        memcpy(metabuf + metabufoff, &one_file->filesize, sizeof(one_file->filesize));
        metabufoff += sizeof(one_file->filesize);

	if (sizeof(one_file->fid) + sizeof(recipe_offset) + sizeof(one_file->chunknum) + sizeof(one_file->filesize) > metabufsize - metabufoff) {
	    fwrite(metabuf, metabufoff, 1, new_metadata_fp);
	    metabufoff = 0;
	}

	recipe_offset += (one_file->chunknum) * one_chunk_size;
	
	number_of_chunks += one_file->chunknum;
	int32_t chunk_size;
	for (i = 0; i < one_file->chunknum; i++) {
	    struct chunk* ruc = g_hash_table_lookup(recently_unique_chunks, &one_file->fps[i]);
	    if (NULL == ruc) {
		if (storage_buffer.container_buffer == NULL) {
		    storage_buffer.container_buffer = create_container();
		    storage_buffer.chunks = g_sequence_new(free_chunk);
		}
		chunk_size = retrieve_from_container(old_pool_fp, one_file->fps_cid[i], &data, one_file->fps[i]);

		if (container_overflow(storage_buffer.container_buffer, chunk_size))
		{
		    write_container_async(storage_buffer.container_buffer);    
		    storage_buffer.container_buffer = create_container();
		    storage_buffer.chunks = g_sequence_new(free_chunk);
		}

		ruc = (struct chunk *)malloc(sizeof(struct chunk));
		ruc->size = chunk_size;
		ruc->id = container_count; 
		ruc->data = data;
		memcpy(ruc->fp, &one_file->fps[i], sizeof(fingerprint));
		
		add_chunk_to_container(storage_buffer.container_buffer, ruc);

		g_hash_table_insert(recently_unique_chunks, &one_file->fps[i], ruc);
	    }
	    
	    chunk_size = ruc->size;

	    if(recordbufoff + sizeof(fingerprint) + sizeof(containerid) + sizeof(chunk_size) > recordbufsize) {
		fwrite(recordbuf, recordbufoff, 1, new_record_fp);
		recordbufoff = 0;
	    }		

	    struct fp_data * one_data = (struct fp_data *)malloc(sizeof(struct fp_data));			
	    one_data->data = data;	
	    memcpy(recordbuf + recordbufoff, one_file->fps[i], sizeof(fingerprint)); 
	    recordbufoff += sizeof(fingerprint);
	    memcpy(recordbuf + recordbufoff, &container_count, sizeof(containerid)); 
	    recordbufoff += sizeof(containerid);
	    memcpy(recordbuf + recordbufoff, &chunk_size, sizeof(chunk_size)); 
	    recordbufoff += sizeof(chunk_size);
	}
    }

    printf("%s remained %lu files\n", group, number_of_files);
    display_hash_table(recently_unique_chunks);
    
    write_container_async(storage_buffer.container_buffer);    
    close_container_store();

    if( recordbufoff ) {
	fwrite(recordbuf, recordbufoff, 1, new_record_fp);
    	recordbufoff = 0;
    }
    if( metabufoff ) {
        fwrite(metabuf, metabufoff, 1, new_metadata_fp);
        metabufoff = 0;
    }

    fseek(new_metadata_fp, 0, SEEK_SET);
    fwrite(&bv_num, sizeof(bv_num), 1, new_metadata_fp);
    fwrite(&deleted, sizeof(deleted), 1, new_metadata_fp);
    fwrite(&number_of_files, sizeof(number_of_files), 1, new_metadata_fp);
    fwrite(&number_of_chunks, sizeof(number_of_chunks), 1, new_metadata_fp);

    fclose(old_pool_fp);
    fclose(new_metadata_fp);
    fclose(new_record_fp);

    storage_hash_table(recently_unique_chunks);
    g_hash_table_destroy(recently_unique_chunks);

    free(recordbuf);
    free(metabuf);


    return NULL;
}

int main(int argc, char *argv[])
{

//	if (3 != argc) {
//		printf("usage: ./decold g1 g2\n");
//		return -1;
//	}
    
//	strcpy(g1, argv[1]);
//	strcpy(g2, argv[2]);
//	printf("g1=%s g2=%s\n", g1, g2);	
	
	/*
	// handle g1
	target_group = 0;

	write_g1_identified_file_temp_queue = sync_queue_new(100);	
	pthread_create(&tid1, NULL, write_identified_file_to_temp_thread, g1);

	remained_files_queue = sync_queue_new(100);
	pthread_create(&tid3, NULL, read_remained_files_data_thread, g1);
    
	intersection(g1_path, g2_path);
	*/


	target_group = 1;
	write_g2_identified_file_temp_queue = sync_queue_new(100);	
	pthread_create(&tid1, NULL, write_identified_file_to_temp_thread, g2);

	remained_files_queue = sync_queue_new(100);
	pthread_create(&tid3, NULL, read_remained_files_data_thread, g1);
    
	intersection(g2_path, g1_path);
	
	return 0;
}
