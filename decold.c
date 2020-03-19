#include "recipe.h"
#include "cal.h"
#include "common.h"
#include "queue.h"
#include "sync_queue.h"
#include "decold.h"


int enable_migration = 1;
int enable_refs = 0;
int enable_topk = 0;
long int big_file = 0;
float migration_threshold = 0.8;

char g1_temp_path[128] = "/root/destor_test/g1/";
char g2_temp_path[128] = "/root/destor_test/g2/";

char g1[32] = "/root/destor_test/g1/recipes/";
char g2[32] = "/root/destor_test/g2/recipes/";

SyncQueue *read_destor_queue;
SyncQueue *write_g1_identified_file_temp_queue;
SyncQueue *write_g2_identified_file_temp_queue;

SyncQueue *write_identified_file_to_destor_queue;
SyncQueue *write_g1_identified_file_to_destor_queue;
SyncQueue *write_g2_identified_file_to_destor_queue;

SyncQueue *update_remianed_files_queue;
SyncQueue *update_g1_remianed_files_queue;
SyncQueue *update_g2_remianed_files_queue;


SyncQueue write_remained_files_queue;
SyncQueue write_g1_remained_files_queue;
SyncQueue write_g2_remained_files_queue;

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
	if (group == 1) {
	    update_remianed_files_queue = update_g1_remianed_files_queue;
	} else {
	    update_remianed_files_queue = update_g2_remianed_files_queue;
	}
	
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
	    if (left > right) {
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
	    if (left > right) {
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

	    sync_queue_push(update_remianed_files_queue, one_file);
	     
	}
	sync_queue_term(update_remianed_files_queue);

}



void intersection(const char *path1, const char *path2)
{
	double time_1 = 0, time_2 = 0, time_3 = 0, time_4 = 0;
	TIMER_DECLARE(1);

	TIMER_BEGIN(1);

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
	
	push_identified_files(identified_file1, identified_file1_count, write_g1_identified_file_temp_queue);
	push_identified_files(identified_file2, identified_file2_count, write_g2_identified_file_temp_queue);

	update_remained_files(0, file1_ord, file1_count, s1_ord, s1_count, identified_file1, identified_file1_count, m1, m1_count);
	update_remained_files(1, file2_ord, file2_count, s2_ord, s2_count, identified_file2, identified_file2_count, m2, m2_count);


	TIMER_END(1, time_1);
}

void * read_from_destor_thread(void *arg)
{
    		
    return NULL;
}

void * write_identified_file_to_temp_thread(void *arg)
{

    char *group = arg;
    char *temp_path;
    SyncQueue *write_identified_file_temp_queue;
    if (!strcmp(group, "g1")) 
    {
	write_identified_file_temp_queue = write_g1_identified_file_temp_queue;
	temp_path = g1_temp_path;
    }
    else
    {
	write_identified_file_temp_queue =write_g2_identified_file_temp_queue;
	temp_path = g2_temp_path;
    }

    
    uint64_t identified_file_count = 0;
    struct identified_file_info *file;
    FILE *filep = NULL;
    while ((file = sync_queue_pop(write_identified_file_temp_queue))) {
	if (NULL == NULL) {
	    filep = fopen(temp_path, "w+");
	    if (NULL == filep) {
		printf("fopen file failed\n");
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
    char *temp_path;
    SyncQueue *write_identified_file_to_destor_queue;
    if (!strcmp(group, "g1")) 
    {
	temp_path = g1_temp_path;
	write_identified_file_to_destor_queue = write_g1_identified_file_to_destor_queue;
    }
    else
    {
	temp_path = g2_temp_path;
	write_identified_file_to_destor_queue = write_g2_identified_file_to_destor_queue;
    }
    FILE *filep = fopen(temp_path, "r");
    if (NULL == filep)
	printf("fopen file failed\n");
    
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

void read_remained_files_data_thread(void *arg) {

    SyncQueue *remained_files_queue;
    struct remained_file_info *one_file;
    char pool_path[128];

    char *group = arg;
    if (!strcmp(group, "g1")) 
	snprintf(pool_path, "%s/%s", g1, container.pool);
	remained_files_queue = update_g1_remianed_files_queue; 
    } else { 
	snprintf(pool_path, "%s/%s", g2, container.pool);
	remained_files_queue = update_g2_remianed_files_queue; 
    }

    static int metabufsize = 64*1024;
    char *metabuf = malloc(metabufsize);
    int32_t metabufoff = 0;
    uint64_t recipe_offset = 0;
    int one_chunk_size = sizeof(fingerprint) + sizeof(containerid) + sizeof(int32_t);

    static int recordbufsize = 64*1024;
    int32_t recordbufoff = 0;
    char *recordbuf = malloc(recordbufsize);
    recipe_offset = one_chunk_size;

    GHashTable *recently_unique_chunks = g_hash_table_new_full(g_int64_hash, g_fingerprint_equal, NULL, free_chunk);
    
     
    FILE *pool_fp = fopen(pool_path);
    char *data = NULL;
    while ((one_file = sync_queue_pop(remained_files_queue))) {
	uint64_t i = 0;
	
        memcpy(metabuf + metabufoff, &(one_file->fid), sizeof(one_file->fid));
        metabufoff += sizeof(one_file->fid);
        memcpy(metabuf + metabufoff, &recipe_offset, sizeof(recipe_offset));
        metabufoff += sizeof(recipe_offset);

        memcpy(metabuf + metabufoff, &one_file->chunknum, sizeof(one_file->chunknum));
        metabufoff += sizeof(one_file->chunknum);
        memcpy(metabuf + metabufoff, &one_file->size, sizeof(one_file->size));
        b->metabufoff += sizeof(r->filesize);

	recipe_offset += (one_file->chunknum) * one_chunk_size;
	
	
	for (i = 0; i < one_file->chunknum; i++) {
	    struct chunk* ruc = g_hash_table_lookup(recently_unique_chunks, &one_file->fps[i]);
	    if (NULL == ruc) {
		if (storage_buffer.container_buffer == NULL) {
		    storage_buffer.container_buffer = create_container();
		    storage_buffer.chunks = g_sequence_new(free_chunk);
		}
		ruc = (struct chunk *)malloc(sizeof(struct chunk));
		
		g_hash_table_insert(recently_unique_chunks, &ne_file->fps[i], );
	    }
	    c->id = ruc->id;

	    int32_t chunk_size = retrieve_from_container(pool_fp, one_file->fps_cid[i], &data, one_file->fps[i]);
	    struct fp_data * one_data = (struct fp_data *)malloc(sizeof(struct fp_data));			
	    one_data->data = data;	
	    memcpy(recordbuf + recordbufoff, one_file->fps[i]), sizeof(fingerprint); 
	    recordbufoff += sizeof(fingerprint);
	    memcpy(recordbuf + recordbufoff, , sizeof(containerid); 
	    recordbufoff += sizeof(containerid);
	    memcpy(recordbuf + recordbufoff, &chunk_size, sizeof(chunk_size); 
	    recordbufoff += sizeof(chunk_size);

	}		
	
    }
    
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

	pthread_t tid1;
	pthread_t tid2;
	read_destor_queue = sync_queue_new(100);	
	write_g1_identified_file_temp_queue = sync_queue_new(100);	

	pthread_create(&tid1, NULL, read_from_destor_thread, NULL);
	pthread_create(&tid2, NULL, write_identified_file_to_temp_thread, NULL);

    
	intersection(g1, g2);
	

	pthread_join(tid2, NULL);
	return 0;
}
