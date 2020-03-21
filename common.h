#ifndef COMMON_H_
#define COMMON_H_

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <inttypes.h>
#include <stddef.h>
#include <string.h>
#include <stdarg.h>
#include <sys/time.h>
#include <errno.h>
#include <unistd.h>
#include <dirent.h>
#include <sys/stat.h>
#include <arpa/inet.h>
#include <assert.h>
#include <glib.h>

#define LEVEL 2
/*
#define ENABLE_MIGRATION 1

//only for refs>1
#define ENABLE_REFS 0

#define ENABLE_TOPK 0

// #define AVG_REFS 20

// define big small file
#define BIG_FILE (10*1024)
#define BIG_MIGRATION_THRESHOLD 0.8
// #define SMALL_MIGRATION_THRESHOLD 0.9
*/

#define TIMER_DECLARE(n) struct timeval b##n,e##n
#define TIMER_BEGIN(n) gettimeofday(&b##n, NULL)
#define TIMER_END(n,t) gettimeofday(&e##n, NULL); (t)+=e##n.tv_usec-b##n.tv_usec+1000000*(e##n.tv_sec-b##n.tv_sec); (t)=(t)/1000000*1.0

/* signal chunk */
#define CHUNK_FILE_START (0x0001)
#define CHUNK_FILE_END (0x0002)
#define CHUNK_SEGMENT_START (0x0004)
#define CHUNK_SEGMENT_END (0x0008)

#define SET_CHUNK(c, f) (c->flag |= f)
#define UNSET_CHUNK(c, f) (c->flag &= ~f)
#define CHECK_CHUNK(c, f) (c->flag & f)

typedef unsigned char fingerprint[20];
typedef int64_t containerid; //container id

gboolean g_fingerprint_equal(const void *fp1, const void *fp2);
gint g_fingerprint_cmp(fingerprint* fp1, fingerprint* fp2, gpointer user_data);

void decold_log(const char *fmt, ...);

#define VERBOSE(fmt, arg...) decold_log(fmt, ##arg);

void hash2code(unsigned char hash[20], char code[40]);

void display_hash_table(GHashTable *table);

void storage_hash_table(GHashTable *table);

#endif
