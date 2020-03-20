#ifndef CAL_H_
#define CAL_H_

#include "common.h"
#include "recipe.h"

struct identified_file_info
{
	uint64_t fid;
	uint64_t num;
	uint64_t filesize;
	uint64_t flag; //1-> dedup, 0-> stay
	fingerprint *fps; //sorted by order
	uint64_t *sizes; //for size
	//int64_t *cids; //for container ids
};

struct migrated_file_info
{
	uint64_t fid;
	uint64_t total_num;
	uint64_t filesize;
	fingerprint *fps; //sorted by order
	uint64_t *arr;//0~total, for size; total~2*total, for if it exist
	// int64_t *sizes; //for size
	// int64_t *in; //if it exist
	//int64_t *cids;
};

void cal_inter(struct fp_info *s1, int64_t s1_count, struct fp_info *s2, int64_t s2_count, struct fp_info **scommon1, int64_t *sc1_count, struct fp_info **scommon2, int64_t *sc2_count);

void file_find(struct file_info *mr, int64_t mr_count, struct fp_info *scommon, int64_t sc_count, struct identified_file_info **fff, int64_t *ff_count, struct migrated_file_info **mm, int64_t *m_count, int64_t mig_count[5]);

#endif
