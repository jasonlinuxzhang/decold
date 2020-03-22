#include "cal.h"

int64_t MIGRATION_COUNT = 0;
extern int enable_migration;
extern int enable_refs;
extern int enable_topk;
extern unsigned long int big_file;
extern float migration_threshold;


static int comp(const void *s1, const void *s2)
{
	return comp_code(((struct fp_info *)s1)->fp, ((struct fp_info *)s2)->fp);
}

static int comp_scommon(const void *s1, const void *s2)
{
	return ((struct fp_info *)s1)->fid - ((struct fp_info *)s2)->fid;
}

static int comp_mr(const void *mr1, const void *mr2)
{
	return ((struct file_info *)mr1)->fid - ((struct file_info *)mr2)->fid;
}


//we copy another S1 S2 for R1 and migartion sort
void cal_inter(struct fp_info *s1, int64_t s1_count, struct fp_info *s2, int64_t s2_count, struct fp_info **scommon1, int64_t *sc1_count, struct fp_info **scommon2, int64_t *sc2_count)
{
	//sorted s1
	qsort(s1, s1_count, sizeof(struct fp_info), comp);

	qsort(s2, s2_count, sizeof(struct fp_info), comp);

	int64_t i;

	//malloc for the scommon
	struct fp_info *scommon11 = (struct fp_info *)malloc(sizeof(struct fp_info) * s1_count);
	struct fp_info *scommon22 = (struct fp_info *)malloc(sizeof(struct fp_info) * s2_count);

	int64_t a = 0, b = 0;
	while (a < s1_count && b < s2_count)
	{
		if (comp_code(s1[a].fp, s2[b].fp) == 0)
		{
			//It can consider re-writer, we think fps will set in the same container

			int64_t c = 1, d = 1;
			//check that c has some same file
			while (a + c < s1_count && comp_code(s1[a].fp, s1[a + c].fp) == 0)
			{
				c++;
			}
			//check that d has some same file.

			while (b + d < s2_count && comp_code(s2[b].fp, s2[b + d].fp) == 0)
			{
				d++;
			}

			for (i = 0; i < c; i++)
			{
				memcpy(scommon11[*sc1_count].fp, s1[a + i].fp, sizeof(fingerprint));
				scommon11[*sc1_count].size = s1[a + i].size;

				scommon11[*sc1_count].cid = s1[a + i].cid;
				scommon11[*sc1_count].fid = s1[a + i].fid;
				scommon11[*sc1_count].order = s1[a + i].order;
				//VERBOSE("G1 GET SAME CHUNK   CHUNK:fid=[%8" PRId64 "], order=%" PRId64 ", size=%" PRId64 ", container_id=%" PRId64 "\n", scommon11[*sc1_count].fid, scommon11[*sc1_count].order, scommon11[*sc1_count].size, scommon11[*sc1_count].cid);
				(*sc1_count)++;
    
			}

			for (i = 0; i < d; i++)
			{
				memcpy(scommon22[*sc2_count].fp, s2[b + i].fp, sizeof(fingerprint));
				scommon22[*sc2_count].size = s2[b + i].size;

				scommon22[*sc2_count].cid = s2[b + i].cid;
				scommon22[*sc2_count].fid = s2[b + i].fid;
				scommon22[*sc2_count].order = s2[b + i].order;
				//VERBOSE("G2 GET SAME CHUNK   CHUNK:fid=[%8" PRId64 "], order=%" PRId64 ", size=%" PRId64 ", container_id=%" PRId64 "\n", scommon22[*sc2_count].fid, scommon22[*sc2_count].order, scommon22[*sc2_count].size, scommon22[*sc2_count].cid);
				(*sc2_count)++;
			}

			a += c;
			b += d;
		}
		else if (comp_code(s1[a].fp, s2[b].fp) > 0)
		{
			b++;
		}
		else
		{
			a++;
		}
	}

	//sorted by fps
	scommon11 = (struct fp_info *)realloc(scommon11, *sc1_count * sizeof(struct fp_info));
	scommon22 = (struct fp_info *)realloc(scommon22, *sc2_count * sizeof(struct fp_info));

	*scommon1 = scommon11;
	*scommon2 = scommon22;
}

//check the full chunks in the S common
//0 none and migration < shreshold, <0 ->migration, >0 intersection
//if the bigger file, threshold smaller
static int64_t file_dedup_migration(struct file_info *mr, uint64_t count, uint64_t fid, uint64_t chunknum, int64_t mig_count[8])
{
	//binarySearch
	int64_t begin = 0, end = count - 1;
	while (begin <= end)
	{
		int64_t mid = (begin + end) / 2;
		if (mr[mid].fid == fid)
		{
			//TO-DO here for miagration!!!
			if (mr[mid].chunknum == chunknum)
				return mr[mid].chunknum; //>0 intersection
			else
			{
				//only big file migration
				if (enable_migration == 1)
				{
					double ratio = 1.0 * chunknum / mr[mid].chunknum;
					if (mr[mid].size >= big_file && ratio >= migration_threshold)
					{
						if (LEVEL >= 2)
							VERBOSE("BIG MIGRATION NO.%" PRId64 ", [%8" PRId64 "](size=%"PRId64"), { get [%" PRId64 "] of [%" PRId64 "] = %lf.} ==> MAY BE MIGRATION!\n", \
							MIGRATION_COUNT++, fid, mr[mid].size, chunknum, mr[mid].chunknum, ratio);

						if(ratio >=0.6)
						{
							mig_count[0]++;
							if(ratio >=0.65)
							{
								mig_count[1]++;
								if(ratio >=0.7)
								{
									mig_count[2]++;
									if(ratio >=0.75)
									{
										mig_count[3]++;
										if(ratio >=0.8)
										{
											mig_count[4]++;
											if(ratio >=0.85)
											{
												mig_count[5]++;
												if(ratio >=0.9)
												{
													mig_count[6]++;
													if(ratio >=0.95)
													{
														mig_count[7]++;
													}
												}
											}
										}
									}
								}
							}

						}

						return 0 - mr[mid].chunknum; //mark for migration
					}

				}

				return 0; //migration < shreshold
			}
		}
		else if (mr[mid].fid < fid)
			begin = mid + 1;
		else
			end = mid - 1;
	}

	return 0; //none
}



void file_find(struct file_info *mr, int64_t mr_count, struct fp_info *scommon, int64_t sc_count, struct identified_file_info **fff, int64_t *ff_count, struct migrated_file_info **mm, int64_t *m_count, int64_t mig_count[8])
{
	int64_t i;
	//sort mr1 by fid
	qsort(mr, mr_count, sizeof(struct file_info), comp_mr);

	if (LEVEL >= 2)
	{
		VERBOSE("\n----------------------------------------------------------------\n");
		//show mr1
		for (i = 0; i < mr_count; i++)
		{
		//	VERBOSE("SORT MAP RECIPE: [%8" PRId64 "],num=%" PRId64 ",size=%" PRId64 ";offset_m=%" PRId64 ";offset_r=%" PRId64 ";\n", mr[i].fid, mr[i].chunknum, \
				mr[i].size, mr[i].offset_m, mr[i].offset_r);
			/*
			VERBOSE("SORT MAP RECIPE: [%8" PRId64 "],num=%" PRId64 ",size=%" PRId64 ";\n", mr[i].fid, mr[i].chunknum, \
			mr[i].size);
			*/
		}
		VERBOSE("\n----------------------------------------------------------------\n");
	}

	struct identified_file_info *ff = (struct identified_file_info *)malloc(sc_count * sizeof(struct identified_file_info));
	struct migrated_file_info *m = (struct migrated_file_info *)malloc(sc_count * sizeof(struct migrated_file_info));

	//sorted ccommon by fid
	qsort(scommon, sc_count, sizeof(struct fp_info), comp_scommon);

	for (i = 0; i < sc_count;)
	{
		//VERBOSE("FILE_FIND CHUNK:fid=[%8" PRId64 "], order=%" PRId64 ", size=%" PRId64 ", container_id=%" PRId64 "\n", scommon[i].fid, scommon[i].order, scommon[i].size, scommon[i].cid);
		int64_t j = 1;
		while (i + j < sc_count && scommon[i].fid == scommon[i + j].fid)
			j++;

		//all chunk = chunksize of the file
		int64_t k = file_dedup_migration(mr, mr_count, scommon[i].fid, j, mig_count);
		if (k > 0)
		{
			//put the files in the common file
			ff[*ff_count].fid = scommon[i].fid;
			ff[*ff_count].num = k;
			ff[*ff_count].fps = (fingerprint *)malloc(k * sizeof(fingerprint));
			ff[*ff_count].sizes = (uint64_t *)malloc(k * sizeof(int64_t));
			//ff[*ff_count].cids = (int64_t *)malloc(k * sizeof(int64_t));

			int64_t s;
			for (s = 0; s < k; s++)
			{
				//fps are sorted by order
				int64_t order = scommon[i + s].order;
				memcpy((ff[*ff_count].fps)[order], scommon[i + s].fp, sizeof(fingerprint));
				(ff[*ff_count].sizes)[order] = scommon[i + s].size;
				//cid
				//(ff[*ff_count].cids)[order] = s1_2[i + s].cid;
			}

			(*ff_count)++;
		}

		if (enable_migration == 1 && k < 0) //migration
		{
			int64_t total_num = (int64_t)(0 - k);
			//put the files in the migarion buffer
			m[*m_count].fid = scommon[i].fid;
			m[*m_count].total_num = total_num;
			m[*m_count].fps = (fingerprint *)malloc(total_num * sizeof(fingerprint));
			m[*m_count].arr = (uint64_t *)calloc(1, 2 * total_num * sizeof(int64_t));
			//m[*m_count].in = (int64_t *)calloc(1, total_num * sizeof(int64_t));
			// m[*m_count].cids = (int64_t *)calloc(1, total_num * sizeof(int64_t));

			int64_t s;
			for (s = 0; s < j; s++)
			{
				//fps are sorted by order
				int64_t order = scommon[i + s].order;

				memcpy((m[*m_count].fps)[order], scommon[i + s].fp, sizeof(fingerprint));

				(m[*m_count].arr)[order] = scommon[i + s].size;
				//in ==1, mean it in the scommon
				(m[*m_count].arr)[order + total_num] = 1;
				//cid
				//(m[*m_count].cids)[order] = s1_2[i + s].cid;
			}

			(*m_count)++;
		}

		// the file is not common file.

		i = i + j;
	}

	ff = (struct identified_file_info *)realloc(ff, *ff_count * sizeof(struct identified_file_info));
	*fff = ff;

	if (enable_migration == 1)
	{
		m = (struct migrated_file_info *)realloc(m, *m_count * sizeof(struct migrated_file_info));
		*mm = m;
	}
}

