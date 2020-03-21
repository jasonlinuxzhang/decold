#include "common.h"
#include "decold.h"

extern int target_group;
extern char g1_path[32];
extern char g2_path[32];
uint64_t item_count = 0;

void hash2code(unsigned char hash[20], char code[40])
{
	int i, j, b;
	unsigned char a, c;
	i = 0;
	for (i = 0; i < 20; i++)
	{
		a = hash[i];
		for (j = 0; j < 2; j++)
		{
			b = a / 16;
			switch (b)
			{
			case 10:
				c = 'A';
				break;
			case 11:
				c = 'B';
				break;
			case 12:
				c = 'C';
				break;
			case 13:
				c = 'D';
				break;
			case 14:
				c = 'E';
				break;
			case 15:
				c = 'F';
				break;
			default:
				c = b + 48;
				break;
			}
			code[2 * i + j] = c;
			a = a << 4;
		}
	}
}

void decold_log(const char *fmt, ...)
{
	va_list ap;
	char msg[1024];

	va_start(ap, fmt);
	vsnprintf(msg, sizeof(msg), fmt, ap);
	va_end(ap);

	fprintf(stdout, "%s", msg);
}

int comp_code(unsigned char hash1[20], unsigned char hash2[20])
{
	int i = 0;
	while (i<20)
	{
		if (hash1[i]>hash2[i])
			return 1;
		else if (hash1[i] < hash2[i])
			return -1;
		else
			i++;
	}

	return 0;
}

void print_unsigned(unsigned char *u, int64_t len)
{
	int64_t i;
	for (i = 0; i < len; i++)
		printf("%hhu", u[i]);
	printf("\n");
}

gboolean g_fingerprint_equal(const void *fp1, const void *fp2)
{
	return !memcmp((fingerprint*)fp1, (fingerprint*)fp2, sizeof(fingerprint));
}

gint g_fingerprint_cmp(fingerprint* fp1, fingerprint* fp2, gpointer user_data) {
    return memcmp(fp1, fp2, sizeof(fingerprint));
}

void print_key_value(gpointer key, gpointer value, gpointer user_data)
{
    char code[40] = {0};
    fingerprint *fp = key;
    struct chunk *ck = value; 
    hash2code(*fp, code);
    printf("%s --> %lu\n", code, ck->id);
}

void display_hash_table(GHashTable *table)
{
    g_hash_table_foreach(table, print_key_value, NULL);
}

void storage_key_value(gpointer key, gpointer value, gpointer user_data)
{

    fingerprint *fp = key;
    struct chunk *ck = value;
    FILE *filep = user_data;
    
    fwrite(*fp, sizeof(fingerprint), 1, filep);
    fwrite(ck, sizeof(struct chunk), 1, filep);
    
    item_count++;
}

void storage_hash_table(GHashTable *table) 
{
    char ghash_file[128] = {0};

    if (0 == target_group) {
	sprintf(ghash_file, "%s/ghash_file", g1_path);
	
    } else {
	sprintf(ghash_file, "%s/ghash_file", g2_path);
    }
    FILE *fp = fopen(ghash_file, "w+");
    if (NULL == fp) {
	printf("fopen %s failed\n", ghash_file);
	return;
    }

    item_count = 0;
    fwrite(&item_count, sizeof(item_count), 1, fp);

    g_hash_table_foreach(table, storage_key_value, fp);

    fseek(fp, 0, SEEK_SET);
    printf("ghash table have %lu item\n", item_count);
    fwrite(&item_count, sizeof(item_count), 1, fp);
    fclose(fp);
}
