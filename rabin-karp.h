#ifndef __RABIN_KARP_H
#define __RABIN_KARP_H

#include "dedupe_fs.h"

#define R 256
#define Q 15497
#define SUBSTRING_LEN 4096
#define HEXA_HASH_LEN 41
#define INT_MAX_LEN 21
#define MINCHUNK 4096
#define MAXCHUNK 8192
#define TRUE 1
#define FALSE 0
#define BITMASK 4095

#define NLINKS_WIDTH 20

#define OFF_HASH_LEN 2*INT_MAX_LEN+HEXA_HASH_LEN+1

void precompute_RM();

int pattern_match(unsigned long long int rkhash);

unsigned long long int Rabin_Karp_Hash(char substring[],unsigned long long int start_index,unsigned long long int end_index,int newchunk, unsigned long long int hash_prev);

char * copy_substring(char *str, char *s, unsigned long long int start,unsigned long long int end);

void create_chunkfile(char str[],char shastr[], size_t);

int compute_rabin_karp(char *filestore_path, file_args *f_args, struct stat *stbuf);
#endif
