#ifndef __RABIN_KARP_H
#define __RABIN_KARP_H

#define SUBSTRING_LEN 5
#define BASE 5
#define MODULO_PRIME 7
#define PATTERN_HASH 4
#define MINCHUNK 8
#define MAXCHUNK 10
#define TRUE 1
#define FALSE 0
#define BITMASK 15

int pattern_match(long long int rkhash);

long int Rabin_Karp_Hash(char substring[],long long int start_index,long long int end_index);

char * copy_substring(char *str, int start,int end);

#endif
