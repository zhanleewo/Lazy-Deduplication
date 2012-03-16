#ifndef DEDUPE_FS_H
#define DEDUPE_FS_H

#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#include <unistd.h>
#include <fcntl.h>
#include <dirent.h>

#include <errno.h>
#include <pthread.h>
#include <fuse.h>

#define MAX_PATH_LEN 1024
#define BUF_LEN 512 
#define STAT_LEN 256

#define SUCCESS 0
#define FAILED -1

#ifndef TRUE
#define TRUE 1
#endif

#ifndef FALSE
#define FALSE 0
#endif

typedef struct thread_arg {
  int thr_num;
} thread_arg_t;

#endif /* DEDUPE_FS_H */
