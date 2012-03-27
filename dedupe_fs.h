#ifndef DEDUPE_FS_H
#define DEDUPE_FS_H

#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#include <unistd.h>
#include <fcntl.h>
#include <dirent.h>
#include <sys/types.h>
#include <sys/syscall.h>
#include <sys/file.h>

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

static pid_t gettid(void) {
  return syscall(SYS_gettid);
}

typedef struct thread_arg {
  int thr_num;
} thread_arg_t;

typedef struct dedupe_fs_globals {
  pthread_t         thr_handle;
  pthread_attr_t    thr_attr;
  thread_arg_t      thr_arg;
  pthread_mutex_t   lk;
} dedupe_globals;

#endif /* DEDUPE_FS_H */
