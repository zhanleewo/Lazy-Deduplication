#include "dedupe_fs.h"

extern char *dedupe_ini_file_store;
extern char *dedupe_metadata;
extern char *dedupe_hashes;

extern void dedupe_fs_fullpath(const char *, const char *);
extern int dedupe_fs_getattr(const char *, struct stat *);

void *lazy_worker_thread(void *arg) {
  int res = 0;

  char filename[MAX_PATH_LEN];
  char out_buf[BUF_LEN];
  char ab_path[MAX_PATH_LEN];

  DIR *dp;
  struct dirent *de;
  struct stat stbuf;

  while(TRUE) {

    dp = opendir(dedupe_ini_file_store);
    if(NULL == dp) {
      sprintf(out_buf, "[%s] unable to open %s\n", __FUNCTION__, dedupe_ini_file_store);
      write(1, out_buf, strlen(out_buf));
      abort();
    }

    while((de = readdir(dp)) != NULL) {
#ifdef _DIRENT_HAVE_D_TYPE
      sprintf(out_buf, "[%s] file name %s : file type %x\n", __FUNCTION__, de->d_name, de->d_type);
      write(1, out_buf, strlen(out_buf));

      strcpy(filename, "/");
      strcat(filename, de->d_name);

      res = dedupe_fs_getattr(filename, &stbuf);
      if(res < 0) {
        sprintf(out_buf, "[%s] stat failed %d\n", __FUNCTION__, errno);
        write(1, out_buf, strlen(out_buf));
      }
#endif
    }

    closedir(dp);

    sleep(1);
  }
}
