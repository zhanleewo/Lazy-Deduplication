#include "dedupe_fs.h"

extern char *dedupe_ini_file_store;
extern char *dedupe_metadata;
extern char *dedupe_hashes;

extern void dedupe_fs_fullpath(const char *, const char *);
extern int dedupe_fs_getattr(const char *, struct stat *);


void process_initial_file_store(char *path) {

  int res = 0;

  DIR *dp;
  struct dirent *de;
  struct stat stbuf;

  char out_buf[BUF_LEN];
  char ab_path[MAX_PATH_LEN];
  char new_path[MAX_PATH_LEN];
  char meta_path[MAX_PATH_LEN];

  dedupe_fs_fullpath(ab_path, path);

  dp = opendir(ab_path);
  if(NULL == dp) {
    sprintf(out_buf, "[%s] unable to open %s\n", __FUNCTION__, ab_path);
    write(1, out_buf, strlen(out_buf));
    abort();
  }

  while((de = readdir(dp)) != NULL) {

#ifdef _DIRENT_HAVE_D_TYPE

    strcpy(new_path, path);
    strcat(new_path, "/");
    strcat(new_path, de->d_name);

    sprintf(out_buf, "[%s] file name %s : file type %x\n", __FUNCTION__, new_path, de->d_type);
    write(1, out_buf, strlen(out_buf));

    if(DT_DIR == de->d_type) {

      if(SUCCESS == strcmp(de->d_name, ".") ||
          SUCCESS == strcmp(de->d_name, "..")) {
        continue;
      }

      process_initial_file_store(new_path);

    } else {

      res = dedupe_fs_getattr(new_path, &stbuf);
      if(res < 0) {
        sprintf(out_buf, "[%s] stat failed on %s %d\n", __FUNCTION__, new_path, errno);
        write(1, out_buf, strlen(out_buf));
      }
    }
#endif
  }
  closedir(dp);
}

void *lazy_worker_thread(void *arg) {

  char out_buf[BUF_LEN];
  //char ab_path[MAX_PATH_LEN];

  while(TRUE) {
#ifdef DEBUG
    sprintf(out_buf, "[%s] executing..\n", __FUNCTION__);
    write(1, out_buf, strlen(out_buf));
#endif

    process_initial_file_store("");

    sleep(1);
  }
}
