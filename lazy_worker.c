#include "dedupe_fs.h"
#include "internal_cmds.h"

extern char *dedupe_file_store;
extern char *dedupe_metadata;
extern char *dedupe_hashes;

extern void dedupe_fs_filestore_path(const char *, const char *);

extern int internal_getattr(const char *, struct stat *);
extern int internal_mkdir(const char *, mode_t);
extern int internal_mknod(const char *, mode_t, dev_t);
extern int internal_rmdir(const char *);
extern int internal_unlink(const char *);
extern int internal_open(const char *, struct fuse_file_info *);
extern int internal_write(const char *, char *, size_t, off_t, struct fuse_file_info *, int locked);
extern int internal_read(const char *, char *, size_t, off_t, struct fuse_file_info *, int locked);
extern int internal_release(const char *, struct fuse_file_info *);

extern int compute_rabin_karp(const char *, file_args *, struct stat *);

void updates_handler(const char *path) {

  int i = 0, j = 0;
  int res = 0, block_num = 0;

  char *new_f_path_end = NULL;
  char *meta_f_path_end = NULL;

  char data_buf[MAXCHUNK] = {0};
  char stat_buf[STAT_LEN] = {0};
  char out_buf[BUF_LEN] = {0};
  char meta_f_path[MAX_PATH_LEN] = {0};
  char ab_path[MAX_PATH_LEN] = {0};
  char ab_f_path[MAX_PATH_LEN] = {0};

  unsigned int *btmsk = NULL;

  struct fuse_file_info bitmask_fi;

  dedupe_fs_filestore_path(ab_path, path);
  dedupe_fs_metadata_path(meta_path, path);

  strcpy(ab_f_path, ab_path);

  new_f_path_end = strstr(ab_f_path, BITMASK_FILE);
  if(NULL == new_f_path_end) {
    sprintf(out_buf, "[%s] not a %s filetype\n", __FUNCTION__, BITMASK_FILE);
    WR_2_STDOUT;
    ABORT;
  }
  *new_f_path_end = NULL;

  meta_f_path_end = strstr(meta_path, BITMASK_FILE);
  if(NULL == meta_f_path_end) {
    sprintf(out_buf, "[%s] not a %s filetype\n", __FUNCTION__, BITMASK_FILE);
    WR_2_STDOUT;
    ABORT;
  }
  *meta_f_path_end = NULL;

  bitmask_fi.flags = O_RDWR;
  res = internal_open(ab_path, &bitmask_fi);
  if(res < 0) {
    ABORT;
  }

  btmsk = (unsigned int *) mmap(NULL, BITMASK_LEN, 
                  PROT_READ | PROT_WRITE, MAP_SHARED, bitmask_fi.fh, (off_t)0);
  if(btmsk == MAP_FAILED) {
    sprintf(out_buf, "[%s] mmap failed on [%s]", __FUNCTION__, path);
    perror(out_buf);
    res = -errno;
    return res;
  }

  internal_release(path, &bitmask_fi);

  res = internal_open(meta_path, &meta_fi);
  if(res < 0) {
    ABORT;
  }

  memset(stat_buf, 0, STAT_LEN);
  res = internal_read(meta_path, stat_buf, STAT_LEN, (off_t)0, &meta_fi, FALSE);
  if(res <= 0) {
    ABORT;
  }

  // TODO Read the file and recompute rabin-karp only for the
  // blocks which has been updated

  for(i = 0 ; i < NUM_BITMASK_WORDS ; i++) {

    if(btmsk[i] >= TRUE) {
      for(j = 0 ; j < 32 ; j++) {

        if(btmsk[i] & (1<<j)) {

          block_num = i*32 + j;

        }
      }
    }
  }

  res = munmap(btmsk, BITMASK_LEN);
  if(FAILED == res) {
    ABORT;
  }

  internal_release(meta_path, &meta_fi);
}

void process_initial_file_store(char *path) {

  int res = 0;

  size_t stat_len = 0;

  struct dirent *de;

  struct stat stbuf, meta_d_stbuf;
  struct stat ab_f_stbuf,  meta_f_stbuf;

  char *new_f_path_end = NULL;

  char out_buf[BUF_LEN] = {0};
  char ab_path[MAX_PATH_LEN] = {0};
  char ab_old_path[MAX_PATH_LEN] = {0};
  char new_path[MAX_PATH_LEN] = {0};
  char meta_path[MAX_PATH_LEN] = {0};

  char ab_f_path[MAX_PATH_LEN] = {0};
  char new_f_path[MAX_PATH_LEN] = {0};
  char meta_f_path[MAX_PATH_LEN] = {0};

  char stat_buf[STAT_LEN] = {0};

  struct fuse_file_info fi, dir_fi;
  struct fuse_file_info bitmask_fi;
  file_args f_args;

  dedupe_fs_filestore_path(ab_old_path, path);

  res = internal_opendir(ab_old_path, &dir_fi);
  if(res < 0) {
    ABORT;
  }

  while((de = readdir(dir_fi.fh)) != NULL) {

#ifdef _DIRENT_HAVE_D_TYPE

    if(SUCCESS == strcmp(de->d_name, ".") ||
      SUCCESS == strcmp(de->d_name, "..")) {
      continue;
    }

    strcpy(new_path, path);
    strcat(new_path, "/");
    strcat(new_path, de->d_name);

    strcpy(ab_path, dedupe_file_store);
    strcat(ab_path, new_path);

    res = internal_getattr(ab_path, &stbuf);
    if(res < 0) {
      ABORT;
    }

    if(DT_DIR == de->d_type) {

      strcpy(meta_path, dedupe_metadata);
      strcat(meta_path, new_path);

      res = internal_getattr(meta_path, &meta_d_stbuf);

      if(-ENOENT == res) {

        res = internal_mkdir(meta_path, stbuf.st_mode);
        if(res < 0) {
          ABORT;
        }
      }

      process_initial_file_store(new_path);

    } else {

      strcpy(new_f_path, path);
      strcat(new_f_path, "/");
      strcat(new_f_path, de->d_name);
 
      if((new_f_path_end = strstr(new_f_path, BITMASK_FILE)) != NULL) {

        *new_f_path_end = '\0';

        strcpy(ab_f_path, dedupe_file_store);
        strcat(ab_f_path, new_f_path);

        strcpy(meta_f_path, dedupe_metadata);
        strcat(meta_f_path, new_f_path);

        res = internal_getattr(meta_f_path, &meta_f_stbuf);

        if(-ENOENT == res) {

          /* Careful with the last block updation */
          res = internal_getattr(ab_f_path, &ab_f_stbuf);
          if(res < 0) {
            ABORT;
          }

          res = internal_mknod(meta_f_path, ab_f_stbuf.st_mode, 0); 
          if(res < 0) {
            ABORT;
          }
         
          fi.flags = O_WRONLY;
          res = internal_open(meta_f_path, &fi);
          if(res < 0) {
            ABORT;
          }
         
          memset(&stat_buf, 0, STAT_LEN);
         
          stbuf2char(stat_buf, &ab_f_stbuf);
          stat_buf[STAT_LEN-1] = '\n';

          res = internal_write(meta_f_path, (char *)stat_buf, STAT_LEN, (off_t)0, &fi, FALSE);
          if(res < 0) {
            ABORT;
          }
         
          if(ab_f_stbuf.st_size > 0) {

            f_args.path = meta_f_path;
            f_args.offset = STAT_LEN;
            f_args.fi = &fi;
         
            res = compute_rabin_karp(ab_f_path, &f_args, &ab_f_stbuf);
            if(res < 0) {
              sprintf(out_buf, "[%s] Rabin-Karp finger-printing failed on [%s]\n", __FUNCTION__, new_path);
              WR_2_STDOUT;
              //TODO decide if return or abort
              ABORT;
            }
          }
         
          res = internal_release(meta_f_path, &fi);
          if(res < 0) {
            ABORT;
          }
         
          res = internal_truncate(ab_f_path, (off_t)0);
          if(res < 0) {
            ABORT;
          }

        } else {

          /* File already exists in the dedupe database */
          /* i.e., file could have been updated since last dedupe pass*/

          /* Careful with the last block updation */

          updates_handler(new_path);

        }
      }
    }
#endif /* _DIRENT_HAVE_D_TYPE */
  }

  internal_releasedir(ab_old_path, &dir_fi);
}

void *lazy_worker_thread(void *arg) {

  char out_buf[BUF_LEN];
  //char ab_path[MAX_PATH_LEN];

  while(TRUE) {
#ifdef DEBUG
    sprintf(out_buf, "[%s] executing..\n", __FUNCTION__);
    WR_2_STDOUT;
#endif

    process_initial_file_store("");

    sleep(90);
  }
}
