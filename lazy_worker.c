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
extern int internal_write(const char *, char *, size_t, off_t, struct fuse_file_info *);
extern int internal_read(const char *, char *, size_t, off_t, struct fuse_file_info *);
extern int internal_release(const char *, struct fuse_file_info *);

extern int compute_rabin_karp(const char *, file_args *, struct stat *);

void process_initial_file_store(char *path) {

  int res = 0;

  size_t stat_len = 0;

  DIR *dp;
  struct dirent *de;
  struct stat stbuf;

  char out_buf[BUF_LEN];
  char ab_path[MAX_PATH_LEN];
  char new_path[MAX_PATH_LEN];
  char meta_path[MAX_PATH_LEN];

  char stat_buf[STAT_LEN];

  struct fuse_file_info fi;
  file_args f_args;

  dedupe_fs_filestore_path(ab_path, path);

  dp = opendir(ab_path);
  if(NULL == dp) {
    sprintf(out_buf, "[%s] unable to opendir [%s] errno [%d]\n", __FUNCTION__, ab_path, errno);
    WR_2_STDOUT;
    ABORT;
  }

  while((de = readdir(dp)) != NULL) {

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

    // Setup the metadata file path
    strcpy(meta_path, dedupe_metadata);
    strcat(meta_path, new_path);

    if(DT_DIR == de->d_type) {

      res = internal_mkdir(meta_path, stbuf.st_mode);
      if(res < 0) {
        ABORT;
      }

      process_initial_file_store(new_path);

      res = internal_rmdir(ab_path);
      if(res < 0) {
        ABORT;
      }

    } else {

      res = internal_mknod(meta_path, stbuf.st_mode, 0); 
      if(res < 0) {
        ABORT;
      }

      fi.flags = O_WRONLY;
      res = internal_open(meta_path, &fi);
      if(res < 0) {
        ABORT;
      }

      memset(&stat_buf, 0, STAT_LEN);

      stbuf2char(stat_buf, &stbuf);
      stat_buf[STAT_LEN-1] = '\n';

      res = internal_write(meta_path, (char *)stat_buf, STAT_LEN, (off_t)0, &fi);
      if(res < 0) {
        ABORT;
      }

      if(stbuf.st_size > 0) {

        f_args.path = meta_path;
        f_args.offset = STAT_LEN;
        f_args.fi = &fi;

        res = compute_rabin_karp(ab_path, &f_args, &stbuf);
        if(res < 0) {
          sprintf(out_buf, "[%s] Rabin-Karp finger-printing failed on [%s]\n", __FUNCTION__, new_path);
          WR_2_STDOUT;
          //TODO decide if return or abort
          ABORT;
        }
      }

      res = internal_release(meta_path, &fi);
      if(res < 0) {
        ABORT;
      }

      // TODO - redesign for locks - if already taken -EWOULDBLOCK then return w/o unlinking the file.
      // TODO - remove a dir from initial file store only if no files exist in it
      // TODO - Detect the boundaries in the file using Rabin-Karp Fingerprinting Algorithm
      // TODO - Calculate the hash value on each data block of the file using SHA-1 (160 bits)
      // TODO - Store the hash values in the metadata file after the stat information
      // TODO - Store the hash block inside the hash store if it does not already exist

      res = internal_unlink(ab_path);
      if(res < 0) {
        ABORT;
      }
    }
#endif /* _DIRENT_HAVE_D_TYPE */
  }

  closedir(dp);
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

    sleep(10);
  }
}
