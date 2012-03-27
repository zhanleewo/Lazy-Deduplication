#include "dedupe_fs.h"

extern char *dedupe_ini_file_store;
extern char *dedupe_metadata;
extern char *dedupe_hashes;

extern void dedupe_fs_fullpath(const char *, const char *);

extern int dedupe_fs_getattr(const char *, struct stat *);
extern int dedupe_fs_mkdir(const char *, mode_t);
extern int dedupe_fs_mknod(const char *, mode_t, dev_t);
extern int dedupe_fs_rmdir(const char *);
extern int dedupe_fs_unlink(const char *);
extern int dedupe_fs_open(const char *, struct fuse_file_info *);
extern int dedupe_fs_write(const char *, const char *, size_t, off_t, struct fuse_file_info *);
extern int dedupe_fs_read(const char *, const char *, size_t, off_t, struct fuse_file_info *);
extern int dedupe_fs_release(const char *, struct fuse_file_info *);

int create_stat_buf(struct stat *stbuf, char *stat_buf, size_t *len) {

  sprintf(stat_buf, 
    "%llu:%llu:%u:%u:%u:%u:%llu:%lld:%lu:%lld:%lu:%lu:%lu", 
    stbuf->st_dev, 
    stbuf->st_ino, 
    stbuf->st_mode,
    stbuf->st_nlink, 
    stbuf->st_uid, 
    stbuf->st_gid, 
    stbuf->st_rdev, 
    stbuf->st_size, 
    stbuf->st_blksize, 
    stbuf->st_blocks, 
    stbuf->st_atime, 
    stbuf->st_mtime, 
    stbuf->st_ctime);

  *len = strlen(stat_buf);

  return 0;
}

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

  dedupe_fs_fullpath(ab_path, path);

  dp = opendir(ab_path);
  if(NULL == dp) {
    sprintf(out_buf, "[%s] unable to opendir [%s] errno [%d]\n", __FUNCTION__, ab_path, errno);
    write(1, out_buf, strlen(out_buf));
    abort();
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

#ifdef DEBUG
    //sprintf(out_buf, "[%s] file name %s : file type %x\n", __FUNCTION__, new_path, de->d_type);
    //write(1, out_buf, strlen(out_buf));
#endif

    res = dedupe_fs_getattr(new_path, &stbuf);
    if(res < 0) {
      sprintf(out_buf, "[%s] stat failed on [%s] errno [%d]\n", __FUNCTION__, new_path, errno);
      write(1, out_buf, strlen(out_buf));
    }

    // Setup the metadata file path
    strcpy(meta_path, "/../..");
    strcat(meta_path, dedupe_metadata);
    strcat(meta_path, new_path);

    if(DT_DIR == de->d_type) {

      res = dedupe_fs_mkdir(meta_path, stbuf.st_mode);
      if(res < 0) {
        sprintf(out_buf, "[%s] mkdir failed on [%s] errno [%d]\n", __FUNCTION__, meta_path, errno);
        write(1, out_buf, strlen(out_buf));
      }

      process_initial_file_store(new_path);

      res = dedupe_fs_rmdir(new_path);
      if(res < 0) {
        sprintf(out_buf, "[%s] rmdir failed on [%s] errno [%d]\n", __FUNCTION__, new_path, errno);
        write(1, out_buf, strlen(out_buf));
      }

    } else {

      res = dedupe_fs_mknod(meta_path, stbuf.st_mode, 0); 
      if(res < 0) {
        sprintf(out_buf, "[%s] mknod failed on [%s] errno [%d]\n", __FUNCTION__, meta_path, errno);
        write(1, out_buf, strlen(out_buf));
      }

      fi.flags = O_WRONLY;
      res = dedupe_fs_open(meta_path, &fi);
      if(res < 0) {
        sprintf(out_buf, "[%s] open failed on [%s] errno [%d]\n", __FUNCTION__, meta_path, errno);
        write(1, out_buf, strlen(out_buf));
      }

      create_stat_buf(&stbuf, stat_buf, &stat_len);

      res = dedupe_fs_write(meta_path, (char *)stat_buf, stat_len, 0, &fi);
      if(res < 0) {
        sprintf(out_buf, "[%s] write failed on [%s] errno [%d]\n", __FUNCTION__, meta_path, errno);
        write(1, out_buf, strlen(out_buf));
      }

      res = dedupe_fs_release(meta_path, &fi);
      if(res < 0) {
        sprintf(out_buf, "[%s] release failed on [%s] errno [%d]\n", __FUNCTION__, meta_path, errno);
        write(1, out_buf, strlen(out_buf));
      }

      // TODO - redesign for locks - if already taken -EWOULDBLOCK then return w/o unlinking the file.
      // TODO - remove a dir from initial file store only if no files exist in it
      // TODO - Detect the boundaries in the file using Rabin-Karp Fingerprinting Algorithm
      // TODO - Calculate the hash value on each data block of the file using SHA-1 (160 bits)
      // TODO - Store the hash values in the metadata file after the stat information
      // TODO - Store the hash block inside the hash store if it does not already exist

      res = dedupe_fs_unlink(new_path);
      if(res < 0) {
        sprintf(out_buf, "[%s] unlink failed on [%s] errno [%d]\n", __FUNCTION__, path, errno);
        write(1, out_buf, strlen(out_buf));
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
    write(1, out_buf, strlen(out_buf));
#endif

    process_initial_file_store("");

    sleep(1);
  }
}
