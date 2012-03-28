//gcc -Wall `pkg-config fuse --cflags --libs` dedupe_fs.c -o dedupe_fs


#define FUSE_USE_VERSION 26

#include "dedupe_fs.h"

char *dedupe_file_store = "/tmp/dedupe_file_store";
char *dedupe_metadata = "/tmp/dedupe_metadata";
char *dedupe_hashes = "/tmp/dedupe_hashes";
dedupe_globals globals;

extern void *lazy_worker_thread(void *);

static void usage() {
  char out_buf[BUF_LEN];

  sprintf(out_buf, "dedupe_fs [MOUNT_POINT]\n");
  write(1, out_buf, strlen(out_buf));

  exit(1);
}

void dedupe_fs_filestore_path(char ab_path[MAX_PATH_LEN], const char *path) {

  memset(ab_path, 0, MAX_PATH_LEN);
  strcpy(ab_path, dedupe_file_store);
  strcat(ab_path, path);
  return;
}

void dedupe_fs_metadata_path(char ab_path[MAX_PATH_LEN], const char *path) {

  memset(ab_path, 0, MAX_PATH_LEN);
  strcpy(ab_path, dedupe_metadata);
  strcat(ab_path, path);
  return;
}

int dedupe_fs_getattr(const char *path, struct stat *stbuf) {

  int res = 0;
  char out_buf[BUF_LEN];
  char stat_buf[STAT_LEN];
  char ab_path[MAX_PATH_LEN];
  char meta_path[MAX_PATH_LEN];
  char new_path[MAX_PATH_LEN];

  struct fuse_file_info fi;

#ifdef DEBUG
  sprintf(out_buf, "[%s] entry\n", __FUNCTION__);
  write(1, out_buf, strlen(out_buf));
#endif

  memset(stbuf, 0, sizeof(struct stat));

  dedupe_fs_filestore_path(ab_path, path);
  res = lstat(ab_path, stbuf);
  if(FAILED == res) {
    // Its okay to fail above as we can check if that file exists inside the dedupe database
    dedupe_fs_metadata_path(meta_path, path);
    memset(stbuf, 0, sizeof(struct stat));
    res = lstat(meta_path, stbuf);
    if(FAILED == res) {
      // File does not exist in our dedupe database;;
      // Oops something went wrong; return error
      res = -errno;
      return res;

    } else if (S_IFDIR == (stbuf->st_mode & S_IFDIR)) {
      // do nothing; already stbuf contains the required valid data

    } else {
      // Not a directory; should be a file

      strcpy(new_path, "/../..");
      strcat(new_path, meta_path);

      fi.flags = O_RDONLY;
      res = dedupe_fs_open(new_path, &fi);
      if(res < 0) {
      } else {
        memset(&stat_buf, 0, STAT_LEN);
        res = dedupe_fs_read(new_path, stat_buf, STAT_LEN, (off_t)0, &fi);
        if(res < 0) {
          // res contains the errno to return to libfuse
        } else {

          // process stat_buf to form the original data
          memset(stbuf, 0, sizeof(struct stat));

          sscanf(stat_buf, 
              "%llu:%llu:%u:%u:%u:%u:%llu:%lld:%lu:%lld:%lu:%lu:%lu\n",
              &stbuf->st_dev, 
              &stbuf->st_ino, 
              &stbuf->st_mode,
              &stbuf->st_nlink, 
              &stbuf->st_uid, 
              &stbuf->st_gid, 
              &stbuf->st_rdev, 
              &stbuf->st_size, 
              &stbuf->st_blksize, 
              &stbuf->st_blocks, 
              &stbuf->st_atime, 
              &stbuf->st_mtime, 
              &stbuf->st_ctime);
        }
        dedupe_fs_release(new_path, &fi);
      }
    }
  }

#ifdef DEBUG
  sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
  write(1, out_buf, strlen(out_buf));
#endif

  return 0;
}

static int dedupe_fs_opendir(
    const char *path, 
    struct fuse_file_info *fi) {
  int res = 0;
  char out_buf[BUF_LEN];
  char ab_path[MAX_PATH_LEN];

  DIR *dp;

#ifdef DEBUG
  sprintf(out_buf, "[%s] entry\n", __FUNCTION__);
  write(1, out_buf, strlen(out_buf));
#endif

  dedupe_fs_metadata_path(ab_path, path);

  // TODO: Add the getattr logic here for opendir and other system calls
  dp = opendir(ab_path);
  if(NULL == dp)
    res = -errno;

  fi->fh = (intptr_t)dp;

#ifdef DEBUG
  sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
  write(1, out_buf, strlen(out_buf));
#endif

  return res;
}

static int dedupe_fs_readdir(
    const char *path,
    void *buf, fuse_fill_dir_t filler, 
    off_t offset, 
    struct fuse_file_info *fi) {

  int res = 0;
  DIR *dp;
  struct dirent *de;

  char out_buf[BUF_LEN];

#ifdef DEBUG
  sprintf(out_buf, "[%s] entry\n", __FUNCTION__);
  write(1, out_buf, strlen(out_buf));
#endif

  dp = (DIR*) (uintptr_t)fi->fh;

  de = readdir(dp);
  if(de == NULL)  {
    res = -errno;
    return res;
  }

  do {
    //memset(&st, 0, sizeof(st));
    //dedupe_fs_getattr(de->d_name, &st);
    if(filler(buf, de->d_name, NULL, 0))
      res = -errno;
  } while((de = readdir(dp)) != NULL);

#ifdef DEBUG
  sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
  write(1, out_buf, strlen(out_buf));
#endif

  return res;
}

static int dedupe_fs_rename(const char *path, const char *newpath) {
  int res = 0;
  char out_buf[BUF_LEN];
  char ab_path[MAX_PATH_LEN];
  char new_ab_path[MAX_PATH_LEN];

#ifdef DEBUG
  sprintf(out_buf, "[%s] entry\n", __FUNCTION__);
  write(1, out_buf, strlen(out_buf));
#endif

  dedupe_fs_filestore_path(ab_path, path);
  dedupe_fs_filestore_path(new_ab_path, newpath);

  res = rename(ab_path, new_ab_path);
  if(FAILED == res)
    res = -errno;

#ifdef DEBUG
  sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
  write(1, out_buf, strlen(out_buf));
#endif

  return res;
}

static int dedupe_fs_symlink(const char *path, const char *link) {

  int res = 0;
  char out_buf[BUF_LEN];
  char ab_link[MAX_PATH_LEN];

#ifdef DEBUG
  sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
  write(1, out_buf, strlen(out_buf));
#endif

  dedupe_fs_filestore_path(ab_link, link);

  res = symlink(path, ab_link);
  if(FAILED == res)
    res = -errno;

#ifdef DEBUG
  sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
  write(1, out_buf, strlen(out_buf));
#endif

  return res;
}

static int dedupe_fs_link(const char *path, const char *newpath) {

  int res = 0;
  char out_buf[BUF_LEN];
  char ab_path[MAX_PATH_LEN];
  char new_ab_path[MAX_PATH_LEN];

#ifdef DEBUG
  sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
  write(1, out_buf, strlen(out_buf));
#endif

  dedupe_fs_filestore_path(ab_path, path);
  dedupe_fs_filestore_path(new_ab_path, newpath);

  res = link(path, new_ab_path);
  if(FAILED == res)
    res = -errno;

#ifdef DEBUG
  sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
  write(1, out_buf, strlen(out_buf));
#endif

  return res;
}

static int dedupe_fs_chmod(const char *path, mode_t mode) {

  int res = 0;
  char out_buf[BUF_LEN];
  char ab_path[MAX_PATH_LEN];

#ifdef DEBUG
  sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
  write(1, out_buf, strlen(out_buf));
#endif

  dedupe_fs_filestore_path(ab_path, path);

  res = chmod(ab_path, mode);
  if(FAILED == res)
    res = -errno;

#ifdef DEBUG
  sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
  write(1, out_buf, strlen(out_buf));
#endif

  return res;
}

static int dedupe_fs_chown(const char *path, uid_t uid, gid_t gid) {

  int res = 0;
  char out_buf[BUF_LEN];
  char ab_path[MAX_PATH_LEN];

#ifdef DEBUG
  sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
  write(1, out_buf, strlen(out_buf));
#endif

  dedupe_fs_filestore_path(ab_path, path);

  res = chown(ab_path, uid, gid);
  if(FAILED == res)
    res = -errno;

#ifdef DEBUG
  sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
  write(1, out_buf, strlen(out_buf));
#endif

  return res;
}

int dedupe_fs_unlink(const char *path)
{
  int res = 0;
  char out_buf[BUF_LEN];
  char ab_path[MAX_PATH_LEN];

#ifdef DEBUG
  sprintf(out_buf, "[%s] entry\n", __FUNCTION__);
  write(1, out_buf, strlen(out_buf));
#endif

  dedupe_fs_filestore_path(ab_path, path);

  res = unlink(ab_path);
  if (res < 0)
    res = -errno;

#ifdef DEBUG
  sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
  write(1, out_buf, strlen(out_buf));
#endif

  return res;
}

int dedupe_fs_rmdir(const char *path) {
  int res = 0;
  char out_buf[BUF_LEN];
  char ab_path[MAX_PATH_LEN];

#ifdef DEBUG
  sprintf(out_buf, "[%s] entry\n", __FUNCTION__);
  write(1, out_buf, strlen(out_buf));
#endif

  dedupe_fs_filestore_path(ab_path, path);

  res = rmdir(ab_path);
  if(FAILED == res)
    res = -errno;

#ifdef DEBUG
  sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
  write(1, out_buf, strlen(out_buf));
#endif

  return res;
}

static int dedupe_fs_access(const char *path, int mask) {

  int res = 0;
  char out_buf[BUF_LEN];
  char ab_path[MAX_PATH_LEN];

#ifdef DEBUG
  sprintf(out_buf, "[%s] entry\n", __FUNCTION__);
  write(1, out_buf, strlen(out_buf));
#endif

  dedupe_fs_filestore_path(ab_path, path);

  res = access(ab_path, mask);
  if(FAILED == res) {
    dedupe_fs_metadata_path(ab_path, path);
    res = access(ab_path, mask);
    if(FAILED == res)
      res = -errno;
  }

#ifdef DEBUG
  sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
  write(1, out_buf, strlen(out_buf));
#endif

  return res;
}

int dedupe_fs_mkdir(const char *path, mode_t mode) {

  int res = 0;
  char out_buf[BUF_LEN];
  char ab_path[MAX_PATH_LEN];

#ifdef DEBUG
  sprintf(out_buf, "[%s] entry\n", __FUNCTION__);
  write(1, out_buf, strlen(out_buf));
#endif

  dedupe_fs_filestore_path(ab_path, path);

  res = mkdir(ab_path, mode);
  if(FAILED == res)
    res = -errno;

#ifdef DEBUG
  sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
  write(1, out_buf, strlen(out_buf));
#endif

  return res;
}

int dedupe_fs_open(const char *path, struct fuse_file_info *fi) {

  int fd, ret = 0;
  char out_buf[BUF_LEN];
  char ab_path[MAX_PATH_LEN];
  struct fuse_context *mycontext = fuse_get_context();

  dedupe_globals *glob = (dedupe_globals *)mycontext->private_data;

#ifdef DEBUG
  sprintf(out_buf, "[%s] entry\n", __FUNCTION__);
  write(1, out_buf, strlen(out_buf));
#endif

  //print_fuse_file_info(fi);
  dedupe_fs_filestore_path(ab_path, path);

  // TODO - redesign lock mechanism
  //pthread_mutex_lock(&globals.lk);

  fd = open(ab_path, fi->flags);
  if(FAILED == fd) {
    //pthread_mutex_unlock(&globals.lk);
    return -errno;
  }

  fi->fh = fd;

  if(FAILED == flock(fi->fh, LOCK_EX)) {
    sprintf(out_buf, "[%s] LOCK on [%s] failed error [%d]\n",
        __FUNCTION__, path, errno);
    write(1, out_buf, strlen(out_buf));
  }

  fi->lock_owner = gettid();

  //pthread_mutex_unlock(&globals.lk);

#ifdef DEBUG
  sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
  write(1, out_buf, strlen(out_buf));
#endif

  return SUCCESS;
}

int dedupe_fs_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi) {

  int res = 0;
  char out_buf[BUF_LEN];

#ifdef DEBUG
  sprintf(out_buf, "[%s] entry\n", __FUNCTION__);
  write(1, out_buf, strlen(out_buf));
#endif

  res = pread(fi->fh, buf, size, offset);
  if(FAILED == res)
    res = -errno;


#ifdef DEBUG
  sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
  write(1, out_buf, strlen(out_buf));
#endif

  return res;
}

int dedupe_fs_write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi) {

  int res;
  char out_buf[BUF_LEN];

#ifdef DEBUG
  sprintf(out_buf, "[%s] entry\n", __FUNCTION__);
  write(1, out_buf, strlen(out_buf));
#endif

  res = pwrite(fi->fh, buf, size, offset);
  if(FAILED == res)
    res = -errno;

#ifdef DEBUG
  sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
  write(1, out_buf, strlen(out_buf));
#endif

  return res;
}

int dedupe_fs_release(const char *path, struct fuse_file_info *fi) {

  int res = 0;
  char out_buf[BUF_LEN];

#ifdef DEBUG
  sprintf(out_buf, "[%s] entry\n", __FUNCTION__);
  write(1, out_buf, strlen(out_buf));
#endif

  if(gettid() == fi->lock_owner) {
    if(FAILED == flock(fi->fh, LOCK_UN)) {
      sprintf(out_buf, "[%s] flock UNLOCK failed on [%s] errno [%d]\n", __FUNCTION__, path, errno);
      write(1, out_buf, strlen(out_buf));
      return -errno;
    }
    fi->lock_owner = 0;
  }

  res = close(fi->fh);

#ifdef DEBUG
  sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
  write(1, out_buf, strlen(out_buf));
#endif

  return res;
}

static int dedupe_fs_releasedir(const char *path, struct fuse_file_info *fi) {
  char out_buf[BUF_LEN];

#ifdef DEBUG
  sprintf(out_buf, "[%s] entry\n", __FUNCTION__);
  write(1, out_buf, strlen(out_buf));
#endif

  closedir((DIR *) (uintptr_t) fi->fh);

#ifdef DEBUG
  sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
  write(1, out_buf, strlen(out_buf));
#endif

  return SUCCESS;

}

static int dedupe_fs_truncate(const char *path, off_t newsize) {
  int res = 0;
  char out_buf[BUF_LEN];
  char ab_path[MAX_PATH_LEN];

#ifdef DEBUG
  sprintf(out_buf, "[%s] entry\n", __FUNCTION__);
  write(1, out_buf, strlen(out_buf));
#endif

  dedupe_fs_filestore_path(ab_path, path);

  res = truncate(ab_path, newsize);
  if(FAILED == res)
    res = -errno;

#ifdef DEBUG
  sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
  write(1, out_buf, strlen(out_buf));
#endif

  return res;
}

static int dedupe_fs_utime(const char *path, struct utimbuf *ubuf) {
  int res = 0;
  char out_buf[BUF_LEN];
  char ab_path[MAX_PATH_LEN];

#ifdef DEBUG
  sprintf(out_buf, "[%s] entry\n", __FUNCTION__);
  write(1, out_buf, strlen(out_buf));
#endif

  dedupe_fs_filestore_path(ab_path, path);

  res = utime(ab_path, ubuf);
  if(FAILED == res)
    res = -errno;

#ifdef DEBUG
  sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
  write(1, out_buf, strlen(out_buf));
#endif

  return res;
}

static int dedupe_fs_create(const char *path, mode_t mode, struct fuse_file_info *fi) {

  int fd = 0;
  char out_buf[BUF_LEN];
  char ab_path[MAX_PATH_LEN];

#ifdef DEBUG
  sprintf(out_buf, "[%s] entry\n", __FUNCTION__);
  write(1, out_buf, strlen(out_buf));
#endif

  dedupe_fs_filestore_path(ab_path, path);
  
  fd = creat(ab_path, mode);
  if(FAILED == fd) {
    sprintf("File creation failed for %s errno:%d\n", ab_path, errno);
    write(1, out_buf, strlen(out_buf));
    return -errno;
  }

  fi->fh = fd;

#ifdef DEBUG
  sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
  write(1, out_buf, strlen(out_buf));
#endif

  return SUCCESS;
}

int dedupe_fs_mknod(const char *path, mode_t mode, dev_t rdev) {
  int res = 0;
  char out_buf[BUF_LEN];
  char ab_path[MAX_PATH_LEN];
  
#ifdef DEBUG
  sprintf(out_buf, "[%s] entry\n", __FUNCTION__);
  write(1, out_buf, strlen(out_buf));
#endif

  dedupe_fs_filestore_path(ab_path, path);

  if (S_ISREG(mode)) {
    res = open(ab_path, O_CREAT | O_EXCL | O_WRONLY, mode);
    if(res >= 0) {
      res = close(res);
    }
  } else if (S_ISFIFO(mode)) {
    res = mkfifo(ab_path, mode);
  } else {
    res = mknod(ab_path, mode, rdev);
  }

#ifdef DEBUG
  sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
  write(1, out_buf, strlen(out_buf));
#endif

  if(FAILED == res)
    return -errno;

  return res;
}

void * dedupe_fs_init(struct fuse_conn_info *conn) {
  int res = 0;
  char out_buf[BUF_LEN];

  struct fuse_context *mycontext = fuse_get_context();
  
#ifdef DEBUG
  sprintf(out_buf, "[%s] entry\n", __FUNCTION__);
  write(1, out_buf, strlen(out_buf));
#endif

  pthread_mutex_init(&globals.lk, NULL);
  pthread_attr_init(&globals.thr_attr);

  res = pthread_attr_setdetachstate(&globals.thr_attr, PTHREAD_CREATE_DETACHED);
  if(res < SUCCESS) {
    sprintf(out_buf, "[%s] unable to set attr to PTHREAD_CREATE_DETACHED\n", __FUNCTION__);
    write(1, out_buf, strlen(out_buf));
  }

  if(FAILED == pthread_create(&globals.thr_handle, &globals.thr_attr, lazy_worker_thread, &globals.thr_arg)) {
    sprintf(out_buf, "[%s] lazy worker creation failed\n", __FUNCTION__);
    write(1, out_buf, strlen(out_buf));
    perror("");
  }

  pthread_attr_destroy(&globals.thr_attr);

#ifdef DEBUG
  sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
  write(1, out_buf, strlen(out_buf));
#endif

  return (mycontext->private_data);

}

void dedupe_fs_destroy(void *private_data) {
  char out_buf[BUF_LEN];

#ifdef DEBUG
  sprintf(out_buf, "[%s] entry\n", __FUNCTION__);
  write(1, out_buf, strlen(out_buf));
#endif

  pthread_mutex_destroy(&globals.lk);

#ifdef DEBUG
  sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
  write(1, out_buf, strlen(out_buf));
#endif

}

static struct fuse_operations dedupe_fs_oper = {
  .init       = dedupe_fs_init,
  .destroy    = dedupe_fs_destroy,
  .open       = dedupe_fs_open,
  .release    = dedupe_fs_release,
  .getattr    = dedupe_fs_getattr,
  .access     = dedupe_fs_access,
  .read       = dedupe_fs_read,
  .write      = dedupe_fs_write,
  .chmod      = dedupe_fs_chmod,
  .chown      = dedupe_fs_chown,
  .rename     = dedupe_fs_rename,
  .link       = dedupe_fs_link,
  .symlink    = dedupe_fs_symlink,
  .truncate   = dedupe_fs_truncate,
  .utime      = dedupe_fs_utime,
  .mknod      = dedupe_fs_mknod,
  .create     = dedupe_fs_create,
  .unlink     = dedupe_fs_unlink,
  .opendir    = dedupe_fs_opendir,
  .mkdir      = dedupe_fs_mkdir,
  .rmdir      = dedupe_fs_rmdir,
  .readdir    = dedupe_fs_readdir,
  .releasedir = dedupe_fs_releasedir,
};

int main(int argc, char **argv) {

  if(argc < 2) {
    usage();
  }

  return fuse_main(argc, argv, &dedupe_fs_oper, NULL);
}
