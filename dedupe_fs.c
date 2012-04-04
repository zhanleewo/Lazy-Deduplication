//gcc -Wall `pkg-config fuse --cflags --libs` dedupe_fs.c -o dedupe_fs


#define FUSE_USE_VERSION 26

#include "dedupe_fs.h"
#include "rabin-karp.h"

char *dedupe_file_store = "/tmp/dedupe_file_store";
char *dedupe_metadata = "/tmp/dedupe_metadata";
char *dedupe_hashes = "/tmp/dedupe_hashes";
char *nlinks = "nlinks.txt";

dedupe_globals globals;

extern void *lazy_worker_thread(void *);

static void usage() {
  char out_buf[BUF_LEN];

  sprintf(out_buf, "dedupe_fs [MOUNT_POINT]\n");
  WR_2_STDOUT;

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

static int dedupe_fs_getattr(const char *path, struct stat *stbuf) {

  int res = 0;

  char out_buf[BUF_LEN] = {0};
  char stat_buf[STAT_LEN] = {0};
  char ab_path[MAX_PATH_LEN] = {0};
  char meta_path[MAX_PATH_LEN] = {0};
  char new_path[MAX_PATH_LEN] = {0};

  struct fuse_file_info fi;

#ifdef DEBUG
  sprintf(out_buf, "[%s] entry\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  memset(stbuf, 0, sizeof(struct stat));

  dedupe_fs_filestore_path(ab_path, path);
  res = lstat(ab_path, stbuf);
  if(FAILED == res) {
    // Its okay to fail above as we can check 
    // if that file exists inside the dedupe database
    dedupe_fs_metadata_path(meta_path, path);
    memset(stbuf, 0, sizeof(struct stat));
    res = lstat(meta_path, stbuf);
    if(FAILED == res) {
      // File does not exist in our dedupe database;;
      // Oops something went wrong; return error
      sprintf(out_buf, "[%s] lstat failed for [%s]", __FUNCTION__, meta_path);
      perror(out_buf);
      res = -errno;
      return res;

    } else if (S_IFDIR == (stbuf->st_mode & S_IFDIR)) {
      // do nothing; already stbuf contains the required valid data

    } else {
      // Not a directory; should be a file
      fi.flags = O_RDONLY;
      res = internal_open(meta_path, &fi);
      if(res < 0) {
      } else {
        memset(&stat_buf, 0, STAT_LEN);
        res = internal_read(meta_path, stat_buf, STAT_LEN, (off_t)0, &fi);
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
        internal_release(meta_path, &fi);
      }
    }
  }

#ifdef DEBUG
  sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  return 0;
}

static int dedupe_fs_opendir(
    const char *path, 
    struct fuse_file_info *fi) {

  int res = 0;

  char out_buf[BUF_LEN] = {0};
  char ab_path[MAX_PATH_LEN] = {0};
  char meta_path[MAX_PATH_LEN] = {0};

  DIR *dp;

#ifdef DEBUG
  sprintf(out_buf, "[%s] entry\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  dedupe_fs_metadata_path(meta_path, path);

  dp = opendir(meta_path);

  if(NULL == dp) {

    dedupe_fs_filestore_path(ab_path, path);

    dp = opendir(ab_path);
    if(NULL == dp) {
      sprintf(out_buf, "[%s] opendir failed on [%s]", __FUNCTION__, ab_path);
      perror(out_buf);
      res = -errno;
      return res;
    }
  }

  fi->fh = (intptr_t)dp;

#ifdef DEBUG
  sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
  WR_2_STDOUT;
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
  WR_2_STDOUT;
#endif

  dp = (DIR*) (uintptr_t)fi->fh;

  de = readdir(dp);
  if(de == NULL)  {
    sprintf(out_buf, "[%s] readdir failed", __FUNCTION__);
    perror(out_buf);
    res = -errno;
    return res;
  }

  do {
    if(filler(buf, de->d_name, NULL, 0))
      res = -errno;
  } while((de = readdir(dp)) != NULL);

#ifdef DEBUG
  sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  return res;
}

static int dedupe_fs_rename(const char *path, const char *newpath) {

  int res = 0;

  char out_buf[BUF_LEN] = {0};
  char ab_path[MAX_PATH_LEN] = {0};
  char new_ab_path[MAX_PATH_LEN] = {0};
  char meta_path[MAX_PATH_LEN] = {0};
  char new_meta_path[MAX_PATH_LEN] = {0};

#ifdef DEBUG
  sprintf(out_buf, "[%s] entry\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  dedupe_fs_metadata_path(meta_path, path);
  dedupe_fs_metadata_path(new_meta_path, newpath);

  res = rename(meta_path, new_meta_path);
  if(FAILED == res) {
    dedupe_fs_filestore_path(ab_path, path);
    dedupe_fs_filestore_path(new_ab_path, path);

    res = rename(ab_path, new_ab_path);
    if(FAILED == res) {
      sprintf(out_buf, "[%s] rename failed from [%s] to [%s]", __FUNCTION__, ab_path, new_ab_path);
      perror(out_buf);
      res = -errno;
      return res;
    }
  }

#ifdef DEBUG
  sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  return res;
}

static int dedupe_fs_symlink(const char *path, const char *link) {

  int res = 0;
  char out_buf[BUF_LEN] = {0};
  char ab_link[MAX_PATH_LEN] = {0};

#ifdef DEBUG
  sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  dedupe_fs_filestore_path(ab_link, link);

  res = symlink(path, ab_link);
  if(FAILED == res) {
    sprintf(out_buf, "[%s] symlink failed on [%s]", __FUNCTION__, path);
    perror(out_buf);
    res = -errno;
  }

#ifdef DEBUG
  sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  return res;
}

static int dedupe_fs_link(const char *path, const char *newpath) {

  int res = 0;
  char out_buf[BUF_LEN] = {0};
  char ab_path[MAX_PATH_LEN] = {0};
  char new_ab_path[MAX_PATH_LEN] = {0};

#ifdef DEBUG
  sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  dedupe_fs_filestore_path(ab_path, path);
  dedupe_fs_filestore_path(new_ab_path, newpath);

  res = link(path, new_ab_path);
  if(FAILED == res) {
    sprintf(out_buf, "[%s] link failed from [%s] to [%s]", __FUNCTION__, path, new_ab_path);
    perror(out_buf);
    res = -errno;
  }

#ifdef DEBUG
  sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  return res;
}

static int dedupe_fs_chmod(const char *path, mode_t mode) {

  int res = 0;

  char out_buf[BUF_LEN] = {0};
  char ab_path[MAX_PATH_LEN] = {0};

#ifdef DEBUG
  sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  dedupe_fs_filestore_path(ab_path, path);

  res = chmod(ab_path, mode);
  if(FAILED == res) {
    sprintf(out_buf, "[%s] chmod failed on [%s]", __FUNCTION__, ab_path);
    perror(out_buf);
    res = -errno;
  }

#ifdef DEBUG
  sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  return res;
}

static int dedupe_fs_chown(const char *path, uid_t uid, gid_t gid) {

  int res = 0;

  char out_buf[BUF_LEN] = {0};
  char ab_path[MAX_PATH_LEN] = {0};

#ifdef DEBUG
  sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  dedupe_fs_filestore_path(ab_path, path);

  res = chown(ab_path, uid, gid);
  if(FAILED == res) {
    sprintf(out_buf, "[%s] chown failed on [%s]", __FUNCTION__, ab_path);
    perror(out_buf);
    res = -errno;
  }

#ifdef DEBUG
  sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  return res;
}

static int dedupe_fs_unlink(const char *path) {

  int res = 0;
  char out_buf[BUF_LEN] = {0};
  char ab_path[MAX_PATH_LEN] = {0};

#ifdef DEBUG
  sprintf(out_buf, "[%s] entry\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  dedupe_fs_filestore_path(ab_path, path);

  res = unlink(ab_path);
  if (res < 0) {
    sprintf(out_buf, "[%s] unlink failed on [%s]", __FUNCTION__, ab_path);
    perror(out_buf);
    res = -errno;
  }

#ifdef DEBUG
  sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  return res;
}

static int dedupe_fs_rmdir(const char *path) {

  int res = 0;

  char out_buf[BUF_LEN] = {0};
  char ab_path[MAX_PATH_LEN] = {0};

#ifdef DEBUG
  sprintf(out_buf, "[%s] entry\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  dedupe_fs_filestore_path(ab_path, path);

  res = rmdir(ab_path);
  if(FAILED == res) {
    sprintf(out_buf, "[%s] rmdir failed on [%s]", __FUNCTION__, ab_path);
    perror(out_buf);
    res = -errno;
  }

#ifdef DEBUG
  sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  return res;
}

static int dedupe_fs_access(const char *path, int mask) {

  int res = 0;

  char out_buf[BUF_LEN] = {0};
  char ab_path[MAX_PATH_LEN] = {0};
  char meta_path[MAX_PATH_LEN] = {0};

#ifdef DEBUG
  sprintf(out_buf, "[%s] entry\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  dedupe_fs_metadata_path(meta_path, path);

  res = access(meta_path, mask);
  if(FAILED == res) {
    dedupe_fs_filestore_path(ab_path, path);
    res = access(ab_path, mask);
    if(FAILED == res) {
      sprintf(out_buf, "[%s] access failed on [%s]", __FUNCTION__, ab_path);
      perror(out_buf);
      res = -errno;
      return res;
    }
  }

#ifdef DEBUG
  sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  return res;
}

static int dedupe_fs_mkdir(const char *path, mode_t mode) {

  int res = 0;
  char out_buf[BUF_LEN];
  char ab_path[MAX_PATH_LEN];

#ifdef DEBUG
  sprintf(out_buf, "[%s] entry\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  dedupe_fs_filestore_path(ab_path, path);

  res = mkdir(ab_path, mode);
  if(FAILED == res) {
    sprintf(out_buf, "[%s] mkdir failed on [%s]", __FUNCTION__, ab_path);
    perror(out_buf);
    res = -errno;
  }

#ifdef DEBUG
  sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  return res;
}

static int dedupe_fs_open(const char *path, struct fuse_file_info *fi) {

  int fd, ret = 0;
  char out_buf[BUF_LEN] = {0};
  char ab_path[MAX_PATH_LEN] = {0};
  char meta_path[MAX_PATH_LEN] = {0};

  struct fuse_context *mycontext = fuse_get_context();

  dedupe_globals *glob = (dedupe_globals *)mycontext->private_data;

#ifdef DEBUG
  sprintf(out_buf, "[%s] entry\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  dedupe_fs_metadata_path(meta_path, path);

  fd = open(meta_path, fi->flags);
  if(FAILED == fd) {

    dedupe_fs_filestore_path(ab_path, path);

    fd = open(ab_path, fi->flags);
    if(FAILED == fd) {
      sprintf(out_buf, "[%s] open failed on [%s]", __FUNCTION__, ab_path);
      perror(out_buf);
      return -errno;
    }
  }

  fi->fh = fd;

  if(FAILED == flock(fi->fh, LOCK_EX)) {
    sprintf(out_buf, "[%s] flock lock failed on [%s]", __FUNCTION__, ab_path);
    perror(out_buf);
  }

  fi->lock_owner = gettid();

#ifdef DEBUG
  sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  return SUCCESS;
}

static int dedupe_fs_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi) {

  int res = 0;

  off_t hash_off = STAT_LEN;
  struct stat stbuf;
  char out_buf[BUF_LEN] = {0};
  char ab_path[MAX_PATH_LEN] = {0};
  char meta_path[MAX_PATH_LEN] = {0};
  char stat_buf[STAT_LEN] = {0};

  char hash_line[OFF_HASH_LEN] = {0};

#ifdef DEBUG
  sprintf(out_buf, "[%s] entry\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  dedupe_fs_metadata_path(meta_path, path);
  res = lstat(meta_path, &stbuf);
  if(FAILED == res) {
    dedupe_fs_filestore_path(ab_path, path);
    res = pread(fi->fh, buf, size, offset);
    if(FAILED == res) {
      sprintf(out_buf, "[%s] pread failed on [%s]", __FUNCTION__, ab_path);
      perror(out_buf);
      res = -errno;
    }
    return res;
  }

  res = pread(fi->fh, stat_buf, STAT_LEN, 0);
  if(FAILED == res) {
    sprintf(out_buf, "[%s] pread failed on [%s]", __FUNCTION__, meta_path);
    perror(out_buf);
    exit(1);
  }

  memset(&stbuf, 0, sizeof(struct stat));
  sscanf(stat_buf, 
      "%llu:%llu:%u:%u:%u:%u:%llu:%lld:%lu:%lld:%lu:%lu:%lu\n",
      &stbuf.st_dev, 
      &stbuf.st_ino, 
      &stbuf.st_mode,
      &stbuf.st_nlink, 
      &stbuf.st_uid, 
      &stbuf.st_gid, 
      &stbuf.st_rdev, 
      &stbuf.st_size, 
      &stbuf.st_blksize, 
      &stbuf.st_blocks, 
      &stbuf.st_atime, 
      &stbuf.st_mtime, 
      &stbuf.st_ctime);

  if(offset + size > stbuf.st_size) {
    sprintf(out_buf, "[%s] filesize [%d] < request [%d]", __FUNCTION__, stbuf.st_size, offset+size);
    perror(out_buf);
    return -EINVAL;
  }

  while(TRUE) {

    memset(hash_line, 0, OFF_HASH_LEN);
    res = pread(fi->fh, hash_line, OFF_HASH_LEN, hash_off);
    if(FAILED == res) {
      sprintf(out_buf, "[%s] pread failed on [%s]", __FUNCTION__, meta_path);
      perror(out_buf);
      res = -errno;
      return res;
    } else if (0 == res) {
      sprintf(out_buf, "[%s] EOF reached on [%s]", __FUNCTION__, meta_path);
      perror(out_buf);
      return res;
    }

  }

#ifdef DEBUG
  sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  return res;
}

static int dedupe_fs_write(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi) {

  int res;
  char out_buf[BUF_LEN];

#ifdef DEBUG
  sprintf(out_buf, "[%s] entry\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  res = pwrite(fi->fh, buf, size, offset);
  if(FAILED == res) {
    sprintf(out_buf, "[%s] pwrite failed on [%s]", __FUNCTION__, path);
    perror(out_buf);
    res = -errno;
  }

#ifdef DEBUG
  sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  return res;
}

static int dedupe_fs_release(const char *path, struct fuse_file_info *fi) {

  int res = 0;
  char out_buf[BUF_LEN];

#ifdef DEBUG
  sprintf(out_buf, "[%s] entry\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  if(gettid() == fi->lock_owner) {
    fi->lock_owner = 0;
    if(FAILED == flock(fi->fh, LOCK_UN)) {
      sprintf(out_buf, "[%s] flock unlock failed on [%s]", __FUNCTION__, path);
      perror(out_buf);
      return -errno;
    }
  }

  res = close(fi->fh);

#ifdef DEBUG
  sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  return res;
}

static int dedupe_fs_releasedir(const char *path, struct fuse_file_info *fi) {
  char out_buf[BUF_LEN];

#ifdef DEBUG
  sprintf(out_buf, "[%s] entry\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  closedir((DIR *) (uintptr_t) fi->fh);

#ifdef DEBUG
  sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  return SUCCESS;

}

static int dedupe_fs_truncate(const char *path, off_t newsize) {
  int res = 0;
  char out_buf[BUF_LEN];
  char ab_path[MAX_PATH_LEN];

#ifdef DEBUG
  sprintf(out_buf, "[%s] entry\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  dedupe_fs_filestore_path(ab_path, path);

  res = truncate(ab_path, newsize);
  if(FAILED == res) {
    sprintf(out_buf, "[%s] truncate failed on [%s]", __FUNCTION__, ab_path);
    perror(out_buf);
    res = -errno;
  }

#ifdef DEBUG
  sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  return res;
}

static int dedupe_fs_utime(const char *path, struct utimbuf *ubuf) {
  int res = 0;
  char out_buf[BUF_LEN];
  char ab_path[MAX_PATH_LEN];

#ifdef DEBUG
  sprintf(out_buf, "[%s] entry\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  dedupe_fs_filestore_path(ab_path, path);

  res = utime(ab_path, ubuf);
  if(FAILED == res)
    sprintf(out_buf, "[%s] utime failed on [%s]", __FUNCTION__, ab_path);
    perror(out_buf);
    res = -errno;

#ifdef DEBUG
  sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  return res;
}

static int dedupe_fs_create(const char *path, mode_t mode, struct fuse_file_info *fi) {

  int fd = 0;
  char out_buf[BUF_LEN];
  char ab_path[MAX_PATH_LEN];

#ifdef DEBUG
  sprintf(out_buf, "[%s] entry\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  dedupe_fs_filestore_path(ab_path, path);
  
  fd = creat(ab_path, mode);
  if(FAILED == fd) {
    sprintf(out_buf, "[%s] creat failed on [%s]", __FUNCTION__, ab_path);
    perror(out_buf);
    return -errno;
  }

  fi->fh = fd;

#ifdef DEBUG
  sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  return SUCCESS;
}

static int dedupe_fs_mknod(const char *path, mode_t mode, dev_t rdev) {
  int res = 0;
  char out_buf[BUF_LEN];
  char ab_path[MAX_PATH_LEN];
  
#ifdef DEBUG
  sprintf(out_buf, "[%s] entry\n", __FUNCTION__);
  WR_2_STDOUT;
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

  if(FAILED == res) {
    sprintf(out_buf, "[%s] mknod failed on [%s]", __FUNCTION__, ab_path);
    perror(out_buf);
    return -errno;
  }

#ifdef DEBUG
  sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  return res;
}

static void * dedupe_fs_init(struct fuse_conn_info *conn) {
  int res = 0;
  char out_buf[BUF_LEN];

  struct fuse_context *mycontext = fuse_get_context();
  
#ifdef DEBUG
  sprintf(out_buf, "[%s] entry\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  pthread_mutex_init(&globals.lk, NULL);
  pthread_attr_init(&globals.thr_attr);

  res = pthread_attr_setdetachstate(&globals.thr_attr, PTHREAD_CREATE_DETACHED);
  if(res < SUCCESS) {
    sprintf(out_buf, "[%s] pthread_attr_setdetachstate failed to PTHREAD_CREATE_DETACHED\n", __FUNCTION__);
    perror(out_buf);
  }

  if(FAILED == pthread_create(&globals.thr_handle, &globals.thr_attr, lazy_worker_thread, &globals.thr_arg)) {
    sprintf(out_buf, "[%s] pthread_create failed for lazy_worker_thread", __FUNCTION__);
    perror(out_buf);
  }

  pthread_attr_destroy(&globals.thr_attr);

#ifdef DEBUG
  sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  return (mycontext->private_data);

}

static void dedupe_fs_destroy(void *private_data) {

  char out_buf[BUF_LEN] = {0};

#ifdef DEBUG
  sprintf(out_buf, "[%s] entry\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  pthread_mutex_destroy(&globals.lk);

#ifdef DEBUG
  sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
  WR_2_STDOUT;
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
