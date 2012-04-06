#include "dedupe_fs.h"
#include "internal_cmds.h"

int internal_create(const char *path, mode_t mode, struct fuse_file_info *fi) {

  int fd = 0;
  char out_buf[BUF_LEN];

#ifdef DEBUG
  sprintf(out_buf, "[%s] entry\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  fd = creat(path, mode);
  if(FAILED == fd) {
    sprintf(out_buf, "[%s] creat failed for [%s]", __FUNCTION__, path);
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


int internal_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi) {

  int res = 0;
  char out_buf[BUF_LEN] = {0};

#ifdef DEBUG
  sprintf(out_buf, "[%s] entry\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  dedupe_fs_lock(path, fi->fh);
  res = pread(fi->fh, buf, size, offset);
  if(FAILED == res) {
    sprintf(out_buf, "[%s] pread failed", __FUNCTION__);
    perror(out_buf);
    res = -errno;
  }
  dedupe_fs_unlock(path, fi->fh);

#ifdef DEBUG
  sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  return res;
}

int internal_opendir(const char *path, struct fuse_file_info *fi) {

  int res = 0;
  char out_buf[BUF_LEN] = {0};

  DIR *dp;

#ifdef DEBUG
  sprintf(out_buf, "[%s] entry\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  dp = opendir(path);

  if(NULL == dp) {
    res = -errno;
    return res;
  }

  fi->fh = (intptr_t)dp;

#ifdef DEBUG
  sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  return res;
}

int internal_readdir(const char *path, void *buf,
    fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi) {

  int res = 0;

  DIR *dp;
  struct dirent *de;

  char out_buf[BUF_LEN] = {0};

#ifdef DEBUG
  sprintf(out_buf, "[%s] entry\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  dp = (DIR*) (uintptr_t)fi->fh;

  de = readdir(dp);
  if(de == NULL)  {
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

int internal_open(const char *path, struct fuse_file_info *fi) {

  int fd = 0, res = 0;
  char out_buf[BUF_LEN] = {0};

#ifdef DEBUG
  sprintf(out_buf, "[%s] entry\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  fd = open(path, fi->flags);
  if(FAILED == fd) {
    res = -errno;
    return res;
  }

  fi->fh = fd;

  /*if(FAILED == flock(fi->fh, LOCK_EX)) {
    sprintf(out_buf, "[%s] LOCK on [%s] failed error [%d]\n",
        __FUNCTION__, path, errno);
    WR_2_STDOUT;
  }

  fi->lock_owner = gettid();*/

#ifdef DEBUG
  sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  return SUCCESS;
}

int internal_getattr(const char *path, struct stat *stbuf) {

  int res = 0;
  char out_buf[BUF_LEN] = {0};

#ifdef DEBUG
  sprintf(out_buf, "[%s] entry\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  memset(stbuf, 0, sizeof(struct stat));

  res = lstat(path, stbuf);
  if(FAILED == res) {
    res = -errno;
    return res;
  }

#ifdef DEBUG
  sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  return 0;
}

int internal_mkdir(const char *path, mode_t mode) {

  int res = 0;
  char out_buf[BUF_LEN] = {0};

#ifdef DEBUG
  sprintf(out_buf, "[%s] entry\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  res = mkdir(path, mode);
  if(FAILED == res)
    res = -errno;

#ifdef DEBUG
  sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  return res;
}

int internal_rmdir(const char *path) {

  int res = 0;
  char out_buf[BUF_LEN] = {0};

#ifdef DEBUG
  sprintf(out_buf, "[%s] entry\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  res = rmdir(path);
  if(FAILED == res)
    res = -errno;

#ifdef DEBUG
  sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  return res;
}

int internal_mknod(const char *path, mode_t mode, dev_t rdev) {
  int res = 0;
  char out_buf[BUF_LEN] = {0};
  
#ifdef DEBUG
  sprintf(out_buf, "[%s] entry\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  if (S_ISREG(mode)) {
    res = open(path, O_CREAT | O_EXCL | O_WRONLY, mode);
    if(res >= 0) {
      res = close(res);
    }
  } else if (S_ISFIFO(mode)) {
    res = mkfifo(path, mode);
  } else {
    res = mknod(path, mode, rdev);
  }

#ifdef DEBUG
  sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  if(FAILED == res)
    return -errno;

  return res;
}

int internal_write(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi) {

  int res;
  char out_buf[BUF_LEN] = {0};

#ifdef DEBUG
  sprintf(out_buf, "[%s] entry\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  dedupe_fs_lock(path, fi->fh);
  res = pwrite(fi->fh, buf, size, offset);
  if(FAILED == res)
    res = -errno;
  dedupe_fs_unlock(path, fi->fh);

#ifdef DEBUG
  sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  return res;
}

int internal_release(const char *path, struct fuse_file_info *fi) {
  int res = 0;
  char out_buf[BUF_LEN] = {0};

#ifdef DEBUG
  sprintf(out_buf, "[%s] entry\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  /*if(gettid() == fi->lock_owner) {
    fi->lock_owner = 0;
    if(FAILED == flock(fi->fh, LOCK_UN)) {
      sprintf(out_buf, "[%s] flock UNLOCK failed on [%s] errno [%d]\n", __FUNCTION__, path, errno);
      WR_2_STDOUT;
      return -errno;
    }
  }*/

  res = close(fi->fh);

#ifdef DEBUG
  sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  return res;
}

int internal_unlink(const char *path) {
  int res = 0;
  char out_buf[BUF_LEN] = {0};

#ifdef DEBUG
  sprintf(out_buf, "[%s] entry\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  res = unlink(path);
  if (res < 0)
    res = -errno;

#ifdef DEBUG
  sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  return res;
}

int internal_releasedir(const char *path, struct fuse_file_info *fi) {

  char out_buf[BUF_LEN] = {0};

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
