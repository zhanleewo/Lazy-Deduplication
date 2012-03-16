//gcc -Wall `pkg-config fuse --cflags --libs` dedupe_fs.c -o dedupe_fs


#define FUSE_USE_VERSION 26

#include <sys/time.h>

#include "dedupe_fs.h"

char *dedupe_ini_file_store = "/tmp/dedupe_file_store";
char *dedupe_metadata = "/tmp/dedupe_metadata";
char *dedupe_hashes = "/tmp/dedupe_hashes";

extern void *lazy_worker_thread(void *);

static void usage() {
  char out_buf[BUF_LEN];

  sprintf(out_buf, "dedupe_fs [MOUNT_POINT]\n");
  write(1, out_buf, strlen(out_buf));

  exit(1);
}

#if 0
static void print_fuse_file_info(struct fuse_file_info *fi) {
  if(NULL != fi) {
    printf("flags:%d fh_old:%lu ", fi->flags, fi->fh_old);
    printf("writepage:%d direct_io:%u ", fi->writepage, fi->direct_io);
    printf("keep_cache:%u flush:%u ", fi->keep_cache, fi->flush);
    printf("nonseekable:%u padding:%u ", fi->nonseekable, fi->padding);
    printf("fh:%llu lock_owner:%llu\n", fi->fh, fi->lock_owner);
  }
}
#endif

void dedupe_fs_fullpath(char ab_path[MAX_PATH_LEN], const char *path) {

  memset(ab_path, 0, MAX_PATH_LEN);
  strcpy(ab_path, dedupe_ini_file_store);
  strcat(ab_path, path);

  return;
}

int dedupe_fs_getattr(const char *path, struct stat *stbuf) {

  int res = 0;
  char out_buf[BUF_LEN];
  char ab_path[MAX_PATH_LEN];

#ifdef DEBUG
  sprintf(out_buf, "[%s] entry\n", __FUNCTION__);
  write(1, out_buf, strlen(out_buf));
#endif

  dedupe_fs_fullpath(ab_path, path);

  memset(stbuf, 0, sizeof(struct stat));

  res = lstat(ab_path, stbuf);
  if(FAILED == res)
    res = -errno;

#ifdef DEBUG
  sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
  write(1, out_buf, strlen(out_buf));
#endif

  return res;
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

  dedupe_fs_fullpath(ab_path, path);

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

  dedupe_fs_fullpath(ab_path, path);
  dedupe_fs_fullpath(new_ab_path, newpath);

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

  dedupe_fs_fullpath(ab_link, link);

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

  dedupe_fs_fullpath(ab_path, path);
  dedupe_fs_fullpath(new_ab_path, newpath);

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

  dedupe_fs_fullpath(ab_path, path);

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

  dedupe_fs_fullpath(ab_path, path);

  res = chown(ab_path, uid, gid);
  if(FAILED == res)
    res = -errno;

#ifdef DEBUG
  sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
  write(1, out_buf, strlen(out_buf));
#endif

  return res;
}

static int dedupe_fs_unlink(const char *path)
{
  int res = 0;
  char out_buf[BUF_LEN];
  char ab_path[MAX_PATH_LEN];

#ifdef DEBUG
  sprintf(out_buf, "[%s] entry\n", __FUNCTION__);
  write(1, out_buf, strlen(out_buf));
#endif

  dedupe_fs_fullpath(ab_path, path);

  res = unlink(ab_path);
  if (res < 0)
    res = -errno;

#ifdef DEBUG
  sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
  write(1, out_buf, strlen(out_buf));
#endif

  return res;
}

static int dedupe_fs_rmdir(const char *path) {
  int res = 0;
  char out_buf[BUF_LEN];
  char ab_path[MAX_PATH_LEN];

#ifdef DEBUG
  sprintf(out_buf, "[%s] entry\n", __FUNCTION__);
  write(1, out_buf, strlen(out_buf));
#endif

  dedupe_fs_fullpath(ab_path, path);

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

  dedupe_fs_fullpath(ab_path, path);

  res = access(ab_path, mask);
  if(FAILED == res)
    res = -errno;

#ifdef DEBUG
  sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
  write(1, out_buf, strlen(out_buf));
#endif

  return res;
}

static int dedupe_fs_mkdir(const char *path, mode_t mode) {

  int res = 0;
  char out_buf[BUF_LEN];
  char ab_path[MAX_PATH_LEN];

#ifdef DEBUG
  sprintf(out_buf, "[%s] entry\n", __FUNCTION__);
  write(1, out_buf, strlen(out_buf));
#endif

  dedupe_fs_fullpath(ab_path, path);

  res = mkdir(ab_path, mode);
  if(FAILED == res)
    return -errno;

#ifdef DEBUG
  sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
  write(1, out_buf, strlen(out_buf));
#endif

  return res;
}

static int dedupe_fs_open(const char *path, struct fuse_file_info *fi) {
  int fd;
  char out_buf[BUF_LEN];
  char ab_path[MAX_PATH_LEN];

#ifdef DEBUG
  sprintf(out_buf, "[%s] entry\n", __FUNCTION__);
  write(1, out_buf, strlen(out_buf));
#endif

  //print_fuse_file_info(fi);
  dedupe_fs_fullpath(ab_path, path);

  fd = open(ab_path, fi->flags);
  if(FAILED == fd)
    return -errno;

  fi->fh = fd;

#ifdef DEBUG
  sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
  write(1, out_buf, strlen(out_buf));
#endif

  return SUCCESS;
}

static int dedupe_fs_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi) {

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

static int dedupe_fs_write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi) {

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

static int dedupe_fs_release(const char *path, struct fuse_file_info *fi) {

  int res = 0;
  char out_buf[BUF_LEN];

#ifdef DEBUG
  sprintf(out_buf, "[%s] entry\n", __FUNCTION__);
  write(1, out_buf, strlen(out_buf));
#endif

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

  dedupe_fs_fullpath(ab_path, path);

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

  dedupe_fs_fullpath(ab_path, path);

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

  dedupe_fs_fullpath(ab_path, path);
  
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

static int dedupe_fs_mknod(const char *path, mode_t mode, dev_t rdev) {
  int res = 0;
  char out_buf[BUF_LEN];
  char ab_path[MAX_PATH_LEN];
  
#ifdef DEBUG
  sprintf(out_buf, "[%s] entry\n", __FUNCTION__);
  write(1, out_buf, strlen(out_buf));
#endif

  dedupe_fs_fullpath(ab_path, path);

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

static struct fuse_operations dedupe_fs_oper = {
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

  int res = 0;
  char out_buf[MAX_PATH_LEN];

  pthread_t thr;
  pthread_attr_t thr_attr;
  thread_arg_t thr_arg;

  if(argc < 2 || argc > 3) {
    usage();
  }

  pthread_attr_init(&thr_attr);

  res = pthread_attr_setdetachstate(&thr_attr, PTHREAD_CREATE_DETACHED);
  if(res < SUCCESS) {
    sprintf(out_buf, "[%s] unable to set attr to PTHREAD_CREATE_DETACHED\n", __FUNCTION__);
    write(1, out_buf, strlen(out_buf));
  }

  if(FAILED == pthread_create(&thr, &thr_attr, lazy_worker_thread, &thr_arg)) {
    sprintf(out_buf, "[%s] lazy worker creation failed\n", __FUNCTION__);
    write(1, out_buf, strlen(out_buf));
    perror("");
  }

  pthread_attr_destroy(&thr_attr);

  return fuse_main(argc, argv, &dedupe_fs_oper, NULL);
}
