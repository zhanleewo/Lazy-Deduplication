#define FUSE_USE_VERSION 26

#include "dedupe_fs.h"
#include "rabin-karp.h"
#include "sha1.h"

char *dedupe_file_store = "/tmp/dedupe_file_store";
char *dedupe_metadata = "/tmp/dedupe_metadata";
char *dedupe_hashes = "/tmp/dedupe_hashes";
char *nlinks = "nlinks.txt";

dedupe_globals globals;

void *lazy_worker_thread(void *);
extern void create_dir_search_str(char *, char *);
extern char *sha1(char *, int);

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

int dedupe_fs_lock(const char *path, uint64_t fd) {

  char out_buf[BUF_LEN] = {0};

  if(FAILED == flock(fd, LOCK_EX)) {
    sprintf(out_buf, "[%s] flock lock failed on [%s]", __FUNCTION__, path);
    perror(out_buf);
    return -errno;
  }

  return SUCCESS;
}

int dedupe_fs_unlock(const char *path, uint64_t fd) {

  char out_buf[BUF_LEN] = {0};

  if(FAILED == flock(fd, LOCK_UN)) {
    sprintf(out_buf, "[%s] flock unlock failed on [%s]", __FUNCTION__, path);
    perror(out_buf);
    return -errno;
  }

  return SUCCESS;
}

void char2stbuf(const char *stat_buf, struct stat *stbuf) {
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

void stbuf2char(char *stat_buf, struct stat *stbuf) {
  memset(stat_buf, 0, STAT_LEN);

  sprintf(stat_buf, 
    "%llu:%llu:%u:%u:%u:%u:%llu:%lld:%lu:%lld:%lu:%lu:%lu\n", 
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
}

char * strrstr(char *str, const char *substr) {
  char *head = NULL, *tail = NULL;

  head = str;
  while((head = strstr(head, substr)) != NULL) {
    tail = head;
    head++; 
  }

  return tail;
}

static int dedupe_fs_getattr(const char *path, struct stat *stbuf) {

  int res = 0, fsize = 0;

  char out_buf[BUF_LEN] = {0};
  char stat_buf[STAT_LEN] = {0};
  char ab_path[MAX_PATH_LEN] = {0};
  char meta_path[MAX_PATH_LEN] = {0};
  char new_path[MAX_PATH_LEN] = {0};
  char bitmap_path[MAX_PATH_LEN] = {0};
  char filesize[FILESIZE_LEN] = {0};

  struct fuse_file_info fi, bitmap_fi;

#ifdef DEBUG
  sprintf(out_buf, "[%s] entry\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  memset(stbuf, 0, sizeof(struct stat));

  dedupe_fs_filestore_path(ab_path, path);
  res = internal_getattr(ab_path, stbuf);
  if(-ENOENT == res) {
    return res;
  }

  memset(stbuf, 0, sizeof(struct stat));

  dedupe_fs_metadata_path(meta_path, path);

  res = internal_getattr(meta_path, stbuf);
  if(res < 0) {
    dedupe_fs_filestore_path(ab_path, path);
    res = internal_getattr(ab_path, stbuf);
    if(res < 0) {
      return res;
    }
  } else if(S_IFDIR == (stbuf->st_mode & S_IFDIR)) {
    // Get the latest dir information from filestore
    dedupe_fs_filestore_path(ab_path, path);
    res = internal_getattr(ab_path, stbuf);
    if(res < 0) {
      return res;
    }
  } else {
    // Not a directory; should be a file
    // Retrieve recent information from database

    dedupe_fs_filestore_path(bitmap_path, path);
    strcat(bitmap_path, BITMAP_FILE);

    bitmap_fi.flags = O_RDONLY;
    res = internal_open(bitmap_path, &bitmap_fi);
    if(res < 0) {
      /* do nothing */
    } else {

      memset(&stat_buf, 0, STAT_LEN);

      res = internal_read(bitmap_path, filesize, FILESIZE_LEN, (off_t)(sizeof(int)*NUM_BITMAP_WORDS), &bitmap_fi, FALSE);
      if(res < 0) {
        internal_release(bitmap_path, &bitmap_fi);
        return res;
      }

      fi.flags = O_RDONLY;
      res = internal_open(meta_path, &fi);
      if(res < 0) {
        internal_release(bitmap_path, &bitmap_fi);
        return res;
      }

      res = internal_read(meta_path, stat_buf, STAT_LEN, (off_t)0, &fi, FALSE);
      if(res < 0) {
        // res contains the errno to return to libfuse
      } else {
        // process stat_buf to form the original data
        char2stbuf(stat_buf, stbuf);
      }

      fsize = *(int *)filesize;
      if(fsize != -1) {
        stbuf->st_size = fsize;
      }

      internal_release(bitmap_path, &bitmap_fi);
      internal_release(meta_path, &fi);
    }
  }

#ifdef DEBUG
  sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  return SUCCESS;
}

static int dedupe_fs_opendir(
    const char *path, 
    struct fuse_file_info *fi) {

  int res = 0;

  char out_buf[BUF_LEN] = {0};
  char ab_path[MAX_PATH_LEN] = {0};

  DIR *dp;

#ifdef DEBUG
  sprintf(out_buf, "[%s] entry\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  dedupe_fs_filestore_path(ab_path, path);

  res = internal_opendir(ab_path, fi);
  if(res < 0) {
    return res;
  }

#ifdef DEBUG
  sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  return SUCCESS;
}

static int dedupe_fs_readdir(
    const char *path,
    void *buf, fuse_fill_dir_t filler, 
    off_t offset, 
    struct fuse_file_info *fi) {

  int res = 0;

  DIR *dp = NULL;
  struct dirent *de = NULL;

  char out_buf[BUF_LEN] = {0};

#ifdef DEBUG
  sprintf(out_buf, "[%s] entry\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  dp = (DIR*) (uintptr_t)fi->fh;

  de = readdir(dp);
  if(de == NULL)  {
    sprintf(out_buf, "[%s] readdir failed on [%s]", __FUNCTION__, path);
    perror(out_buf);
    res = -errno;
    return res;
  }

  do {

    if((NULL != strstr(de->d_name, DELETE_FILE)) ||
        (NULL != strstr(de->d_name, BITMAP_FILE))) {
      continue;
    }

    if(filler(buf, de->d_name, NULL, 0)) {
      res = -errno;
    }
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
  char bitmap_path[MAX_PATH_LEN] = {0};
  char new_bitmap_path[MAX_PATH_LEN] = {0};
  char meta_path[MAX_PATH_LEN] = {0};
  char new_meta_path[MAX_PATH_LEN] = {0};

#ifdef DEBUG
  sprintf(out_buf, "[%s] entry\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  dedupe_fs_filestore_path(ab_path, path);
  dedupe_fs_filestore_path(new_ab_path, newpath);

  dedupe_fs_metadata_path(meta_path, path);
  dedupe_fs_metadata_path(new_meta_path, newpath);

  strcpy(bitmap_path, ab_path);
  strcat(bitmap_path, BITMAP_FILE);

  strcpy(new_bitmap_path, new_ab_path);
  strcat(new_bitmap_path, BITMAP_FILE);

  res = internal_rename(meta_path, new_meta_path);
  res = internal_rename(bitmap_path, new_bitmap_path);
  res = internal_rename(ab_path, new_ab_path);

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
  char meta_link[MAX_PATH_LEN] = {0};

#ifdef DEBUG
  sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  dedupe_fs_filestore_path(ab_link, link);

  res = symlink(path, ab_link);
  if(FAILED == res) {
    dedupe_fs_metadata_path(meta_link, link);

    res = symlink(path, meta_link);
    if(FAILED == res) {
      sprintf(out_buf, "[%s] symlink failed from [%s] to [%s]", __FUNCTION__, path, meta_link);
      perror(out_buf);
      res = -errno;
    }
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
  char meta_path[MAX_PATH_LEN] = {0};
  char new_meta_path[MAX_PATH_LEN] = {0};

#ifdef DEBUG
  sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  dedupe_fs_filestore_path(ab_path, path);
  dedupe_fs_filestore_path(new_ab_path, newpath);

  res = link(ab_path, new_ab_path);
  if(FAILED == res) {
    dedupe_fs_metadata_path(meta_path, path);
    dedupe_fs_metadata_path(new_meta_path, newpath);

    if(FAILED == res) {
      sprintf(out_buf, "[%s] link failed from [%s] to [%s]", __FUNCTION__, meta_path, new_meta_path);
      perror(out_buf);
      res = -errno;
    }
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
  char stat_buf[STAT_LEN] = {0};
  char meta_path[MAX_PATH_LEN] = {0};

  struct stat stbuf;
  struct fuse_file_info fi;

#ifdef DEBUG
  sprintf(out_buf, "[%s] entry\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  dedupe_fs_filestore_path(ab_path, path);

  res = chmod(ab_path, mode);
  if(res < 0) {
    return res;
  }

  dedupe_fs_metadata_path(meta_path, path);

  fi.flags = O_RDWR;
  res = internal_open(meta_path, &fi);
  if(-ENOENT == res) {
    return SUCCESS;
  } else if(res < 0) {
    return res;
  }

  res = internal_read(meta_path, stat_buf, STAT_LEN, (off_t)0, &fi, FALSE);
  if(res <= 0) {
    internal_release(meta_path, &fi);
    return res;
  }

  char2stbuf(stat_buf, &stbuf);

  // TODO need to add checks for verify the access rights
  stbuf.st_mode = mode;

  stbuf2char(stat_buf, &stbuf);

  res = internal_write(meta_path, stat_buf, STAT_LEN, (off_t)0, &fi, FALSE);
  if(res < 0) {
    internal_release(meta_path, &fi);
    return res;
  }

  res = internal_release(meta_path, &fi);
  if(FAILED == res) {
    return res;
  }

#ifdef DEBUG
  sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  return res;
}

static int dedupe_fs_chown(const char *path, uid_t uid, gid_t gid) {

  int res = 0;

  char stat_buf[STAT_LEN] = {0};
  char out_buf[BUF_LEN] = {0};
  char ab_path[MAX_PATH_LEN] = {0};
  char meta_path[MAX_PATH_LEN] = {0};

  struct stat stbuf;
  struct fuse_file_info fi;

#ifdef DEBUG
  sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  dedupe_fs_filestore_path(ab_path, path);

  res = chown(ab_path, uid, gid);
  if(res < 0) {
    return res;
  }

  dedupe_fs_metadata_path(meta_path, path);
  fi.flags = O_RDWR;
  res = internal_open(meta_path, &fi);
  if(-ENOENT == res) {
    return SUCCESS;
  } else if(res < 0) {
    return res;
  }

  res = internal_read(meta_path, stat_buf, STAT_LEN, (off_t)0, &fi, FALSE);
  if(res <= 0) {
    internal_release(meta_path, &fi);
    return res;
  }

  char2stbuf(stat_buf, &stbuf);
  // TODO need to add checks for verify the access rights
  stbuf.st_uid = uid;
  stbuf.st_gid = gid;

  stbuf2char(stat_buf, &stbuf);

  res = internal_write(meta_path, stat_buf, STAT_LEN, (off_t)0, &fi, FALSE);
  if(res < 0) {
    internal_release(meta_path, &fi);
    return res;
  }

  res = internal_release(meta_path, &fi);
  if(FAILED == res) {
    return res;
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
  char meta_path[MAX_PATH_LEN] = {0};
  char bitmap_path[MAX_PATH_LEN] = {0};
  char ab_del_path[MAX_PATH_LEN] = {0};
  char meta_del_path[MAX_PATH_LEN] = {0};
  char bitmap_del_path[MAX_PATH_LEN] = {0};

#ifdef DEBUG
  sprintf(out_buf, "[%s] entry\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  dedupe_fs_filestore_path(ab_path, path);
  dedupe_fs_filestore_path(bitmap_path, path);
  dedupe_fs_metadata_path(meta_path, path);

  strcat(bitmap_path, BITMAP_FILE);

  dedupe_fs_filestore_path(ab_del_path, path);
  dedupe_fs_metadata_path(meta_del_path, path);

  strcat(ab_del_path, DELETE_FILE);
  strcat(meta_del_path, DELETE_FILE);
  strcpy(bitmap_del_path, bitmap_path);
  strcat(bitmap_del_path, DELETE_FILE);

  res = internal_rename(meta_path, meta_del_path);
  res = internal_rename(bitmap_path, bitmap_del_path);
  res = internal_rename(ab_path, ab_del_path);

  #ifdef DEBUG
  sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  return res;
}

static int dedupe_fs_rmdir(const char *path) {

  int res = SUCCESS;

  struct dirent *de;

  char out_buf[BUF_LEN] = {0};
  char ab_path[MAX_PATH_LEN] = {0};
  char meta_path[MAX_PATH_LEN] = {0};
  char ab_del_path[MAX_PATH_LEN] = {0};
  char meta_del_path[MAX_PATH_LEN] = {0};

  char new_path[MAX_PATH_LEN] = {0};

  struct fuse_file_info dir_fi;

#ifdef DEBUG
  sprintf(out_buf, "[%s] entry\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  dedupe_fs_filestore_path(ab_path, path);
  dedupe_fs_metadata_path(meta_path, path);

  res = internal_opendir(ab_path, &dir_fi);
  if(res < 0) {
    return res;
  }

  while((de = readdir(dir_fi.fh)) != NULL) {

    if((SUCCESS == strcmp(de->d_name, ".")) ||
        (SUCCESS == strcmp(de->d_name, "..")) ||
        (NULL != strstr(de->d_name, BITMAP_FILE))) {
      continue;
    }

    strcpy(new_path, ab_path);
    strcat(new_path, "/");
    strcat(new_path, de->d_name);

    if(NULL == strstr(new_path, DELETE_FILE)) {
      res = -ENOTEMPTY;
      break;
    }

  }

  internal_releasedir(ab_path, &dir_fi);

  if(SUCCESS == res) {
    strcpy(ab_del_path, ab_path);
    strcat(ab_del_path, DELETE_FILE);

    strcpy(meta_del_path, meta_path);
    strcat(meta_del_path, DELETE_FILE);

    internal_rename(meta_path, meta_del_path);
    internal_rename(ab_path, ab_del_path);
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

  dedupe_fs_filestore_path(ab_path, path);

  res = access(ab_path, mask);
  if(FAILED == res) {
    dedupe_fs_metadata_path(meta_path, path);
    res = access(meta_path, mask);
    if(FAILED == res) {
      sprintf(out_buf, "[%s] access failed on [%s]", __FUNCTION__, meta_path);
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

  int res = 0;

  unsigned int *bitmap = NULL;

  char out_buf[BUF_LEN] = {0};
  char ab_path[MAX_PATH_LEN] = {0};

  struct fuse_context *mycontext = fuse_get_context();

  dedupe_globals *glob = (dedupe_globals *)mycontext->private_data;

#ifdef DEBUG
  sprintf(out_buf, "[%s] entry\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  dedupe_fs_filestore_path(ab_path, path);

  //printf("O_CREAT [%d] O_TRUNC [%d] O_EXCL [%d] O_APPEND [%d]\n", O_CREAT, O_TRUNC, O_EXCL, O_APPEND);

  if((fi->flags & O_APPEND) == O_APPEND) {
    fi->flags &= ~O_APPEND;
  }

  res = internal_open(ab_path, fi);
  if(res < 0) {
    return res;
  }

#ifdef DEBUG
  sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  return SUCCESS;
}

static int dedupe_fs_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi) {

  int res = 0, read = 0, meta_file = 0;

  unsigned int *bitmap = NULL;

  time_t tm;

  size_t meta_f_readcnt = 0;
  size_t hash_read = 0, toread = 0, r_cnt = 0;

  off_t hash_off = 0, req_off = offset;
  off_t st_off = 0, end_off = 0;
  off_t block_num = 0, cur_off = 0;

  char *sha1 = NULL, *saveptr = NULL;
  char *st = NULL, *end = NULL;

  char out_buf[BUF_LEN] = {0};
  char ab_path[MAX_PATH_LEN] = {0};
  char meta_path[MAX_PATH_LEN] = {0};
  char stat_buf[STAT_LEN] = {0};
  char hash_line[OFF_HASH_LEN] = {0};
  char srchstr[MAX_PATH_LEN] = {0};
  char filechunk[MAXCHUNK + 1] = {0};
  char bitmap_file_path[MAX_PATH_LEN] = {0};

  struct stat stbuf = {0}, meta_stbuf = {0};

  struct fuse_file_info meta_fi = {0}, hash_fi = {0};
  struct fuse_file_info wr_fi = {0}, bitmap_fi = {0};

#ifdef DEBUG
  sprintf(out_buf, "[%s] entry\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  dedupe_fs_metadata_path(meta_path, path);
  dedupe_fs_filestore_path(ab_path, path);

  dedupe_fs_lock(ab_path, fi->fh);

  dedupe_fs_filestore_path(bitmap_file_path, path);
  strcat(bitmap_file_path, BITMAP_FILE);

  bitmap_fi.flags = O_RDWR;
  res = internal_open(bitmap_file_path, &bitmap_fi);
  if(res < 0) {
    dedupe_fs_unlock(ab_path, fi->fh);
    return res;
  }

  bitmap = (unsigned int *) mmap(NULL, BITMAP_LEN, 
      PROT_READ | PROT_WRITE, MAP_SHARED, bitmap_fi.fh, (off_t)0);
  if(bitmap == MAP_FAILED) {

    internal_release(bitmap_file_path, &bitmap_fi);
    dedupe_fs_unlock(ab_path, fi->fh);

    sprintf(out_buf, "[%s] mmap failed on [%s]", __FUNCTION__, bitmap_file_path);
    perror(out_buf);
    res = -errno;
    return res;
  }

  internal_release(bitmap_file_path, &bitmap_fi);

  res = internal_getattr(meta_path, &meta_stbuf);

  if(res != -ENOENT) {

    meta_file = TRUE;
    meta_fi.flags = O_RDONLY;
    res = internal_open(meta_path, &meta_fi);
    if(res < 0) {
      munmap((void*)bitmap, BITMAP_LEN);
      dedupe_fs_unlock(ab_path, fi->fh);
      return res;
    }

    res = internal_read(meta_path, stat_buf, STAT_LEN, (off_t)0, &meta_fi, FALSE);
    if(res <= 0) {
      internal_release(meta_path, &meta_fi);
      munmap((void*)bitmap, BITMAP_LEN);
      dedupe_fs_unlock(ab_path, fi->fh);
      return res;
    }

    meta_f_readcnt += STAT_LEN;
    char2stbuf(stat_buf, &stbuf);
    hash_off = STAT_LEN;

  } else {

    res = internal_getattr(ab_path, &stbuf);
    if(res < 0) {
      internal_release(meta_path, &meta_fi);
      munmap((void*)bitmap, BITMAP_LEN);
      dedupe_fs_unlock(ab_path, fi->fh);
      return res;
    }
  }

  if(bitmap[NUM_BITMAP_WORDS] != (unsigned int)-1) {
    stbuf.st_size = bitmap[NUM_BITMAP_WORDS];
  }

  if(req_off > stbuf.st_size) {
    return SUCCESS;
  }

  if((req_off+size) > stbuf.st_size) {
    toread = stbuf.st_size - req_off;
    if(toread < 0) {
      toread = 0;
    }
  } else {
    toread = size;
  }

  printf("[%s] path [%s] size [%ld] off [%ld]\n", __FUNCTION__, path, size, offset);

  // TODO Redesign lock
  while(toread > 0) {

    block_num = req_off / MINCHUNK;

    printf("toread [%ld] read [%d] req_off [%lld] block_num [%lld]\n", toread, read, req_off, block_num);

    if(bitmap[block_num/32] & (1<<(block_num%32))) {
 
      /* Requested block is present in the filestore */
      printf("chunk inside filestore\n");

      if(toread < MINCHUNK) {
        hash_read = toread;
      } else {
        hash_read = MINCHUNK;
      }
 
      r_cnt = internal_read(ab_path, buf+read, hash_read, req_off, fi, TRUE);
      if(r_cnt <= 0) {
        internal_release(meta_path, &meta_fi);
        munmap((void*)bitmap, BITMAP_LEN);
        dedupe_fs_unlock(ab_path, fi->fh);
        return (read+r_cnt);
      }
 
      toread -= r_cnt;
      read += r_cnt;
      req_off += r_cnt;
 
    } else {
 
      if(toread < MINCHUNK)
        hash_read = toread;
      else 
        hash_read = MINCHUNK;
 
      printf("chunk inside dedupe block\n");

      while(meta_f_readcnt < meta_stbuf.st_size) {
  
        memset(hash_line, 0, OFF_HASH_LEN);
        res = internal_read(meta_path, hash_line, OFF_HASH_LEN, hash_off, &meta_fi, FALSE);
        if(res <= 0) {
          internal_release(meta_path, &meta_fi);
          munmap((void*)bitmap, BITMAP_LEN);
          dedupe_fs_unlock(ab_path, fi->fh);
          return read;
        }
  
        st = strtok_r(hash_line, ":", &saveptr);
        st_off = (off_t)atoll(st);
  
        end = strtok_r(NULL, ":", &saveptr);
        end_off = (off_t)atoll(end);
  
        sha1 = strtok_r(NULL, ":", &saveptr);
        sha1[strlen(sha1)-1] = '\0';
  
        if(req_off >= st_off && req_off <= end_off) {
  
          create_dir_search_str(srchstr, sha1);
          strcat(srchstr, "/");
          strcat(srchstr, sha1);
  
          hash_fi.flags = O_RDONLY;
          res = internal_open(srchstr, &hash_fi);
          if(res < 0) {
            internal_release(meta_path, &meta_fi);
            munmap((void*)bitmap, BITMAP_LEN);
            dedupe_fs_unlock(ab_path, fi->fh);
            return read;
          }
  
          r_cnt = internal_read(srchstr, buf+read, hash_read, req_off-st_off, &hash_fi, FALSE);
          if(r_cnt <= 0) {
            internal_release(meta_path, &meta_fi);
            internal_release(srchstr, &hash_fi);
            munmap((void*)bitmap, BITMAP_LEN);
            dedupe_fs_unlock(ab_path, fi->fh);
            return read;
          }
  
          res = internal_release(srchstr, &hash_fi);
          if(res < 0) {
            internal_release(meta_path, &meta_fi);
            munmap((void*)bitmap, BITMAP_LEN);
            dedupe_fs_unlock(ab_path, fi->fh);
            return read;
          }
  
          toread -= r_cnt;
          read += r_cnt;
          req_off += r_cnt;
          hash_read -= r_cnt;
        }
  
        if(hash_read <= 0) {
          break;
        }

        meta_f_readcnt += OFF_HASH_LEN;
        hash_off += OFF_HASH_LEN;
      }
    }
  }

  if(meta_file == TRUE) {
    // update the st_atime modified during file read

    internal_release(meta_path, &meta_fi);

    time(&tm);
    stbuf.st_atime = tm;
 
    wr_fi.flags = O_RDWR;
    res = internal_open(meta_path, &wr_fi);
    if(res < 0) {
      munmap((void*)bitmap, BITMAP_LEN);
      dedupe_fs_unlock(ab_path, fi->fh);
      return read;
    }
 
    memset(stat_buf, 0, STAT_LEN);
    stbuf2char(stat_buf, &stbuf);
 
    res = internal_write(meta_path, stat_buf, STAT_LEN, (off_t)0, &wr_fi, FALSE);
    if(res < 0) {
      internal_release(meta_path, &wr_fi);
      munmap((void*)bitmap, BITMAP_LEN);
      dedupe_fs_unlock(ab_path, fi->fh);
      return read;
    }
 
    internal_release(meta_path, &wr_fi);
  }

  munmap((void*)bitmap, BITMAP_LEN);
  dedupe_fs_unlock(ab_path, fi->fh);

#ifdef DEBUG
  sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  return read;
}

static int dedupe_fs_write(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi) {

  int res = 0, meta_file = 0, read = 0;

  unsigned int *bitmap = NULL;

  size_t meta_f_readcnt = 0, r_cnt = 0;
  size_t req_off_len = 0, req_size_len = 0;
  size_t write_len = 0;

  off_t block_num = 0, hash_off = 0;
  off_t req_off_st = 0, req_size_st = 0;
  off_t st_off = 0, end_off = 0;

  char *sha1 = NULL, *saveptr = NULL;
  char *st = NULL, *end = NULL;

  char out_buf[BUF_LEN] = {0};
  char srchstr[MAX_PATH_LEN] = {0};
  char stat_buf[STAT_LEN] = {0};
  char ab_path[MAX_PATH_LEN] = {0};
  char meta_path[MAX_PATH_LEN] = {0};
  char hash_line[OFF_HASH_LEN] = {0};
  char write_buf[MINCHUNK] = {0};
  char bitmap_file_path[MAX_PATH_LEN] = {0};

  struct stat meta_stbuf = {0};
  struct stat stbuf = {0};

  struct fuse_file_info meta_fi = {0};
  struct fuse_file_info hash_fi = {0};
  struct fuse_file_info bitmap_fi = {0};

#ifdef DEBUG
  sprintf(out_buf, "[%s] entry\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  printf("[%s] path [%s] size [%ld] off [%lld] flags [%x]\n", __FUNCTION__, path, size, offset, fi->flags);

  /*
     Modify the file stats here, esp the st_size
     Handle the case where application called a pwrite,
     on the offset above the existing offset
   */

  dedupe_fs_metadata_path(meta_path, path);

  dedupe_fs_filestore_path(ab_path, path);

  dedupe_fs_lock(ab_path, fi->fh);

  dedupe_fs_filestore_path(bitmap_file_path, path);
  strcat(bitmap_file_path, BITMAP_FILE);

  bitmap_fi.flags = O_RDWR;
  res = internal_open(bitmap_file_path, &bitmap_fi);
  if(res < 0) {
    return res;
  }

  bitmap = (unsigned int *) mmap(NULL, BITMAP_LEN, 
      PROT_READ | PROT_WRITE, MAP_SHARED, bitmap_fi.fh, (off_t)0);
  if(bitmap == MAP_FAILED) {

    internal_release(bitmap_file_path, &bitmap_fi);
    dedupe_fs_unlock(ab_path, fi->fh);

    sprintf(out_buf, "[%s] mmap failed on [%s]", __FUNCTION__, bitmap_file_path);
    perror(out_buf);
    res = -errno;
    return res;
  }

  internal_release(bitmap_file_path, &bitmap_fi);

  res = internal_getattr(meta_path, &meta_stbuf);


  if(res != -ENOENT) {

    printf("pwrite call\n");

    meta_file = TRUE;
    meta_fi.flags = O_RDONLY;
    res = internal_open(meta_path, &meta_fi);
    if(res < 0) {
      munmap((void*)bitmap, BITMAP_LEN);
      dedupe_fs_unlock(ab_path, fi->fh);
      return res;
    }

    res = internal_read(meta_path, stat_buf, STAT_LEN, (off_t)0, &meta_fi, FALSE);
    if(res <= 0) {
      internal_release(meta_path, &meta_fi);
      munmap((void*)bitmap, BITMAP_LEN);
      dedupe_fs_unlock(ab_path, fi->fh);
      return res;
    }

    char2stbuf(stat_buf, &stbuf);

    if(stbuf.st_size == 0) {
      internal_release(meta_path, &meta_fi);
      goto first_write;
    }

    meta_f_readcnt = STAT_LEN;
    hash_off = STAT_LEN;

    /*
     * When offset is not a multiple of 4K read the start of the 4K until offset
     * and store it. Same holds when size is not a multiple of 4K.
     */

    if((offset%MINCHUNK) != 0) {

      read = 0;
      req_off_st = (offset/MINCHUNK) * MINCHUNK;
      req_off_len = offset - req_off_st;

      write_len += req_off_len;

      // Read req_off_st to req_off_end from hash blocks

      while(meta_f_readcnt < meta_stbuf.st_size) {

        memset(hash_line, 0, OFF_HASH_LEN);
        res = internal_read(meta_path, hash_line, OFF_HASH_LEN, hash_off, &meta_fi, FALSE);
        if(res <= 0) {
          internal_release(meta_path, &meta_fi);
          munmap((void*)bitmap, BITMAP_LEN);
          dedupe_fs_unlock(ab_path, fi->fh);
          return res;
        }

        st = strtok_r(hash_line, ":", &saveptr);
        st_off = (off_t)atoll(st);
  
        end = strtok_r(NULL, ":", &saveptr);
        end_off = (off_t)atoll(end);
 
        sha1 = strtok_r(NULL, ":", &saveptr);
        sha1[strlen(sha1)-1] = '\0';
 
        if(req_off_st >= st_off && req_off_st <= end_off) {

          create_dir_search_str(srchstr, sha1);
          strcat(srchstr, "/");
          strcat(srchstr, sha1);
  
          hash_fi.flags = O_RDONLY;
          res = internal_open(srchstr, &hash_fi);
          if(res < 0) {
            internal_release(meta_path, &meta_fi);
            munmap((void*)bitmap, BITMAP_LEN);
            dedupe_fs_unlock(ab_path, fi->fh);
            return res;
          }

          r_cnt = internal_read(srchstr, write_buf+read, req_off_len, req_off_st-st_off, &hash_fi, FALSE);
          if(r_cnt <= 0) {
            internal_release(srchstr, &hash_fi);
            internal_release(meta_path, &meta_fi);
            munmap((void*)bitmap, BITMAP_LEN);
            dedupe_fs_unlock(ab_path, fi->fh);
            return res;
          }

          res = internal_release(srchstr, &hash_fi);
          if(res < 0) {
            internal_release(meta_path, &meta_fi);
            munmap((void*)bitmap, BITMAP_LEN);
            dedupe_fs_unlock(ab_path, fi->fh);
            return res;
          }

          read += r_cnt;
          req_off_st += r_cnt;
          req_off_len -= r_cnt;
        }

        if(req_off_len <= 0) {
          break;
        }

        meta_f_readcnt += OFF_HASH_LEN;
        hash_off += OFF_HASH_LEN;
      }
    }

    // memcpy the data sent from application

    memcpy(write_buf+(offset%MINCHUNK), buf, size);
    read += size;
    write_len += size;

    if(((offset+size)%MINCHUNK) != 0) {
      // Overwrite of size bytes from offset (or) insertion/overwrite
      // on the endblock of the file

      req_size_st = offset+size;
      if(stbuf.st_size > req_size_st) {
        if((((offset+size)/MINCHUNK + 1) * MINCHUNK) < stbuf.st_size) {
          req_size_len = (((offset+size)/MINCHUNK + 1) * MINCHUNK);
        }
        else {
          req_size_len = stbuf.st_size;
        }
      }
      else {
        req_size_len = req_size_st;
      }

      req_size_len = req_size_len - req_size_st;
      if(req_size_len < 0) {
        req_size_len = 0;
      }
      //meta_f_readcnt = STAT_LEN;
      //hash_off = STAT_LEN;

      write_len += req_size_len;

      while(meta_f_readcnt < meta_stbuf.st_size) {

        memset(hash_line, 0, OFF_HASH_LEN);
        res = internal_read(meta_path, hash_line, OFF_HASH_LEN, hash_off, &meta_fi, FALSE);
        if(res <= 0) {
          internal_release(meta_path, &meta_fi);
          munmap((void*)bitmap, BITMAP_LEN);
          dedupe_fs_unlock(ab_path, fi->fh);
          return res;
        }

        st = strtok_r(hash_line, ":", &saveptr);
        st_off = (off_t)atoll(st);
  
        end = strtok_r(NULL, ":", &saveptr);
        end_off = (off_t)atoll(end);
  
        sha1 = strtok_r(NULL, ":", &saveptr);
        sha1[strlen(sha1)-1] = '\0';

        if(req_size_st >= st_off && req_size_st <= end_off) {
          create_dir_search_str(srchstr, sha1);
          strcat(srchstr, "/");
          strcat(srchstr, sha1);
  
          hash_fi.flags = O_RDONLY;
          res = internal_open(srchstr, &hash_fi);
          if(res < 0) {
            internal_release(meta_path, &meta_fi);
            munmap((void*)bitmap, BITMAP_LEN);
            dedupe_fs_unlock(ab_path, fi->fh);
            return res;
          }
 
          r_cnt = internal_read(srchstr, write_buf+read, req_size_len, req_size_st-st_off, &hash_fi, FALSE);
          if(r_cnt < 0) {
            internal_release(srchstr, &hash_fi);
            internal_release(meta_path, &meta_fi);
            munmap((void*)bitmap, BITMAP_LEN);
            dedupe_fs_unlock(ab_path, fi->fh);
            return res;
          }

          res = internal_release(srchstr, &hash_fi);
          if(res < 0) {
            internal_release(meta_path, &meta_fi);
            munmap((void*)bitmap, BITMAP_LEN);
            dedupe_fs_unlock(ab_path, fi->fh);
            return res;
          }

          read += r_cnt;
          req_size_st += r_cnt;
          req_size_len -= r_cnt;
        }

        if(req_size_len <= 0) {
          break;
        }

        meta_f_readcnt += OFF_HASH_LEN;
        hash_off += OFF_HASH_LEN;
      }
    }

    res = internal_write(ab_path, write_buf, write_len, (off_t)((offset/MINCHUNK)*MINCHUNK), fi, TRUE);
    if(res < 0) {
      internal_release(meta_path, &meta_fi);
      munmap((void*)bitmap, BITMAP_LEN);
      dedupe_fs_unlock(ab_path, fi->fh);
      return res;
    }

    block_num = offset / MINCHUNK;
    bitmap[block_num/32] |= 1<<(block_num%32);

    printf("offset [%lld] size [%ld]\n", offset ,size);
    if((size_t)(offset+size) > stbuf.st_size) {
      bitmap[NUM_BITMAP_WORDS] = (unsigned int)(offset+size);
    } else {
      bitmap[NUM_BITMAP_WORDS] = (unsigned int)(stbuf.st_size);
    }

  } else {

first_write:
    printf("First time write\n");

    memcpy(write_buf+(offset%MINCHUNK), buf, size);

    res = internal_write(ab_path, write_buf+(offset%MINCHUNK), size, offset, fi, TRUE);
    if(res < 0) {
      munmap((void*)bitmap, BITMAP_LEN);
      dedupe_fs_unlock(ab_path, fi->fh);
      return res;
    }

    block_num = offset / MINCHUNK;
    bitmap[block_num/32] |= 1<<(block_num%32);

    bitmap[NUM_BITMAP_WORDS] = (unsigned int)(offset+size);
  }

  munmap((void*)bitmap, BITMAP_LEN);
  dedupe_fs_unlock(ab_path, fi->fh);

#ifdef DEBUG
  sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  return ((int)size);
}

static int dedupe_fs_release(const char *path, struct fuse_file_info *fi) {

  int res = 0;
  char out_buf[BUF_LEN];

  unsigned int *bitmap = NULL;

#ifdef DEBUG
  sprintf(out_buf, "[%s] entry\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  close(fi->fh);

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

  int i = 0, j = 0, res = 0;
  int meta_present = FALSE;
  int block_num = -1;

  unsigned int *bitmap = NULL;

  time_t tm;

  char out_buf[BUF_LEN] = {0};
  char stat_buf[STAT_LEN] = {0};
  char filesize[FILESIZE_LEN] = {0};
  char ab_path[MAX_PATH_LEN] = {0};
  char meta_path[MAX_PATH_LEN] = {0};
  char bitmap_path[MAX_PATH_LEN] = {0};

  struct stat stbuf = {0};
  struct rlimit rlim = {0};

  struct fuse_file_info fi = {0};
  struct fuse_file_info meta_fi = {0};
  struct fuse_file_info bitmap_fi = {0};

#ifdef DEBUG
  sprintf(out_buf, "[%s] entry\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  getrlimit(RLIMIT_FSIZE, &rlim);

  if(newsize < 0 || newsize > rlim.rlim_cur) {
    res = -EINVAL;
    return res;
  }

  /*
   * File editors call truncate prior to updating the file. 
   * On truncate, set the bitmap entry's filesize to 0
   */

  dedupe_fs_filestore_path(ab_path, path);

  fi.flags = O_RDONLY;
  res = internal_open(ab_path, &fi);
  if(res < 0) {
    return res;
  }

  dedupe_fs_lock(ab_path, fi.fh);

  dedupe_fs_metadata_path(meta_path, path);

  meta_fi.flags = O_RDWR;
  res = internal_open(meta_path, &meta_fi);
  if(-ENOENT == res) {
    res = internal_truncate(ab_path, newsize);
    printf("done\n");
    if(res < 0) {
      dedupe_fs_unlock(ab_path, fi.fh);
      return res;
    }
  } else {
    meta_present = TRUE;
  }

  dedupe_fs_filestore_path(bitmap_path, path);
  strcat(bitmap_path, BITMAP_FILE);

  bitmap_fi.flags = O_RDWR;
  res = internal_open(bitmap_path, &bitmap_fi);
  if(res < 0) {
    if(TRUE == meta_present) {
      internal_release(meta_path, &meta_fi);
    }
    dedupe_fs_unlock(ab_path, fi.fh);
    internal_release(ab_path, &fi);
    return res;
  }

  bitmap = (unsigned int *) mmap(NULL, BITMAP_LEN,
      PROT_READ | PROT_WRITE, MAP_SHARED, bitmap_fi.fh, (off_t)0);

  if(bitmap == MAP_FAILED) {

    if(TRUE == meta_present) {
      internal_release(meta_path, &meta_fi);
    }
    internal_release(bitmap_path, &bitmap_fi);
    dedupe_fs_unlock(ab_path, fi.fh);
    internal_release(ab_path, &fi);

    sprintf(out_buf, "[%s] mmap failed on [%s]", __FUNCTION__, bitmap_path);
    perror(out_buf);
    res = -errno;
    return res;
  }

  internal_release(bitmap_path, &bitmap_fi);

  block_num = (newsize-1) / MINCHUNK;

  for(i = block_num/32 ; i < NUM_BITMAP_WORDS ; i++) {

    if(i == (block_num/32)) {
      j = block_num%32 + 1;
    } else {
      j = 0;
    }

    for( ; j < 32 ; j++) {
      bitmap[i] &= ~(1<<j);
    }
  }

  bitmap[NUM_BITMAP_WORDS] = (unsigned int)newsize;

  munmap((void*)bitmap, BITMAP_LEN);

#if 0
  res = internal_write(bitmap_path, &newsize, FILESIZE_LEN, (off_t)(sizeof(int)*NUM_BITMAP_WORDS), &bitmap_fi, FALSE);
  if(res < 0) {
    if(TRUE == meta_present) {
    internal_release(meta_path, &meta_fi);
    }
    dedupe_fs_unlock(ab_path, fi.fh);
    internal_release(ab_path, &fi);
    return res;
  }
#endif

  if(TRUE == meta_present) {

    // update st_mtime and st_ctime during truncate

    res = internal_read(meta_path, stat_buf, STAT_LEN, (off_t)0, &meta_fi);
    if(res < 0) {
      if(TRUE == meta_present) {
        internal_release(meta_path, &meta_fi);
      }
      dedupe_fs_unlock(ab_path, fi.fh);
      internal_release(ab_path, &fi);
      return res;
    }

    char2stbuf(stat_buf, &stbuf);

    time(&tm);
    stbuf.st_size = newsize;
    stbuf.st_mtime = tm;
    stbuf.st_ctime = tm;
 
    stbuf2char(stat_buf, &stbuf);
 
    res = internal_write(meta_path, stat_buf, STAT_LEN, (off_t)0, &meta_fi, FALSE);
    if(res < 0) {
      if(TRUE == meta_present) {
        internal_release(meta_path, &meta_fi);
      }
      dedupe_fs_unlock(ab_path, fi.fh);
      internal_release(ab_path, &fi);
      return res;
    }
 
    res = internal_release(meta_path, &meta_fi);
    if(FAILED == res) {
      dedupe_fs_unlock(ab_path, fi.fh);
      return res;
    }
  }

  dedupe_fs_unlock(ab_path, fi.fh);
  internal_release(ab_path, &fi);

#ifdef DEBUG
  sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  return SUCCESS;
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
  if(FAILED == res) {
    sprintf(out_buf, "[%s] utime failed on [%s]", __FUNCTION__, ab_path);
    perror(out_buf);
    res = -errno;
  }

#ifdef DEBUG
  sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  return res;
}

static int dedupe_fs_create(const char *path, mode_t mode, struct fuse_file_info *fi) {

  int res = 0;

  char out_buf[BUF_LEN] = {0};
  char ab_path[MAX_PATH_LEN] = {0};
  char bitmap_file_path[MAX_PATH_LEN] = {0};

  struct fuse_file_info bitmap_fi = {0};

  unsigned int bitmap[NUM_BITMAP_WORDS+1] = {0};

#ifdef DEBUG
  sprintf(out_buf, "[%s] entry\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  dedupe_fs_filestore_path(ab_path, path);
 
  printf("Creation flags [%x]\n", fi->flags);
  res = internal_create(ab_path, mode, fi);
  if(res < 0) {
    return res;
  }

  dedupe_fs_filestore_path(bitmap_file_path, path);
  strcat(bitmap_file_path, BITMAP_FILE);

  res = internal_mknod(bitmap_file_path, mode, 0);
  if(res < 0) {
    return res;
  }

  bitmap_fi.flags = O_RDWR;
  res = internal_open(bitmap_file_path, &bitmap_fi);
  if(res < 0) {
    return res;
  }

  bitmap[NUM_BITMAP_WORDS] = (unsigned int)-1;

  res = internal_write(bitmap_file_path, (void*)&bitmap, BITMAP_LEN, (off_t)0, &bitmap_fi, FALSE);
  if(res < 0) {
    return res;
  }

  internal_release(bitmap_file_path, &bitmap_fi);

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

  precompute_RM();

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
