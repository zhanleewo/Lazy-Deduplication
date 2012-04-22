//gcc -Wall `pkg-config fuse --cflags --libs` dedupe_fs.c -o dedupe_fs


#define FUSE_USE_VERSION 26

#include "dedupe_fs.h"
#include "rabin-karp.h"
#include "sha1.h"

char *dedupe_file_store = "/tmp/dedupe_file_store";
char *dedupe_metadata = "/tmp/dedupe_metadata";
char *dedupe_hashes = "/tmp/dedupe_hashes";
char *nlinks = "nlinks.txt";

static unsigned int *bitmask = NULL;

dedupe_globals globals;

extern void *lazy_worker_thread(void *);
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

  dedupe_fs_metadata_path(meta_path, path);
  memset(stbuf, 0, sizeof(struct stat));
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
    // Retrieve not-so-recent information 
    // from dedupe database
    fi.flags = O_RDONLY;
    res = internal_open(meta_path, &fi);
    if(res < 0) {
    } else {
      memset(&stat_buf, 0, STAT_LEN);
      res = internal_read(meta_path, stat_buf, STAT_LEN, (off_t)0, &fi, FALSE);
      if(res < 0) {
        // res contains the errno to return to libfuse
      } else {
        // process stat_buf to form the original data
        char2stbuf(stat_buf, stbuf);
      }

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

  int res = 0, flag = FAILED;

  DIR *dp;
  struct dirent *de;

  char out_buf[BUF_LEN];
  char meta_path[MAX_PATH_LEN] = {0};

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
    if(NULL == strstr(de->d_name, BITMASK_FILE)) {
      if(filler(buf, de->d_name, NULL, 0))
        res = -errno;
    }
    flag = SUCCESS;
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

  dedupe_fs_filestore_path(ab_path, path);
  dedupe_fs_filestore_path(new_ab_path, newpath);

  res = rename(ab_path, new_ab_path);
  if(FAILED == res) {
    dedupe_fs_metadata_path(meta_path, path);
    dedupe_fs_metadata_path(new_meta_path, path);

    res = rename(meta_path, new_meta_path);
    if(FAILED == res) {
      sprintf(out_buf, "[%s] rename failed from [%s] to [%s]", __FUNCTION__, meta_path, new_meta_path);
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
  if(SUCCESS == res) {
    return res;
  }

  dedupe_fs_metadata_path(meta_path, path);

  fi.flags = O_RDWR;
  res = internal_open(meta_path, &fi);
  if(res < 0) {
    return res;
  }

  res = internal_read(meta_path, stat_buf, STAT_LEN, (off_t)0, &fi, FALSE);
  if(res <= 0) {
    return res;
  }

  char2stbuf(stat_buf, &stbuf);

  // TODO need to add checks for verify the access rights
  stbuf.st_mode = mode;

  stbuf2char(stat_buf, &stbuf);

  res = internal_write(meta_path, stat_buf, STAT_LEN, (off_t)0, &fi, FALSE);
  if(res < 0) {
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
  if(SUCCESS == res) {
    return res;
  }

  dedupe_fs_metadata_path(meta_path, path);
  fi.flags = O_RDWR;
  res = internal_open(meta_path, &fi);
  if(FAILED == res) {
    return res;
  }

  res = internal_read(meta_path, stat_buf, STAT_LEN, (off_t)0, &fi, FALSE);
  if(res <= 0) {
    return res;
  }

  char2stbuf(stat_buf, &stbuf);
  // TODO need to add checks for verify the access rights
  stbuf.st_uid = uid;
  stbuf.st_gid = gid;

  stbuf2char(stat_buf, &stbuf);

  res = internal_write(meta_path, stat_buf, STAT_LEN, (off_t)0, &fi, FALSE);
  if(res < 0) {
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

#ifdef DEBUG
  sprintf(out_buf, "[%s] entry\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  dedupe_fs_filestore_path(ab_path, path);
  res = unlink(ab_path);
  if (res < 0) {
    // TODO need to handle file unlink from chunk database and decrementing link count
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

  res = internal_rmdir(ab_path);
  if(SUCCESS == res) {
    return res;
  }

  // TODO need to handle file unlink part of the code as well also few other checks

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
  char out_buf[BUF_LEN] = {0};
  char ab_path[MAX_PATH_LEN] = {0};
  char bitmask_file_path[MAX_PATH_LEN] = {0};

  struct fuse_file_info bitmask_fi;

  struct fuse_context *mycontext = fuse_get_context();

  dedupe_globals *glob = (dedupe_globals *)mycontext->private_data;

#ifdef DEBUG
  sprintf(out_buf, "[%s] entry\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  dedupe_fs_filestore_path(ab_path, path);

  res = internal_open(ab_path, fi);
  if(res < 0) {
    return res;
  }

  dedupe_fs_filestore_path(bitmask_file_path, path);
  strcat(bitmask_file_path, BITMASK_FILE);

  bitmask_fi.flags = O_RDWR;
  res = internal_open(bitmask_file_path, &bitmask_fi);
  if(res < 0) {
    return res;
  }

  bitmask = (unsigned int *) mmap(NULL, BITMASK_LEN, 
      PROT_READ | PROT_WRITE, MAP_SHARED, bitmask_fi.fh, (off_t)0);
  if(bitmask == MAP_FAILED) {
    sprintf(out_buf, "[%s] mmap failed on [%s]", __FUNCTION__, bitmask_file_path);
    perror(out_buf);
    res = -errno;
    return res;
  }

  internal_release(bitmask_file_path, &bitmask_fi);

#ifdef DEBUG
  sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  return SUCCESS;
}

static int dedupe_fs_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi) {

  int res = 0, read = 0, meta_file = 0;

  time_t tm;

  size_t meta_f_readcnt = 0;
  size_t hash_read = 0, toread = 0, r_cnt = 0;

  off_t hash_off = 0, req_off = offset;
  off_t st_off = 0, end_off = 0;
  off_t block_num = 0, cur_off = 0;

  struct stat stbuf, meta_stbuf;
  struct fuse_file_info meta_fi, hash_fi, wr_fi;

  char *sha1 = NULL, *saveptr = NULL;
  char *st = NULL, *end = NULL;

  char out_buf[BUF_LEN] = {0};
  char ab_path[MAX_PATH_LEN] = {0};
  char meta_path[MAX_PATH_LEN] = {0};
  char stat_buf[STAT_LEN] = {0};
  char hash_line[OFF_HASH_LEN] = {0};
  char srchstr[MAX_PATH_LEN] = {0};
  char filechunk[MAXCHUNK + 1] = {0};

#ifdef DEBUG
  sprintf(out_buf, "[%s] entry\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  dedupe_fs_metadata_path(meta_path, path);
  dedupe_fs_filestore_path(ab_path, path);

  dedupe_fs_lock(ab_path, fi->fh);

  res = internal_getattr(meta_path, &meta_stbuf);

  if(res != -ENOENT) {

    meta_file = TRUE;
    meta_fi.flags = O_RDONLY;
    res = internal_open(meta_path, &meta_fi);
    if(res < 0) {
      dedupe_fs_unlock(ab_path, fi->fh);
      return res;
    }

    res = internal_read(meta_path, stat_buf, STAT_LEN, (off_t)0, &meta_fi, FALSE);
    if(res <= 0) {
      ABORT;
    }

    meta_f_readcnt += STAT_LEN;
    char2stbuf(stat_buf, &stbuf);
    hash_off = STAT_LEN;

  } else {

    res = internal_getattr(ab_path, &stbuf);
    if(res < 0) {
      dedupe_fs_unlock(ab_path, fi->fh);
      return res;
    }
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

    if(bitmask[block_num/32] & (1<<(block_num%32))) {
 
      /* Requested block is present in the filestore */
      printf("chunk inside filestore\n");

      if(toread < MINCHUNK) {
        hash_read = toread;
      } else {
        hash_read = MINCHUNK;
      }
 
      r_cnt = internal_read(ab_path, buf+read, hash_read, req_off, fi, TRUE);
      if(r_cnt <= 0) {
        dedupe_fs_unlock(ab_path, fi->fh);
        return r_cnt;
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
          dedupe_fs_unlock(ab_path, fi->fh);
          return res;
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
            return res;
          }
  
          r_cnt = internal_read(srchstr, buf+read, hash_read, req_off-st_off, &hash_fi, FALSE);
          if(r_cnt <= 0) {
            dedupe_fs_unlock(ab_path, fi->fh);
            return read;
          }
  
          res = internal_release(srchstr, &hash_fi);
          if(res < 0) {
            dedupe_fs_unlock(ab_path, fi->fh);
            return res;
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
      dedupe_fs_unlock(ab_path, fi->fh);
      return res;
    }
 
    memset(stat_buf, 0, STAT_LEN);
    stbuf2char(stat_buf, &stbuf);
 
    res = internal_write(meta_path, stat_buf, STAT_LEN, (off_t)0, &wr_fi, FALSE);
    if(res < 0) {
      internal_release(meta_path, &wr_fi);
      dedupe_fs_unlock(ab_path, fi->fh);
      return res;
    }
 
    internal_release(meta_path, &wr_fi);
  }

  dedupe_fs_unlock(ab_path, fi->fh);

#ifdef DEBUG
  sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  return read;
}

static int dedupe_fs_write(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi) {

  int res = 0, meta_file = 0, read = 0;

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

  struct stat meta_stbuf = {0};
  struct stat stbuf = {0};

  struct fuse_file_info meta_fi = {0};
  struct fuse_file_info hash_fi = {0};

#ifdef DEBUG
  sprintf(out_buf, "[%s] entry\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  printf("[%s] path [%s] size [%ld] off [%ld]\n", __FUNCTION__, path, size, offset);

  dedupe_fs_metadata_path(meta_path, path);

  dedupe_fs_filestore_path(ab_path, path);

  dedupe_fs_lock(ab_path, fi->fh);

  res = internal_getattr(meta_path, &meta_stbuf);

  if(res != -ENOENT) {

    printf("not a first time\n");

    meta_file = TRUE;
    meta_fi.flags = O_RDONLY;
    res = internal_open(meta_path, &meta_fi);
    if(res < 0) {
      ABORT;
    }

    res = internal_read(meta_path, stat_buf, STAT_LEN, (off_t)0, &meta_fi, FALSE);
    if(res <= 0) {
      ABORT;
    }

    char2stbuf(stat_buf, &stbuf);
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
          ABORT;
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
            ABORT;
          }

          r_cnt = internal_read(srchstr, write_buf+read, req_off_len, req_off_st-st_off, &hash_fi, FALSE);
          if(r_cnt <= 0) {
            ABORT;
          }

          res = internal_release(srchstr, &hash_fi);
          if(res < 0) {
            ABORT;
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
      if(stbuf.st_size > req_size_st)
        req_size_len = (((offset+size)/MINCHUNK + 1) * MINCHUNK);
      else
        req_size_len = req_size_st;

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
          ABORT;
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
           ABORT;
          }
 
          r_cnt = internal_read(srchstr, write_buf+read, req_size_len, req_size_st-st_off, &hash_fi, FALSE);
          if(r_cnt <= 0) {
            ABORT;
          }

          res = internal_release(srchstr, &hash_fi);
          if(res < 0) {
            ABORT;
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

    res = internal_write(ab_path, write_buf, write_len, (offset/MINCHUNK)*MINCHUNK, fi, TRUE);
    if(res < 0) {
      dedupe_fs_unlock(ab_path, fi->fh);
      ABORT;
    }

    block_num = offset / MINCHUNK;
    bitmask[block_num/32] |= 1<<(block_num%32);

  } else {

    printf("First time write\n");

    memcpy(write_buf+(offset%MINCHUNK), buf, size);

    res = internal_write(ab_path, write_buf+(offset%MINCHUNK), size, offset, fi, TRUE);
    if(res < 0) {
      dedupe_fs_unlock(ab_path, fi->fh);
      return res;
    }

    block_num = offset / MINCHUNK;
    bitmask[block_num/32] |= 1<<(block_num%32);

  }

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

#ifdef DEBUG
  sprintf(out_buf, "[%s] entry\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  res = munmap((void*)bitmask, BITMASK_LEN);
  if(res < 0) {
    res = -errno;
    return res;
  }

  printf("file unmaped\n");

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

  time_t tm;

  off_t cur_off = 0, hash_off = 0, rem_size = 0;
  off_t st_off = 0, end_off = 0, num_hash_lines = 0;

  char *hash = NULL, *sha1_out = NULL, *saveptr = NULL;

  char stat_buf[STAT_LEN] = {0};
  char out_buf[BUF_LEN] = {0};
  char ab_path[MAX_PATH_LEN] = {0};
  char meta_path[MAX_PATH_LEN] = {0};
  char hash_line[OFF_HASH_LEN] = {0};
  char filechunk[MAXCHUNK+1] = {0};
  char meta_data[OFF_HASH_LEN] = {0};

  struct stat stbuf = {0};
  struct rlimit rlim = {0};
  struct fuse_file_info fi = {0};

#ifdef DEBUG
  sprintf(out_buf, "[%s] entry\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  getrlimit(RLIMIT_FSIZE, &rlim);

  if(newsize < 0 || newsize > rlim.rlim_cur) {
    res = -EINVAL;
    return res;
  }

  dedupe_fs_filestore_path(ab_path, path);

  res = truncate(ab_path, newsize);
  if(SUCCESS == res) {
    return res;
  }

  dedupe_fs_metadata_path(meta_path, path);

  fi.flags = O_RDWR;
  res = internal_open(meta_path, &fi);
  if(res < 0) {
    return res;
  }

  res = internal_read(meta_path, stat_buf, STAT_LEN, (off_t)0, &fi, FALSE);
  if(res <= 0) {
    internal_release(meta_path, &fi);
    return res;
  }

  char2stbuf(stat_buf, &stbuf);

  hash_off = STAT_LEN;

  if(newsize == stbuf.st_size) {
    internal_release(meta_path, &fi);
    return SUCCESS;
  } else if (newsize < stbuf.st_size) {

    while(TRUE) {
      res = internal_read(meta_path, hash_line, OFF_HASH_LEN, hash_off, &fi, FALSE);
      if(res <= 0) {
        internal_release(meta_path, &fi);
        return res;
      }

      st_off = (off_t)atoll(strtok_r(hash_line, ":", &saveptr));
      end_off = (off_t)atoll(strtok_r(NULL, ":", &saveptr));
      hash = strtok_r(NULL, ":", &saveptr);
 
      hash[strlen(hash)-1] = '\0';

      hash_off += OFF_HASH_LEN;

      if((newsize-1) >= st_off && (newsize-1) <= end_off) {
        break;
      }
    }

    // TODO incorporate unlink for all the hashes
    // Create new hash for the last block

  } else {
    // Find the last hashline in the metadata file
    cur_off = stbuf.st_size - STAT_LEN;
    num_hash_lines = cur_off / OFF_HASH_LEN;

    hash_off = cur_off + (num_hash_lines -1) * OFF_HASH_LEN;

    res = internal_read(meta_path, hash_line, OFF_HASH_LEN, hash_off, &fi, FALSE);
    if(res <= 0) {
      internal_release(meta_path, &fi);
      return res;
    }

    st_off = (off_t)atoll(strtok_r(hash_line, ":", &saveptr));
    end_off = (off_t)atoll(strtok_r(NULL, ":", &saveptr));

    rem_size = newsize - (end_off + 1);

    while(rem_size > MAXCHUNK) {

      memset(filechunk, 0, MAXCHUNK);
      sha1_out = sha1(filechunk, MAXCHUNK);
      create_chunkfile(filechunk, sha1_out, MAXCHUNK);

      memset(meta_data, 0, OFF_HASH_LEN);
      st_off = end_off + 1;
      end_off += MAXCHUNK;
      hash_off += OFF_HASH_LEN;

      snprintf(meta_data, OFF_HASH_LEN, "%lld:%lld:%s\n", st_off, end_off, sha1_out);
      res = internal_write(meta_path, meta_data, OFF_HASH_LEN, hash_off, &fi, FALSE);
      if(res < 0) {
        internal_release(meta_path, &fi);
        return res;
      }

      rem_size -= MAXCHUNK;
      free(sha1_out);
      sha1_out = NULL;
    }

    memset(filechunk, 0, rem_size);
    sha1_out = sha1(filechunk, rem_size);
    create_chunkfile(filechunk, sha1_out, rem_size);

    memset(meta_data, 0, OFF_HASH_LEN);
    st_off = end_off + 1;
    end_off += rem_size;
    hash_off += OFF_HASH_LEN;

    snprintf(meta_data, OFF_HASH_LEN, "%lld:%lld:%s\n", st_off, end_off, sha1_out);
    res = internal_write(meta_path, meta_data, OFF_HASH_LEN, hash_off, &fi, FALSE);
    if(res < 0) {
      internal_release(meta_path, &fi);
      return res;
    }

    free(sha1_out);
    sha1_out = NULL;
  }

  // update st_mtime and st_ctime during truncate
  time(&tm);
  stbuf.st_size = newsize;
  stbuf.st_mtime = tm;
  stbuf.st_ctime = tm;

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

  int res = 0;

  char out_buf[BUF_LEN] = {0};
  char ab_path[MAX_PATH_LEN] = {0};
  char bitmask_file_path[MAX_PATH_LEN] = {0};

  struct fuse_file_info bitmask_fi;

#ifdef DEBUG
  sprintf(out_buf, "[%s] entry\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  dedupe_fs_filestore_path(ab_path, path);
  
  res = internal_create(ab_path, mode, fi);
  if(res < 0) {
    return res;
  }

  dedupe_fs_filestore_path(bitmask_file_path, path);
  strcat(bitmask_file_path, BITMASK_FILE);

  res = internal_mknod(bitmask_file_path, mode, 0);
  if(res < 0) {
    return res;
  }

  bitmask_fi.flags = O_RDWR;
  res = internal_open(bitmask_file_path, &bitmask_fi);
  if(res < 0) {
    return res;
  }

  res = internal_write(bitmask_file_path, "", 1, (off_t)BITMASK_LEN - 1, &bitmask_fi, FALSE);
  if(res < 0) {
    return res;
  }

  bitmask = (unsigned int *) mmap(NULL, BITMASK_LEN, PROT_READ | PROT_WRITE, MAP_SHARED, (int)bitmask_fi.fh, (off_t)0);
  if(bitmask == MAP_FAILED) {
    sprintf(out_buf, "[%s] mmap failed on [%s]", __FUNCTION__, bitmask_file_path);
    perror(out_buf);
    res = -errno;
    return res;
  }

  internal_release(bitmask_file_path, &bitmask_fi);

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
