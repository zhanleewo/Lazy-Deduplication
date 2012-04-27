#include "internal_cmds.h"
#include "rabin-karp.h"

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

int internal_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi, int locked) {

  int res = 0;
  char out_buf[BUF_LEN] = {0};

#ifdef DEBUG
  sprintf(out_buf, "[%s] entry\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  if(FALSE == locked) {
    dedupe_fs_lock(path, fi->fh);
  }

  res = pread(fi->fh, buf, size, offset);
  if(FAILED == res) {
    sprintf(out_buf, "[%s] pread failed on [%s]", __FUNCTION__);
    perror(out_buf);
    res = -errno;
  }

  if(FALSE == locked) {
    dedupe_fs_unlock(path, fi->fh);
  }

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
    sprintf(out_buf, "[%s] opendir failed on [%s]", __FUNCTION__, path);
    perror(out_buf);
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

int internal_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi) {

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
    sprintf(out_buf, "[%s] open failed on [%s]", __FUNCTION__, path);
    perror(out_buf);
    res = -errno;
    return res;
  }

  fi->fh = fd;

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
    sprintf(out_buf, "[%s] lstat failed on [%s]", __FUNCTION__, path);
    perror(out_buf);
    res = -errno;
  }

#ifdef DEBUG
  sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  return res;
}

int internal_mkdir(const char *path, mode_t mode) {

  int res = 0;
  char out_buf[BUF_LEN] = {0};

#ifdef DEBUG
  sprintf(out_buf, "[%s] entry\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  res = mkdir(path, mode);
  if(FAILED == res) {
    sprintf(out_buf, "[%s] mkdir failed on [%s]", __FUNCTION__, path);
    perror(out_buf);
    res = -errno;
  }

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
  if(FAILED == res) {
    sprintf(out_buf, "[%s] rmdir failed on [%s]", __FUNCTION__, path);
    perror(out_buf);
    res = -errno;
  }

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

  if(res < 0) {
    sprintf(out_buf, "[%s] mknod failed on [%s]", __FUNCTION__, path);
    perror(out_buf);
  }

#ifdef DEBUG
  sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  if(FAILED == res)
    return -errno;

  return res;
}

int internal_seek(const char *path, off_t offset, struct fuse_file_info *fi) {
  off_t res = 0;
  char out_buf[BUF_LEN] = {0};

#ifdef DEBUG
  sprintf(out_buf, "[%s] entry\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  res = lseek(fi->fh, offset, SEEK_SET);
  if(res == (off_t)FAILED) {
    sprintf(out_buf, "[%s] lseek failed to seek to [%lld] on [%s]", __FUNCTION__, offset, path);
    perror(out_buf);
    return -errno;
  }

#ifdef DEBUG
  sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  return SUCCESS;
}

int internal_write(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi, int locked) {

  int res;
  char out_buf[BUF_LEN] = {0};

#ifdef DEBUG
  sprintf(out_buf, "[%s] entry\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  if(FALSE == locked) {
    dedupe_fs_lock(path, fi->fh);
  }

  res = pwrite(fi->fh, buf, size, offset);
  if(FAILED == res) {
    sprintf(out_buf, "[%s] pwrite failed on [%s]", __FUNCTION__, path);
    perror(out_buf);
    res = -errno;
  }

  if(FALSE == locked) {
    dedupe_fs_unlock(path, fi->fh);
  }

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

  res = close(fi->fh); 
  if(FAILED == res) {
    sprintf(out_buf, "[%s] close failed on [%s]", __FUNCTION__, path);
    perror(out_buf);
    res = -errno;
  }

#ifdef DEBUG
  sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  return res;
}

int internal_rename(const char *path, const char *newpath) {

  int res = 0;
  char out_buf[BUF_LEN] = {0};

#ifdef DEBUG
  sprintf(out_buf, "[%s] entry\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  res = rename(path, newpath);
  if(res < 0) {
    sprintf(out_buf, "[%s] rename failed from [%s] to [%s]", __FUNCTION__, path, newpath);
    perror(out_buf);
    res = -errno;
  }

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
  if (res < 0) {
    sprintf(out_buf, "[%s] unlink failed on [%s]", __FUNCTION__, path);
    perror(out_buf);
    res = -errno;
  }

#ifdef DEBUG
  sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
  WR_2_STDOUT;
#endif
  return res;
}


int internal_unlink_hash_block(const char *sha1) {
  int res=0,nlinks_num=0,isempty=0;
  char out_buf[BUF_LEN] = {0};
  char dir_srchstr[MAX_PATH_LEN] = {0};
  char remove_path[MAX_PATH_LEN] = {0};
  char file_chunk_path[MAX_PATH_LEN] = {0};
  char nlinks_path[MAX_PATH_LEN] = {0};
  char nlinks_cnt[NLINKS_WIDTH] = {0};

  struct fuse_file_info nlinks_fi,dir_fi;

  #ifdef DEBUG
    sprintf(out_buf,"[%s] entry\n", __FUNCTION__);
    WR_2_STDOUT;	
  #endif

  create_dir_search_str(dir_srchstr, sha1);
  
  strcat(file_chunk_path,dir_srchstr);
  strcat(file_chunk_path, "/");
  strcat(file_chunk_path, sha1);

  strcpy(nlinks_path, dir_srchstr);
  strcat(nlinks_path, "/");
  strcat(nlinks_path, "nlinks.txt");  
 
  nlinks_fi.flags = O_RDWR;
  res = internal_open(nlinks_path, &nlinks_fi);
  if(res < 0) {
      exit(errno);
  }
 
  dedupe_fs_lock(nlinks_path, nlinks_fi.fh); 
 
  res = internal_read(nlinks_path, nlinks_cnt, NLINKS_WIDTH, (off_t)0, &nlinks_fi, FALSE);
    if(res < 0) {
      exit(errno);
    }

    sscanf(nlinks_cnt, "%d", &nlinks_num);
    nlinks_num -= 1;

    sprintf(nlinks_cnt, "%d", nlinks_num);

    res = internal_write(nlinks_path, nlinks_cnt, NLINKS_WIDTH, (off_t)0, &nlinks_fi, FALSE);
    if(res < 0) {
      exit(errno);
    }

    if(nlinks_num <= FALSE) {
      dedupe_fs_unlock(nlinks_path,nlinks_fi.fh);
      res = internal_release(nlinks_path, &nlinks_fi);
      if(res < 0) {
          exit(errno);
    }
    }

    else {
      res = internal_unlink(file_chunk_path);
      if(res < 0) {
	  exit(errno);
     }   

      dedupe_fs_unlock(nlinks_path,nlinks_fi.fh);
      res = internal_release(nlinks_path, &nlinks_fi);
      if(res < 0) {
          exit(errno);
    }
      res = internal_unlink(nlinks_path);
      if(res < 0) {
          exit(errno);
      }     
 
      res = internal_rmdir(dir_srchstr);
      if(res < 0) {
	  exit(errno);
      }
     
      memset(remove_path,'\0', MAX_PATH_LEN);

      strncpy(remove_path,dir_srchstr,16);
      remove_path[16]='\0';
      isempty = internal_isdirempty(remove_path,&dir_fi);
      if(isempty == TRUE)  {
          res = internal_rmdir(dir_srchstr);
          if(res < 0) {
	     exit(errno);
      }
      }
      else {
        #ifdef DEBUG
  	  sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
  	  WR_2_STDOUT;
	#endif
	return SUCCESS;
      }
     
      memset(remove_path,'\0', MAX_PATH_LEN);

      strncpy(remove_path,dir_srchstr,7);
      remove_path[7]='\0';
      isempty = internal_isdirempty(remove_path,&dir_fi);
      if(isempty == TRUE)  {
          res = internal_rmdir(dir_srchstr);
          if(res < 0) {
             exit(errno);
      }
      }
      else {
      #ifdef DEBUG
	 sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
 	 WR_2_STDOUT;
      #endif
      return SUCCESS;
      }

      memset(remove_path,'\0', MAX_PATH_LEN);

      strncpy(remove_path,dir_srchstr,2);
      remove_path[2]='\0';
      isempty = internal_isdirempty(remove_path,&dir_fi);
      if(isempty == TRUE)  {
          res = internal_rmdir(dir_srchstr);
          if(res < 0) {
             exit(errno);
      }
      }
      }

  #ifdef DEBUG
    sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
    WR_2_STDOUT;
  #endif
  return SUCCESS;

}

int internal_unlink_file(const char *path) {

  int res = 0,r_cnt=0,meta_f_readcnt=0;
  char out_buf[BUF_LEN] = {0};
  char stat_buf[STAT_LEN] = {0};
  char meta_path[MAX_PATH_LEN] = {0};
  char ab_path[MAX_PATH_LEN] = {0};
  char hash_line[OFF_HASH_LEN] = {0};
  char bitmask_file_path[MAX_PATH_LEN] = {0};

  struct stat stbuf, meta_stbuf;
  struct fuse_file_info meta_fi;

  off_t hash_off =0;
  char *sha1,*saveptr,*st,*end;
  sha1 = saveptr = st = end = NULL;
#ifdef DEBUG
  sprintf(out_buf, "[%s] entry\n", __FUNCTION__);
  WR_2_STDOUT;
#endif
  
  dedupe_fs_metadata_path(meta_path, path);
  dedupe_fs_filestore_path(ab_path, path);
  res = internal_getattr(meta_path, &meta_stbuf);

  if(res != -ENOENT) {
    meta_fi.flags = O_RDONLY;
    res = internal_open(meta_path, &meta_fi);
    if(res < 0) {
      return res;
  }
  }

  dedupe_fs_lock(meta_path,meta_fi.fh);

  res = internal_read(meta_path,stat_buf, STAT_LEN, (off_t)0, &meta_fi, TRUE);
  if(res < 0) {
     ABORT;
  }
  meta_f_readcnt += STAT_LEN;
  char2stbuf(stat_buf, &stbuf);
  hash_off = STAT_LEN;
  
  while(meta_f_readcnt < meta_stbuf.st_size) {

        memset(hash_line, 0, OFF_HASH_LEN);
        res = internal_read(meta_path, hash_line, OFF_HASH_LEN, hash_off, &meta_fi, TRUE);
        if(res < 0) {
          return res;
        }

        st = strtok_r(hash_line, ":", &saveptr);
        end = strtok_r(NULL, ":", &saveptr);
        sha1 = strtok_r(NULL, ":", &saveptr);
        sha1[strlen(sha1)-1] = '\0';

        res = internal_unlink_hash_block(sha1);
        if(res < 0) {
           return res;
        }
	meta_f_readcnt += OFF_HASH_LEN;
        hash_off += OFF_HASH_LEN;
  }

  dedupe_fs_unlock(meta_path,meta_fi.fh);
  res = internal_release(meta_path, &meta_fi);
  if(res < 0) {
     ABORT;
  }

  res = internal_unlink(meta_path);
  if(res < 0) {
     return res;
  }

  res = internal_unlink(ab_path);
  if(res < 0) {
     return res;
  }

  dedupe_fs_filestore_path(bitmask_file_path, path);
  strcat(bitmask_file_path, BITMASK_FILE);

  res = internal_unlink(bitmask_file_path);
  if(res < 0) {
     return res;
  }   

#ifdef DEBUG
   sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
   WR_2_STDOUT;
#endif
   return SUCCESS;
}

int internal_truncate(const char *path, off_t newsize) {

  int res = 0;
  char out_buf[BUF_LEN] = {0};

#ifdef DEBUG
  sprintf(out_buf, "[%s] entry\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  res = truncate(path, newsize);
  if(FAILED == res) {
    sprintf(out_buf, "[%s] truncate failed on [%s] for [%ld] size", __FUNCTION__, path, newsize);
    perror(out_buf);
    res = -errno;
    return res;
  }


#ifdef DEBUG
  sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  return SUCCESS;
}

int internal_releasedir(const char *path, struct fuse_file_info *fi) {

  int res = 0;
  char out_buf[BUF_LEN] = {0};

#ifdef DEBUG
  sprintf(out_buf, "[%s] entry\n", __FUNCTION__);
  WR_2_STDOUT;
#endif

  res = closedir((DIR *) (uintptr_t) fi->fh);
  if(FAILED == res) {
    sprintf(out_buf, "[%s] closedir failed on [%s]", __FUNCTION__, path);
    perror(out_buf);
    res = -errno;
  }

#ifdef DEBUG
  sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
  WR_2_STDOUT;
#endif
  return SUCCESS;
}

int internal_isdirempty(const char *path,struct fuse_file_info *fi) {

   int res =0,n=0;
   char out_buf[BUF_LEN] = {0}; 
   struct dirent *d;
   DIR *dir;

#ifdef DEBUG
  sprintf(out_buf, "[%s] entry\n", __FUNCTION__);
  WR_2_STDOUT;
#endif
  
   res = internal_opendir(path,fi);
   if(res < 0) {
	exit(errno);
   }

    fi->fh = (intptr_t)dir;
     
    while ((d = readdir(fi->fh)) != NULL) {
    if(++n > 2)
      break;
    }

    res = closedir((DIR *)(intptr_t)fi->fh);
    if(FAILED == res) {
      sprintf(out_buf, "[%s] closedir failed on [%s]", __FUNCTION__, path);
      perror(out_buf);
      res = -errno;
    }
     
    #ifdef DEBUG
      sprintf(out_buf, "[%s] exit\n", __FUNCTION__);
      WR_2_STDOUT;
    #endif

    if (n <= 2) 
        return TRUE;
    else
        return FALSE;
}
