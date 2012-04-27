#include "dedupe_fs.h"
#include "internal_cmds.h"
#include "rabin-karp.h"

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

void updates_handler(const char *path) {

  int i = 0, j = 0, res = 0;
  int read = 0, toread = 0, todedupe = 0;

  off_t old_data_len = 0, new_data_endoff = 0;

  size_t r_cnt = 0, meta_f_readcnt = 0;
  size_t tot_file_read = 0;
  size_t new_file_size = 0;

  off_t st_hash_off = 0, end_hash_off = 0;
  off_t st_new_off = 0, end_new_off = 0;
  off_t cur_block_off = 0;

  off_t hash_off = 0;
  off_t new_meta_off = 0;

  char *st = NULL, *end = NULL;
  char *new_f_path_end = NULL;
  char *del_f_path_end = NULL;
  char *meta_f_path_end = NULL;
  char *sha1 = NULL, *saveptr = NULL;

  char hash_line[OFF_HASH_LEN] = {0};
  char new_hash_line[OFF_HASH_LEN] = {0};
  char stat_buf[STAT_LEN] = {0};
  char new_stat_buf[STAT_LEN] = {0};
  char out_buf[BUF_LEN] = {0};
  char meta_path[MAX_PATH_LEN] = {0};
  char del_path[MAX_PATH_LEN] = {0};
  char srchstr[MAX_PATH_LEN] = {0};
  char ab_path[MAX_PATH_LEN] = {0};
  char new_meta_path[MAX_PATH_LEN] = {0};
  char ab_f_path[MAX_PATH_LEN] = {0};
  char data_buf[MAXCHUNK+MINCHUNK] = {0};
  char old_data_buf[MAXCHUNK] = {0};
  char filechunk[MAXCHUNK] = {0};
  char new_sha1[HEXA_HASH_LEN] = {0};

  unsigned int *btmap = NULL;

  struct stat meta_stbuf = {0};
  struct stat metadata_stbuf = {0};
  struct stat ab_f_stbuf = {0};

  struct fuse_file_info bitmap_fi = {0};
  struct fuse_file_info new_meta_fi = {0};
  struct fuse_file_info meta_fi = {0};
  struct fuse_file_info hash_fi = {0};
  struct fuse_file_info ab_fi = {0};

  dedupe_fs_filestore_path(ab_path, path);
  dedupe_fs_metadata_path(meta_path, path);

  strcpy(ab_f_path, ab_path);

  new_f_path_end = strstr(ab_f_path, BITMAP_FILE);
  if(NULL == new_f_path_end) {
    sprintf(out_buf, "[%s] [%s] not a %s filetype\n", __FUNCTION__, ab_f_path, BITMAP_FILE);
    WR_2_STDOUT;
    ABORT;
  }
  *new_f_path_end = NULL;

  meta_f_path_end = strstr(meta_path, BITMAP_FILE);
  if(NULL == meta_f_path_end) {
    sprintf(out_buf, "[%s] [%s] not a %s filetype\n", __FUNCTION__, meta_path, BITMAP_FILE);
    WR_2_STDOUT;
    ABORT;
  }
  *meta_f_path_end = NULL;

  strcpy(new_meta_path, meta_path);
  strcat(new_meta_path, NEW_META);

  bitmap_fi.flags = O_RDWR;
  res = internal_open(ab_path, &bitmap_fi);
  if(res < 0) {
    ABORT;
  }

  btmap = (unsigned int *) mmap(NULL, BITMAP_LEN, 
                  PROT_READ | PROT_WRITE, MAP_SHARED, bitmap_fi.fh, (off_t)0);
  if(btmap == MAP_FAILED) {
    sprintf(out_buf, "[%s] mmap failed on [%s]", __FUNCTION__, path);
    perror(out_buf);
    res = -errno;
    return res;
  }

  internal_release(path, &bitmap_fi);

  ab_fi.flags = O_RDONLY;
  res = internal_open(ab_f_path, &ab_fi);
  if(res < 0) {
    ABORT;
  }

  dedupe_fs_lock(ab_f_path, ab_fi.fh);

  if(btmap[NUM_BITMAP_WORDS] == (unsigned int)-1) {

    dedupe_fs_unlock(ab_f_path, ab_fi.fh);
    internal_release(ab_f_path, &ab_fi);

    res = munmap(btmap, BITMAP_LEN);
    if(FAILED == res) {
      ABORT;
    }
    return;
  }

  res = internal_getattr(ab_f_path, &ab_f_stbuf);
  if(res < 0) {
    ABORT;
  }

  res = internal_getattr(meta_path, &meta_stbuf);
  if(res < 0) {
    ABORT;
  }

  meta_fi.flags = O_RDONLY;
  res = internal_open(meta_path, &meta_fi);
  if(res < 0) {
    ABORT;
  }

  memset(stat_buf, 0, STAT_LEN);
  res = internal_read(meta_path, stat_buf, STAT_LEN, (off_t)0, &meta_fi, FALSE);
  if(res <= 0) {
    ABORT;
  }

  char2stbuf(stat_buf, &metadata_stbuf);

  res = internal_create(new_meta_path, 0600, &new_meta_fi);
  if(res < 0) {
    ABORT;
  }

  new_meta_off = STAT_LEN;

  // TODO Read the file and recompute rabin-karp only for the
  // blocks which has been updated

  // TODO Add support for truncate and file block deletion.
  // Store the size of the updated file as well in the bitmap file
  // Check this size before updation 
  // (-1 for pwrite (or) unmodified), other size value imply the actual size

  while(i < NUM_BITMAP_WORDS) {

    cur_block_off = i * MINCHUNK;

    if(read-old_data_len > 0) {
      memcpy(data_buf, old_data_buf, read-old_data_len);
    }

    read -= old_data_len;

    while(read < MAXCHUNK) {

      if(btmap[i/32] & (1<<(i%32))) {

        res = internal_read(ab_f_path, data_buf+read, MINCHUNK, cur_block_off, &ab_fi, TRUE);
        if(res <= 0) {
          ABORT;
        }

        read += res;
        tot_file_read += res;
        cur_block_off += res;

        btmap[1/32] &= ~(1<<(i%32));

      } else {

        if(tot_file_read >= btmap[NUM_BITMAP_WORDS]) {
          i += 1;
          break;
        }

        meta_f_readcnt = STAT_LEN;
        hash_off = STAT_LEN;

        // add logic for the last file
        toread = MINCHUNK;

        while(meta_f_readcnt < meta_stbuf.st_size) {

          memset(hash_line, 0, OFF_HASH_LEN);
          res = internal_read(meta_path, hash_line, OFF_HASH_LEN, hash_off, &meta_fi, FALSE);
          if(res <= 0) {
            ABORT;
          }

          st = strtok_r(hash_line, ":", &saveptr);
          st_hash_off = (off_t)atoll(st);
     
          end = strtok_r(NULL, ":", &saveptr);
          end_hash_off = (off_t)atoll(end);
     
          sha1 = strtok_r(NULL, ":", &saveptr);
          sha1[strlen(sha1)-1] = '\0';
     
          if(cur_block_off >= st_hash_off && cur_block_off <= end_hash_off) {
     
            create_dir_search_str(srchstr, sha1);
            strcat(srchstr, "/");
            strcat(srchstr, sha1);
         
            hash_fi.flags = O_RDONLY;
            res = internal_open(srchstr, &hash_fi);
            if(res < 0) {
              ABORT;
            }

            r_cnt = internal_read(srchstr, data_buf+read, toread, cur_block_off-st_hash_off, &hash_fi, FALSE);
            if(r_cnt < 0) {
              ABORT;
            }
         
            res = internal_release(srchstr, &hash_fi);
            if(res < 0) {
              ABORT;
            }
     
            toread -= r_cnt;
            read += r_cnt;
            tot_file_read += r_cnt;
            cur_block_off += r_cnt;
          }
     
          if(toread <= 0) {
            break;
          }

          hash_off += OFF_HASH_LEN;
          meta_f_readcnt += OFF_HASH_LEN;
        }
      }

      i += 1;
    }

    if(read > MAXCHUNK) {
      todedupe = MAXCHUNK;
    } else {
      todedupe = read;
    }

    if(read > 0) {
      dedupe_data_buf(data_buf, &new_data_endoff, todedupe, new_sha1);

      copy_substring(data_buf, filechunk, (off_t)0, new_data_endoff);
      /* If chunk already exists don't do anything */
      create_chunkfile(filechunk, new_sha1, new_data_endoff+1);

      old_data_len = new_data_endoff+1;
      if(read-old_data_len > 0) {
        memcpy(old_data_buf, data_buf+old_data_len, read-old_data_len);
      }

      if(st_new_off == 0)
        end_new_off += new_data_endoff;
      else
        end_new_off += old_data_len;

      memset(new_hash_line, 0, OFF_HASH_LEN);
      snprintf(new_hash_line, OFF_HASH_LEN, "%lld:%lld:%s\n", st_new_off, end_new_off, new_sha1);
      internal_write(new_meta_path, new_hash_line, OFF_HASH_LEN, new_meta_off, &new_meta_fi, FALSE);
      new_file_size += end_new_off-st_new_off+1;

      new_meta_off += OFF_HASH_LEN;

      st_new_off = end_new_off+1;

    } else {
      old_data_len = 0;
    }
  }

  /* Change the stat structure of the modified file */
  ab_f_stbuf.st_dev = metadata_stbuf.st_dev;
  ab_f_stbuf.st_ino = metadata_stbuf.st_ino;
  ab_f_stbuf.st_mode = metadata_stbuf.st_mode;
  ab_f_stbuf.st_nlink = metadata_stbuf.st_nlink;
  ab_f_stbuf.st_uid = metadata_stbuf.st_uid;
  ab_f_stbuf.st_gid = metadata_stbuf.st_gid;
  ab_f_stbuf.st_rdev = metadata_stbuf.st_rdev;
  ab_f_stbuf.st_size = new_file_size;
  ab_f_stbuf.st_blksize = metadata_stbuf.st_blksize;
  ab_f_stbuf.st_blocks = metadata_stbuf.st_blocks;

  memset(new_stat_buf, STAT_LEN, 0);
  stbuf2char(new_stat_buf, &ab_f_stbuf);

  internal_write(new_meta_path, new_stat_buf, STAT_LEN, 0, &new_meta_fi, FALSE);

  internal_release(new_meta_path, &new_meta_fi);

  internal_release(meta_path, &meta_fi);

  strcpy(del_path, path);
  del_f_path_end = strstr(del_path, BITMAP_FILE);

  if(NULL == del_f_path_end) {
    sprintf(out_buf, "[%s] [%s] not a %s filetype\n", __FUNCTION__, del_path, BITMAP_FILE);
    WR_2_STDOUT;
    ABORT;
  }
  *del_f_path_end = NULL;

  internal_unlink_file(del_path, FALSE);

  res = internal_rename(new_meta_path, meta_path);
  if(res < 0) {
    ABORT;
  }

  /* Truncate the file in the filestore */

  internal_truncate(ab_f_path, (off_t)0);

  btmap[NUM_BITMAP_WORDS] = (unsigned int)-1;

  dedupe_fs_unlock(ab_f_path, ab_fi.fh);
  internal_release(ab_f_path, &ab_fi);

  res = munmap(btmap, BITMAP_LEN);
  if(FAILED == res) {
    ABORT;
  }
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
  struct fuse_file_info bitmap_fi;
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
 
      if((new_f_path_end = strstr(new_f_path, BITMAP_FILE)) != NULL) {

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

    sleep(20);
  }
}
