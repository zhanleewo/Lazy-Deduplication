#include <stdio.h>
#include <stdarg.h>
#include <stdlib.h>
#include <math.h>
#include "rabin-karp.h"
#include "internal_cmds.h"
#include "sha1.h"

unsigned long long int RM=1;

extern char *dedupe_hashes;
extern char *nlinks;

extern int internal_open(const char *, struct fuse_file_info *);
extern int internal_create(const char *, mode_t, struct fuse_file_info *);
extern int internal_opendir(const char *, struct fuse_file_info *);
extern int internal_mkdir(const char *, mode_t);
extern int internal_releasedir(const char *, struct fuse_file_info *);
extern int internal_read(const char *, char *, size_t, off_t, struct fuse_file_info *, int locked);
extern int internal_write(const char *, char *, size_t, off_t, struct fuse_file_info *, int locked);

void precompute_RM()
{
        int i;
        for(i=1;i<=SUBSTRING_LEN-1;i++)
                RM = (R * RM)%Q;
}

int pattern_match(unsigned long long int rkhash)
{
        unsigned long long int num;
        num = (rkhash & (unsigned long long int)BITMASK);
        if(num == FALSE)
        {
                return TRUE;
        }
        else
        {
                return FALSE;
        }
}


unsigned long long int Rabin_Karp_Hash(char *substring, unsigned long long int start_index, unsigned long long int end_index, int newchunk, unsigned long long int hash_prev)
{
        unsigned long long int i,hash_current = 0;
        if(newchunk==0)
        {
                for(i=0;i<=SUBSTRING_LEN-1;i++)
                        hash_current = (R * hash_current + substring[start_index+i])%Q;
                        // Depending on the values have to modulo each value again to avoid overflow
        }
        else
        {
                hash_current = (hash_prev + Q - RM*substring[start_index-1] % Q) % Q;
                hash_current = (hash_current*R + substring[end_index]) % Q;
        }

        return hash_current;
}



char* copy_substring(char *str, char *s, unsigned long long int start,unsigned long long int end)
{
  unsigned long long int i=0;

  while(start+i<=end)
  {
    s[i] = str[start+i];
    i++;
  }
  s[i]='\0';
  return s;
}

void create_dir_search_str(char *dir_srchstr, char *sha1) {
  strcpy(dir_srchstr, dedupe_hashes);
  strcat(dir_srchstr, "/");
  strncat(dir_srchstr, sha1, 2);
  strcat(dir_srchstr, "/");
  strncat(dir_srchstr, sha1+2, 4);
  strcat(dir_srchstr, "/");
  strncat(dir_srchstr, sha1+6, 8);
  strcat(dir_srchstr, "/");
  strcat(dir_srchstr, sha1+14);
}

void create_chunkfile(char *filechunk, char *sha1, size_t len) {
  unsigned long long int res = 0, nlinks_num = 0;

  char out_buf[BUF_LEN] = {0};
  char dir_srchstr[MAX_PATH_LEN] = {0};
  char file_chunk_path[MAX_PATH_LEN] = {0};
  char nlinks_path[MAX_PATH_LEN] = {0};

  char nlinks_cnt[NLINKS_WIDTH] = {0};

  struct fuse_file_info fi, nlinks_fi;

  create_dir_search_str(dir_srchstr, sha1);

  res = internal_opendir(dir_srchstr, &fi);
  if(-ENOENT == res) {

    strcpy(file_chunk_path, dedupe_hashes);
    strcat(file_chunk_path, "/");
    strncat(file_chunk_path, sha1, 2);

    res = internal_opendir(file_chunk_path, &fi);
    if(-ENOENT == res) {
      res = internal_mkdir(file_chunk_path, 0755);
      if(res < 0) {
        exit(errno);
      }
    } else {
      internal_releasedir(file_chunk_path, &fi);
    }

    strcat(file_chunk_path, "/");
    strncat(file_chunk_path, sha1+2, 4);

    res = internal_opendir(file_chunk_path, &fi);
    if(-ENOENT == res) {
      res = internal_mkdir(file_chunk_path, 0755);
      if(res < 0) {
        exit(errno);
      }
    } else {
      internal_releasedir(file_chunk_path, &fi);
    }

    strcat(file_chunk_path, "/");
    strncat(file_chunk_path, sha1+6, 8);

    res = internal_opendir(file_chunk_path, &fi);
    if(-ENOENT == res) {
      res = internal_mkdir(file_chunk_path, 0755);
      if(res < 0) {
        exit(errno);
      }
    } else {
      internal_releasedir(file_chunk_path, &fi);
    }

    strcat(file_chunk_path, "/");
    strncat(file_chunk_path, sha1+14, 26);

    res = internal_opendir(file_chunk_path, &fi);
    if(-ENOENT == res) {
      res = internal_mkdir(file_chunk_path, 0755);
      if(res < 0) {
        exit(errno);
      }
    } else {
      internal_releasedir(file_chunk_path, &fi);
    }

    // create the chunk file 
    strcat(file_chunk_path, "/");
    strcat(file_chunk_path, sha1);

    res = internal_create(file_chunk_path, 0755, &fi);
    if(res < 0) {
      exit(errno);
    }

    res = internal_write(file_chunk_path, filechunk, len, (off_t)0, &fi, FALSE);
    if(res < 0) {
      exit(errno);
    }

    res = internal_release(file_chunk_path, &fi);
    if(res < 0) {
      exit(errno);
    }

    // create the nlinks file
    strcpy(nlinks_path, dir_srchstr);
    strcat(nlinks_path, "/");
    strcat(nlinks_path, nlinks);

    res = internal_create(nlinks_path, 0755, &fi);
    if(res < 0) {
      exit(errno);
    }

    // Setup the number of links to 1
    filechunk[0] = '1';
    filechunk[1] = '\0';
    res = internal_write(nlinks_path, filechunk, 1, (off_t)0, &fi, FALSE);
    if(res < 0) {
      exit(errno);
    }

    res = internal_release(nlinks_path, &fi);
    if(res < 0) {
      exit(errno);
    }

  } else {

    strcpy(nlinks_path, dir_srchstr);
    strcat(nlinks_path, "/");
    strcat(nlinks_path, nlinks);

    nlinks_fi.flags = O_RDWR;
    res = internal_open(nlinks_path, &nlinks_fi);
    if(res < 0) {
      exit(errno);
    }

    res = internal_read(nlinks_path, nlinks_cnt, NLINKS_WIDTH, (off_t)0, &nlinks_fi, FALSE);
    if(res < 0) {
      exit(errno);
    }

    sscanf(nlinks_cnt, "%d", &nlinks_num);
    nlinks_num += 1;

    sprintf(nlinks_cnt, "%d", nlinks_num);

    res = internal_write(nlinks_path, nlinks_cnt, NLINKS_WIDTH, (off_t)0, &nlinks_fi, FALSE);
    if(res < 0) {
      exit(errno);
    }

    res = internal_release(nlinks_path, &nlinks_fi);
    if(res < 0) {
      exit(errno);
    }

    res = internal_releasedir(dir_srchstr, &fi);
    if(res < 0) {
      exit(errno);
    }
  }
}

int compute_rabin_karp(char *filestore_path, file_args *f_args, struct stat *stbuf) {

  long long int res = 0;
  unsigned long long int nbytes = 0, old_data_len = 0, pos=0;
  unsigned long long int newchunk = 0, rkhash = 0;

  off_t stblk = 0, endblk = 0;
  off_t read_off = 0, write_off = 0, st_off = -1;

  char out_buf[BUF_LEN], *sha1_out = NULL;
  char filedata[MAXCHUNK + 1] = {0};
  char temp_data[MAXCHUNK + 1] = {0};
  char filechunk[MAXCHUNK + 1] = {0};
  char meta_data[OFF_HASH_LEN] = {0};
  char bitmask_file_path[MAX_PATH_LEN] = {0};

  unsigned int *btmsk = NULL;

  struct fuse_file_info fi, bitmask_fi;

  nbytes = MAXCHUNK;
  write_off = f_args->offset;

  fi.flags = O_RDONLY;
  res = internal_open(filestore_path, &fi);
  if(res < 0) {
    ABORT;
  }

  strcpy(bitmask_file_path, filestore_path);
  strcat(bitmask_file_path, BITMASK_FILE);

  bitmask_fi.flags = O_RDWR;
  res = internal_open(bitmask_file_path, &bitmask_fi);
  if(res < 0) {
    return res;
  }

  btmsk = (unsigned int *) mmap(NULL, BITMASK_LEN, PROT_READ | PROT_WRITE, MAP_SHARED, bitmask_fi.fh, (off_t)0);
  if(btmsk == MAP_FAILED) {
    sprintf(out_buf, "[%s] mmap failed on [%s]", __FUNCTION__, bitmask_file_path);
    perror(out_buf);
    res = -errno;
    return res;
  }

  internal_release(bitmask_file_path, &bitmask_fi);

  while(TRUE) {

    memcpy(filedata, temp_data, old_data_len);
    nbytes = MAXCHUNK - old_data_len;
    st_off += endblk + 1;
    
    if(read_off < stbuf->st_size) {
      res = internal_read(filestore_path, filedata + old_data_len, nbytes, read_off, &fi, FALSE);
      if(res < 0) {
        ABORT;
      }
    } else {
	res = 0;
    }

    read_off += res;

    stblk = endblk = 0;
    pos = (stblk + MINCHUNK) - SUBSTRING_LEN;
    endblk = SUBSTRING_LEN - 1 + pos;
    newchunk = 0;
    rkhash = 0;

    if((old_data_len + res - 1) <= endblk) {
      endblk = old_data_len + res - 1;

      copy_substring(filedata, filechunk, stblk, endblk);
      sha1_out = sha1(filechunk, endblk-stblk+1);

      create_chunkfile(filechunk, sha1_out, endblk-stblk+1);

      memset(meta_data, 0, OFF_HASH_LEN);
      snprintf(meta_data, OFF_HASH_LEN, "%lld:%lld:%s\n", st_off, st_off+endblk, sha1_out);

      internal_write(f_args->path, meta_data, OFF_HASH_LEN, write_off, f_args->fi, FALSE);
      write_off += OFF_HASH_LEN;
      old_data_len = 0;

      free(sha1_out);
      sha1_out = NULL;

      break;
    }

    while(TRUE) {

      rkhash = Rabin_Karp_Hash(filedata, pos, endblk,newchunk,rkhash);

      if(TRUE == pattern_match(rkhash)) {

        copy_substring(filedata, filechunk, stblk, endblk);
        sha1_out = sha1(filechunk, endblk-stblk+1);

        create_chunkfile(filechunk, sha1_out, endblk-stblk+1);

        memset(meta_data, 0, OFF_HASH_LEN);
        snprintf(meta_data, OFF_HASH_LEN, "%lld:%lld:%s\n", st_off, st_off+endblk, sha1_out);

        internal_write(f_args->path, meta_data, OFF_HASH_LEN, write_off, f_args->fi, FALSE);
     
        write_off += OFF_HASH_LEN;
     
        free(sha1_out);
        sha1_out = NULL;
     
        old_data_len = (old_data_len + res) - (endblk + 1);
        if(old_data_len > 0) {
          memset(temp_data, 0, MAXCHUNK);
          memcpy(temp_data, filedata + endblk + 1, old_data_len);
        }

        break;

      } else if(((endblk-stblk+1) == MAXCHUNK) ||
          (st_off+endblk+1 == stbuf->st_size)) {

        copy_substring(filedata, filechunk, stblk, endblk);
        sha1_out = sha1(filechunk, endblk-stblk+1);

        create_chunkfile(filechunk, sha1_out, endblk-stblk+1);

        memset(meta_data, 0, OFF_HASH_LEN);
        snprintf(meta_data, OFF_HASH_LEN, "%lld:%lld:%s\n", st_off, st_off+endblk, sha1_out);

        internal_write(f_args->path, meta_data, OFF_HASH_LEN, write_off, f_args->fi, FALSE);
        write_off += OFF_HASH_LEN;
        old_data_len = 0;

        free(sha1_out);
        sha1_out = NULL;

        break;

      } else  {
        pos++;
        endblk++;
	newchunk++;
      }
    }

    if(st_off+endblk+1 == stbuf->st_size) {
      break;
    }
  }

  res = munmap(btmsk, BITMASK_LEN);
  if(FAILED == res) {
    ABORT;
  }

  res = internal_release(filestore_path, &fi);
  if(res < 0) {
    ABORT;
  }
}
