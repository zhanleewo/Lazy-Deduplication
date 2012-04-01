#include <stdio.h>
#include <stdarg.h>
#include <stdlib.h>
#include <math.h>
#include "rabin-karp.h"
#include "sha1.h"

extern char *dedupe_hashes;
extern char *nlinks;

extern int dedupe_fs_open(const char *, struct fuse_file_info *);
extern int dedupe_fs_create(const char *, mode_t, struct fuse_file_info *);
extern int dedupe_fs_opendir(const char *, struct fuse_file_info *);
extern int dedupe_fs_mkdir(const char *, mode_t);
extern int dedupe_fs_releasedir(const char *, struct fuse_file_info *);
extern int dedupe_fs_read(const char *, char *, size_t, off_t, struct fuse_file_info *);
extern int dedupe_fs_write(const char *, char *, size_t, off_t, struct fuse_file_info *);

unsigned long long int hash_prev=0;
unsigned long long int hash_current=0;

int pattern_match(unsigned long long int rkhash)
{
  unsigned long long int num; // Based on the value of MODULO and BASE , the datatype of this has to be changed
  num = (rkhash & (unsigned long long int)BITMASK);
  if(num == BITMASK)
  {
    return TRUE;
  }
  else
  {
    return FALSE;
  }
}

unsigned long long int Rabin_Karp_Hash(char substring[],unsigned long long int start_index,unsigned long long int end_index) 
{
  unsigned long long int i,power;
  if(start_index==0)
  {
    for(i=start_index;i<=end_index;i++)
    {
      power = (unsigned long long int) (pow(BASE,(SUBSTRING_LEN-1-i)));
      hash_current += (((substring[i] % MODULO_PRIME) * ((power)%MODULO_PRIME))%MODULO_PRIME);
    }
  }
  else
  {
    hash_current=((((hash_prev%MODULO_PRIME - ((substring[start_index-1] % MODULO_PRIME) * (((unsigned long long int)pow(BASE,SUBSTRING_LEN-1)) % MODULO_PRIME) % MODULO_PRIME ) % MODULO_PRIME) * (BASE % MODULO_PRIME)) % MODULO_PRIME + substring[start_index+end_index] % MODULO_PRIME) % MODULO_PRIME); 

  }
  hash_prev = hash_current;	
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
  strcat(dir_srchstr, dedupe_hashes);
  strcat(dir_srchstr, "/");
  strncat(dir_srchstr, sha1, 2);
  strcat(dir_srchstr, "/");
  strncat(dir_srchstr, sha1+2, 4);
  strcat(dir_srchstr, "/");
  strncat(dir_srchstr, sha1+6, 8);
  strcat(dir_srchstr, "/");
  strcat(dir_srchstr, sha1+14);
}

void create_chunkfile(char *filechunk, char *sha1, size_t len)
{
  unsigned long long int res = 0, nlinks_num = 0;

  char out_buf[BUF_LEN] = {0};
  char dir_srchstr[MAX_PATH_LEN] = {0};
  char file_chunk_path[MAX_PATH_LEN] = {0};
  char nlinks_path[MAX_PATH_LEN] = {0};

  char nlinks_cnt[NLINKS_WIDTH] = {0};

  struct fuse_file_info fi, nlinks_fi;

  strcpy(dir_srchstr, "/../..");
  create_dir_search_str(dir_srchstr, sha1);

  printf("dir_srchstr [%s]\n", dir_srchstr);
  res = dedupe_fs_opendir(dir_srchstr, &fi);
  if(-ENOENT == res) {

    strcpy(file_chunk_path, "/../..");
    strcat(file_chunk_path, dedupe_hashes);
    strcat(file_chunk_path, "/");
    strncat(file_chunk_path, sha1, 2);

    res = dedupe_fs_opendir(file_chunk_path, &fi);
    if(-ENOENT == res) {
      res = dedupe_fs_mkdir(file_chunk_path, 0755);
      if(res < 0) {
        sprintf(out_buf, "[%s] mkdir failed error [%d]\n", __FUNCTION__, errno);
        WR_2_STDOUT;
        exit(1);
      }
    } else {
      dedupe_fs_releasedir(file_chunk_path, &fi);
    }

    strcat(file_chunk_path, "/");
    strncat(file_chunk_path, sha1+2, 4);

    res = dedupe_fs_opendir(file_chunk_path, &fi);
    if(-ENOENT == res) {
      res = dedupe_fs_mkdir(file_chunk_path, 0755);
      if(res < 0) {
        sprintf(out_buf, "[%s] mkdir failed error [%d]\n", __FUNCTION__, errno);
        WR_2_STDOUT;
        exit(1);
      }
    } else {
      dedupe_fs_releasedir(file_chunk_path, &fi);
    }

    strcat(file_chunk_path, "/");
    strncat(file_chunk_path, sha1+6, 8);

    res = dedupe_fs_opendir(file_chunk_path, &fi);
    if(-ENOENT == res) {
      res = dedupe_fs_mkdir(file_chunk_path, 0755);
      if(res < 0) {
        sprintf(out_buf, "[%s] mkdir failed error [%d]\n", __FUNCTION__, errno);
        WR_2_STDOUT;
        exit(1);
      }
    } else {
      dedupe_fs_releasedir(file_chunk_path, &fi);
    }

    strcat(file_chunk_path, "/");
    strncat(file_chunk_path, sha1+14, 26);

    res = dedupe_fs_opendir(file_chunk_path, &fi);
    if(-ENOENT == res) {
      res = dedupe_fs_mkdir(file_chunk_path, 0755);
      if(res < 0) {
        sprintf(out_buf, "[%s] mkdir failed error [%d]\n", __FUNCTION__, errno);
        WR_2_STDOUT;
        exit(1);
      }
    } else {
      dedupe_fs_releasedir(file_chunk_path, &fi);
    }

    // create the chunk file 
    strcat(file_chunk_path, "/");
    strcat(file_chunk_path, sha1);

    printf("file_chunk_path [%s]\n", file_chunk_path);

    res = dedupe_fs_create(file_chunk_path, 0755, &fi);
    if(res < 0) {
      sprintf(out_buf, "[%s] creat failed error [%d]\n", __FUNCTION__, errno);
      WR_2_STDOUT;
      exit(1);
    }

    res = dedupe_fs_write(file_chunk_path, filechunk, len, 0, &fi);
    if(res < 0) {
      sprintf(out_buf, "[%s] write failed error [%d]\n", __FUNCTION__, errno);
      WR_2_STDOUT;
      exit(1);
    }

    res = dedupe_fs_release(file_chunk_path, &fi);
    if(res < 0) {
      sprintf(out_buf, "[%s] release failed error [%d]\n", __FUNCTION__, errno);
      WR_2_STDOUT;
      exit(1);
    }

    // create the nlinks file
    strcpy(nlinks_path, dir_srchstr);
    strcat(nlinks_path, "/");
    strcat(nlinks_path, nlinks);

    printf("nlinks_path [%s]\n", nlinks_path);

    res = dedupe_fs_create(nlinks_path, 0755, &fi);
    if(res < 0) {
      sprintf(out_buf, "[%s] creat failed error [%d]\n", __FUNCTION__, errno);
      WR_2_STDOUT;
      exit(1);
    }

    // Setup the number of links to 1
    filechunk[0] = '1';
    filechunk[1] = '\0';
    res = dedupe_fs_write(nlinks_path, filechunk, 1, 0, &fi);
    if(res < 0) {
      sprintf(out_buf, "[%s] write failed error [%d]\n", __FUNCTION__, errno);
      WR_2_STDOUT;
      exit(1);
    }

    res = dedupe_fs_release(nlinks_path, &fi);
    if(res < 0) {
      sprintf(out_buf, "[%s] release failed error [%d]\n", __FUNCTION__, errno);
      WR_2_STDOUT;
      exit(1);
    }

  } else {

    strcpy(nlinks_path, dir_srchstr);
    strcat(nlinks_path, "/");
    strcat(nlinks_path, nlinks);

    nlinks_fi.flags = O_RDWR;
    res = dedupe_fs_open(nlinks_path, &nlinks_fi);
    if(res < 0) {
      sprintf(out_buf, "[%s] open failed error [%d]\n", __FUNCTION__, errno);
      WR_2_STDOUT;
      exit(1);
    }

    res = dedupe_fs_read(nlinks_path, nlinks_cnt, NLINKS_WIDTH, 0, &nlinks_fi);
    if(res < 0) {
      sprintf(out_buf, "[%s] read failed error [%d]\n", __FUNCTION__, errno);
      WR_2_STDOUT;
      exit(1);
    }

    /*if(FAILED == lseek(nlinks_fi.fh, 0, SEEK_SET)) {
      sprintf(out_buf, "[%s] lseek failed error [%d]\n", __FUNCTION__, errno);
      WR_2_STDOUT;
      exit(1);
    }*/

    printf("res = %d nlinks_cnt [%s]\n", res, nlinks_cnt);

    sscanf(nlinks_cnt, "%d", &nlinks_num);
    nlinks_num += 1;

    printf("link nct [%d]\n", nlinks_num);

    sprintf(nlinks_cnt, "%d", nlinks_num);

    printf("link nct [%s]\n", nlinks_cnt);

    res = dedupe_fs_write(nlinks_path, nlinks_cnt, NLINKS_WIDTH, 0, &nlinks_fi);
    if(res < 0) {
      sprintf(out_buf, "[%s] write failed error [%d]\n", __FUNCTION__, errno);
      WR_2_STDOUT;
      exit(1);
    }

    res = dedupe_fs_release(nlinks_path, &nlinks_fi);
    if(res < 0) {
      sprintf(out_buf, "[%s] release failed error [%d]\n", __FUNCTION__, errno);
      WR_2_STDOUT;
      exit(1);
    }

    res = dedupe_fs_releasedir(dir_srchstr, &fi);
    if(res < 0) {
      sprintf(out_buf, "[%s] releasedir failed error [%d]\n", __FUNCTION__, errno);
      WR_2_STDOUT;
      exit(1);
    }

  }

}

int compute_rabin_karp(char *filestore_path, file_args *f_args, struct stat *stbuf) {

  unsigned long long int res = 0, readcnt = 0, pos = 0;
  unsigned long long int nbytes = 0, old_data_len = 0;
  int flag = TRUE;

  unsigned long long int rkhash = 0;

  off_t stblk = 0, endblk = 0;
  off_t read_off = 0, write_off = 0, st_off = 0;

  char out_buf[BUF_LEN], *sha1_out = NULL;
  char filedata[MAXCHUNK + 1] = {0};
  char temp_data[MAXCHUNK + 1] = {0};
  char filechunk[MAXCHUNK + 1] = {0};
  char meta_data[OFF_HASH_LEN] = {0};
  struct fuse_file_info fi;

  nbytes = MAXCHUNK;
  write_off = f_args->offset;

  printf("filestore_path [%s]\n", filestore_path);
  fi.flags = O_RDONLY;
  res = dedupe_fs_open(filestore_path, &fi);
  if(res < 0) {
    sprintf(out_buf, "[%s] open failed error [%d]\n", __FUNCTION__, errno);
    WR_2_STDOUT;
    ABORT;
  }

  while(TRUE) {

    memcpy(filedata, temp_data, old_data_len);
    nbytes = MAXCHUNK - old_data_len;
    st_off = read_off - old_data_len;

    if(read_off < stbuf->st_size) {
      res = dedupe_fs_read(filestore_path, filedata, nbytes, read_off, &fi);
      if(res < 0) {
        sprintf(out_buf, "[%s] read failed error [%d]\n", __FUNCTION__, errno);
        WR_2_STDOUT;
        abort();
      }
    } else {
      res = 0;
    }

    readcnt += res;
    read_off += res;

    stblk = endblk = 0;
    pos = (stblk + MINCHUNK) - SUBSTRING_LEN;
    endblk = SUBSTRING_LEN - 1 + pos;

    if(endblk > old_data_len + res) {
      endblk = old_data_len + res;
    }

    while(TRUE) {

      rkhash = Rabin_Karp_Hash(filedata, pos, endblk);

      if(SUCCESS == pattern_match(rkhash)) {

        copy_substring(filedata, filechunk, stblk, endblk);
        sha1_out = sha1(filechunk, endblk-stblk+1);

        // TODO Check if the chunk (hashdata) already exists in the db
        create_chunkfile(filechunk, sha1_out, endblk-stblk+1);

        memset(meta_data, 0, OFF_HASH_LEN);
        snprintf(meta_data, OFF_HASH_LEN, "%lld:%lld:%s\n", st_off, st_off+endblk, sha1_out);

        dedupe_fs_write(f_args->path, meta_data, strlen(meta_data), write_off, f_args->fi);
     
        write_off += strlen(meta_data);
     
        free(sha1_out);
        sha1_out = NULL;
     
        old_data_len = MAXCHUNK - endblk - 1;
        if(old_data_len > 0) {
          memset(temp_data, 0, MAXCHUNK);
          memcpy(temp_data, filedata + endblk + 1, old_data_len);
        }

        break;

      } else if(((endblk-stblk+1) == MAXCHUNK) ||
          (readcnt == stbuf->st_size)) {


        if((readcnt == stbuf->st_size) &&
            (endblk < old_data_len+res)) {
          endblk = old_data_len + res;
        }
        copy_substring(filedata, filechunk, stblk, endblk);
        sha1_out = sha1(filechunk, endblk-stblk+1);
        // TODO Check if the chunk (hashdata) already exists in the db
        create_chunkfile(filechunk, sha1_out, endblk-stblk+1);

        memset(meta_data, 0, OFF_HASH_LEN);
        snprintf(meta_data, OFF_HASH_LEN, "%lld:%lld:%s\n", st_off, st_off+endblk, sha1_out);

        dedupe_fs_write(f_args->path, meta_data, strlen(meta_data), write_off, f_args->fi);
        write_off += strlen(meta_data);
        old_data_len = 0;

        free(sha1_out);
        sha1_out = NULL;

        break;

      } else  {
        pos++;
        endblk++;
      }
    }

    if(readcnt == stbuf->st_size) {
      break;
    }
  }

  res = dedupe_fs_release(filestore_path, &fi);
  if(res < 0) {
    sprintf(out_buf, "[%s] release failed on [%s] errno [%d]\n", __FUNCTION__,filestore_path, errno);
    WR_2_STDOUT;
    ABORT;
  }
}
