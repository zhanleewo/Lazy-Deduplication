#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include "rabin-karp.h"
#include "sha1.h"

extern int dedupe_fs_open(const char *, struct fuse_file_info *);
extern int dedupe_fs_read(const char *, char *, size_t, off_t, struct fuse_file_info *);
extern int dedupe_fs_write(const char *, char *, size_t, off_t, struct fuse_file_info *);

long long int hash_prev=0;
long long int hash_current=0;

int pattern_match(long long int rkhash)
{
  unsigned int num; // Based on the value of MODULO and BASE , the datatype of this has to be changed
  num = (rkhash & BITMASK);
  if(num)
  {
    return TRUE;
  }
  else
  {
    return FALSE;
  }
}

long int Rabin_Karp_Hash(char substring[],long long int start_index,long long int end_index) 
{
  int i,power;
  if(start_index==0)
  {
    for(i=start_index;i<=end_index;i++)
    {
      power = (int) (pow(BASE,(SUBSTRING_LEN-1-i)));
      hash_current += (((substring[i] % MODULO_PRIME) * ((power)%MODULO_PRIME))%MODULO_PRIME);
    }
  }
  else
  {
    hash_current=((((hash_prev%MODULO_PRIME - ((substring[start_index-1] % MODULO_PRIME) * (((int)pow(BASE,SUBSTRING_LEN-1)) % MODULO_PRIME) % MODULO_PRIME ) % MODULO_PRIME) * (BASE % MODULO_PRIME)) % MODULO_PRIME + substring[start_index+end_index] % MODULO_PRIME) % MODULO_PRIME); 

  }
  hash_prev = hash_current;	
  //printf("The Rabin hash string is %s\n",substring);
  return hash_current;
}

char * copy_substring(char *str, char *s, int start,int end)
{
  int i=0;
  while(start+i<=end)
  {
    s[i] = str[start+i];
    i++;
  }
  s[i]='\0';
  return s;
}

void create_chunkfile(char str[],char shastr[], size_t length)
{
  FILE *filech;
  char *filename = (char *)malloc(strlen(shastr)+4);
  strcpy(filename,shastr);
  strcat(filename,".txt");
  printf("Chunk File name - %s\n",filename);
  filech = fopen(filename,"wb");
  if(filech == NULL)
  {
    printf("File chunk not created or returned");
  }
  else
  {
    //printf("Chunk File created");
    //fprintf(filech,str);
    fwrite(str, 1, length, filech);
  }
  free(filename);
  fclose(filech);
}

int compute_rabin_karp(char *filestore_path, file_args *f_args, struct stat *stbuf) {

  int res = 0, readcnt = 0, pos = 0;
  int nbytes = 0, old_data_len = 0;
  int flag = TRUE;

  long long int rkhash = 0;
  long long int stblk = 0, endblk = 0;

  off_t read_off = 0, write_off = 0;
  char out_buf[BUF_LEN], *sha1_out = NULL;
  char filedata[MAXCHUNK + 1];
  char temp_data[MAXCHUNK + 1];
  char shadata[MAXCHUNK + 1];
  struct fuse_file_info fi;

  nbytes = MAXCHUNK;
  write_off = f_args->offset;

  res = dedupe_fs_open(filestore_path, &fi);
  if(res < 0) {
    sprintf(out_buf, "[%s] open failed error [%d]\n", __FUNCTION__, errno);
    write(1, out_buf, strlen(out_buf));
    ABORT;
  }

  while(TRUE) {

    if(old_data_len > 0) {
      memcpy(filedata, temp_data, old_data_len);
      nbytes = MAXCHUNK - old_data_len;
    }

    res = dedupe_fs_read(filestore_path, filedata, nbytes, read_off, &fi);
    if(res < 0) {
      sprintf(out_buf, "[%s] read failed error [%d]\n", __FUNCTION__, errno);
      write(1, out_buf, strlen(out_buf));
      abort();
    }

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

        copy_substring(filedata, shadata, stblk, endblk);
        sha1_out = sha1(shadata);
        //create_chunkfile(shadata, sha1_out, endblk-stblk+1);
     
        dedupe_fs_write(f_args->path, sha1_out, strlen(sha1_out), write_off, f_args->fi);
     
        write_off += strlen(sha1_out);
     
        free(sha1_out);
        sha1_out = NULL;
     
        old_data_len = MAXCHUNK - endblk;
        if(old_data_len > 0) {
          memset(temp_data, 0, MAXCHUNK);
          memcpy(temp_data, filedata + endblk + 1, old_data_len);
        }

        break;

      } else if((endblk-stblk+1) == MAXCHUNK) {

        //printf("inside Max chunk block");
        //printf("Block size is %d to %d\n",startblock, endblock);
        copy_substring(filedata, shadata, stblk, endblk);
        //printf("%s - %s\n",shastring,sha1(shastring));
        sha1_out = sha1(shadata);
        //create_chunkfile(shadata, sha1_out, endblk-stblk+1);
        dedupe_fs_write(f_args->path, sha1_out, strlen(sha1_out), write_off, f_args->fi);
        //printf("The readstring is %s\n",readfilestring);
        write_off += strlen(sha1_out);
        old_data_len = 0;

        free(sha1_out);
        sha1_out = NULL;

        break;

      } else  {
        pos++;
        endblk++;
      }
    }

    readcnt += res;
    if(readcnt == stbuf->st_size) {
      break;
    }
  }

  res = dedupe_fs_release(filestore_path, &fi);
  if(res < 0) {
    sprintf(out_buf, "[%s] release failed on [%s] errno [%d]\n", __FUNCTION__,filestore_path, errno);
    write(1, out_buf, strlen(out_buf));
    ABORT;
  }
}
