#include "dedupe_fs.h"

void print_fuse_file_info(struct fuse_file_info *fi) {
  if(NULL != fi) {
    printf("flags:%d fh_old:%lu ", fi->flags, fi->fh_old);
    printf("writepage:%d direct_io:%u ", fi->writepage, fi->direct_io);
    printf("keep_cache:%u flush:%u ", fi->keep_cache, fi->flush);
    printf("nonseekable:%u padding:%u ", fi->nonseekable, fi->padding);
    printf("fh:%llu lock_owner:%llu\n", fi->fh, fi->lock_owner);
  }
}

