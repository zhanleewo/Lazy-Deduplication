#ifndef _INTERNAL_CMDS
#define _INTERNAL_CMDS

#include <fuse.h>

int internal_opendir(const char *, struct fuse_file_info *);

int internal_readdir(const char *, void *, fuse_fill_dir_t, off_t, struct fuse_file_info *);

int internal_open(const char *, struct fuse_file_info *);

int internal_getattr(const char *, struct stat *);

int internal_mkdir(const char *, mode_t );

int internal_rmdir(const char *);

int internal_mknod(const char *, mode_t, dev_t);

int internal_write(const char *, char *, size_t, off_t, struct fuse_file_info *, int locked);

int internal_read(const char *, char *, size_t, off_t, struct fuse_file_info *, int locked);

int internal_release(const char *, struct fuse_file_info *);

int internal_unlink(const char *);

int internal_unlink_file(const char *, int);

int internal_unlink_hash_block(const char *);

int internal_releasedir(const char *, struct fuse_file_info *); 

int internal_isdirempty(const char *,struct fuse_file_info *);

#endif
