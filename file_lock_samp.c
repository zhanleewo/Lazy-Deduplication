#include <stdio.h>
#include <unistd.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>

#include <string.h>

#include <pthread.h>

void *func_th(void *arg) {
  int fd_w = 0, ret = 0;
  char buf[100];

  sprintf(buf, "[%s] executing\n", __FUNCTION__);
  write(1, buf,strlen(buf));

  fd_w = open("testfile", O_RDONLY);
  if(-1 == fd_w) {
    perror("error opening file");
  } else {
    printf("[%s] open pass\n", __FUNCTION__);
  }

  ret = flock(fd_w, LOCK_EX);
  if(-1 == ret) {
    perror("[func_th] error");
  } else {
    printf("[%s] Write lock acquired\n", __FUNCTION__);
  }

  ret = flock(fd_w, LOCK_UN);
  if(-1 == ret) {
    perror("[func_th] error");
  } else {
    printf("[%s] Write unlock\n", __FUNCTION__);
  }

  close(fd_w);

  pthread_exit(NULL);
}

int main(void) {
  int fd_r = 0, fd_w = 0;
  int ret = 0;

  pthread_t th;

  fd_r = open("testfile", O_RDONLY);
  if(fd_r == -1) {
    perror("Error in file open O_RDONLY");
  } else {
    printf("[%s] open pass\n", __FUNCTION__);
  }

  fd_w = open("testfile", O_WRONLY);
  if(fd_w == -1) {
    perror("Error in file open O_WRONLY");
  } else {
    printf("[%s] open pass\n", __FUNCTION__);
  }

  ret = flock(fd_r, LOCK_EX);
  if(ret == -1) {
    perror("Error in flock LOCK_EX read");
  } else {
    printf("[%s] Read lock acquired\n", __FUNCTION__);
    ret = flock(fd_r, LOCK_UN);
    if(-1 == ret) {
      perror("error in flock unlock");
    }
  }

  ret = flock(fd_w, LOCK_EX);
  if(ret == -1) {
    perror("Error in flock F_TLOCK write");
  } else {
    printf("[%s] Write lock acquired\n", __FUNCTION__);
  }

  pthread_create(&th, NULL, func_th, NULL);

  pthread_exit(NULL);

  ret = flock(fd_w, LOCK_UN);
  if(ret == -1) {
    perror("Error in flock LOCK_UN write");
  } else {
    printf("[%s] Unlocked write\n", __FUNCTION__);
  }

  close(fd_r);
  close(fd_w);
  return 0;
}
