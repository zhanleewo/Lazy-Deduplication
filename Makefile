CC=gcc
SRC=dedupe_fs.c lazy_worker.c debug_log.c rabin-karp.c sha1.c
CFLAGS=-g -w
DEBUGFLAGS=-DDEBUG -DHAVE_CONFIG_H -D_FILE_OFFSET_BITS=64 -D_REENTRANT
LINK_FLAGS=-lpthread -lm -lgcrypt -lgpg-error

dedupe: clean 
	rm -fr /tmp/dedupe_*
	mkdir /tmp/dedupe_file_store
	mkdir /tmp/dedupe_metadata
	mkdir /tmp/dedupe_hashes
	mkdir /tmp/dedupe_fs
	${CC} -Wall `pkg-config fuse --cflags --libs` ${DEBUGFLAGS} ${CFLAGS} ${SRC} -o dedupe_fs ${LINK_FLAGS}

clean:
	rm -f dedupe_fs
