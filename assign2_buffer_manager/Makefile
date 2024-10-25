CC=gcc
CFLAGS=-I.
DEPS = dberror.h storage_mgr.h buffer_mgr.h buffer_mgr_stat.h test_helper.h

OBJ = dberror.o storage_mgr.o buffer_mgr.o buffer_mgr_stat.o

all: test_assign2_1 test_assign2_2

test_assign2_1: test_assign2_1.o $(OBJ)
	$(CC) -o $@ $^ $(CFLAGS)

test_assign2_2: test_assign2_2.o $(OBJ)
	$(CC) -o $@ $^ $(CFLAGS)

%.o: %.c $(DEPS)
	$(CC) -c -o $@ $< $(CFLAGS)

.PHONY : clean
clean :
	$(RM) *.o test_assign2_1 -r
	$(RM) *.o test_assign2_2 -r
