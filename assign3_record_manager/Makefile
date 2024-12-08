# This Makefile is used to compile and link the test_assign3 program.

# Declare phony targets to avoid conflicts with files of the same name.
.PHONY: all clean

# The 'all' target is the default when 'make' is run without arguments.
all: test_assign3

# Compile and link the program 'test_assign3'.
test_assign3: storage_mgr.c test_assign3_1.c dberror.c buffer_mgr_stat.c buffer_mgr.c record_mgr.c rm_serializer.c expr.c
	
	gcc -o test_assign3 storage_mgr.c test_assign3_1.c dberror.c buffer_mgr_stat.c buffer_mgr.c record_mgr.c rm_serializer.c expr.c

	@echo "Compilation done!"

# "remove" or "clean" command disposes temporary compiled files
remove clean:
	@if [ -f "test_assign3" ] || [ -f "test_table_r" ]; then \
		if [ -f "test_assign3" ]; then \
			echo "Removing test_assign3..."; \
			rm test_assign3; \
		fi; \
		if [ -f "test_table_r" ]; then \
			echo "Removing test_table_r..."; \
			rm test_table_r; \
		fi; \
	else \
		echo "Files already disposed."; \
	fi