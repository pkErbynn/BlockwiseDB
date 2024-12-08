
#include <stdio.h>
#include <stdlib.h>
#include<sys/stat.h>
#include<sys/types.h>
#include<unistd.h>
#include<string.h>
#include<math.h>

#include "storage_mgr.h"
#include "dberror.h"


/***********************************************
 *  Page File Management Module Implementation
 ***********************************************/

void initStorageManager (void) {
	printf("Start StorageManager Execution...");
}

// Create Page file 
RC createPageFile(char *fileName) {
// Guard clause on input validation
    if (fileName == NULL)
    {
        return RC_FILE_NOT_FOUND;
    }

    FILE *filePointer;

    filePointer = fopen(fileName, "r");     // Open file in read mode

    // If file does not exist, create new file and be ready to write in it
    if (filePointer == NULL)
    {
        filePointer = fopen(fileName, "w");
    }
 
    // If file already exists, write each page with '\0' data
    for (int i = 0; i < PAGE_SIZE; i++)
    {
        fputc('\0', filePointer);
    }

    fclose(filePointer);

    return RC_OK;
}

// Open an existing page file
RC openPageFile(char *fileName, SM_FileHandle *fileHandle) {
    FILE *file = fopen(fileName, "rb+");
    if (!file) return RC_FILE_NOT_FOUND;

    char *pageBuffer = (char *) calloc(PAGE_SIZE, sizeof(char));
    if (!pageBuffer) return RC_MEMORY_ALLOCATION_FAIL;

    if (fread(pageBuffer, PAGE_SIZE, 1, file) != 1) {
        free(pageBuffer);
        fclose(file);
        return RC_READ_FAILED;
    }

    fileHandle->fileName = fileName;
    fileHandle->totalNumPages = atoi(pageBuffer);
    fileHandle->curPagePos = 0;
    fileHandle->mgmtInfo = file;

    free(pageBuffer);
    return RC_OK;
}

// Close the page file
RC closePageFile(SM_FileHandle *fileHandle) {
   fseek(fileHandle->mgmtInfo, 0, SEEK_SET);
    char pageBuffer[PAGE_SIZE];
    snprintf(pageBuffer, PAGE_SIZE, "%d", fileHandle->totalNumPages);

    if (fwrite(pageBuffer, PAGE_SIZE, 1, fileHandle->mgmtInfo) != 1) return RC_WRITE_FAILED;

    fclose(fileHandle->mgmtInfo);
    return RC_OK;
}

// Open an existing page file
RC destroyPageFile(char *fileName) {
     for (int attempts = 0; attempts < 3; attempts++) {
        if (remove(fileName) == 0) return RC_OK;
    }
    return RC_DESTROY_FAILED;
}

// Read a block at specified page number
RC readBlock(int pageNum, SM_FileHandle *fileHandle, SM_PageHandle memPage) {
    if (pageNum < 0 || pageNum >= fileHandle->totalNumPages) return RC_READ_NON_EXISTING_PAGE;

    fseek(fileHandle->mgmtInfo, (pageNum + 1) * PAGE_SIZE, SEEK_SET);
    if (fread(memPage, PAGE_SIZE, 1, fileHandle->mgmtInfo) != 1) return RC_READ_FAILED;

    fileHandle->curPagePos = pageNum;
    return RC_OK;
}

// Read the first block
RC readFirstBlock(SM_FileHandle *fileHandle, SM_PageHandle memPage) {
    return readBlock(0, fileHandle, memPage);
}

// Read the last block
RC readLastBlock(SM_FileHandle *fileHandle, SM_PageHandle memPage) {
    return readBlock(fileHandle->totalNumPages - 1, fileHandle, memPage);
}


/**********************************
*    Reading blocks from disc
***********************************/

// Read previous block
RC readPreviousBlock(SM_FileHandle *fileHandle, SM_PageHandle memPage) {
    if (fileHandle->curPagePos <= 0) return RC_READ_NON_EXISTING_PAGE;
    return readBlock(fileHandle->curPagePos - 1, fileHandle, memPage);
}

// Read current block
RC readCurrentBlock(SM_FileHandle *fileHandle, SM_PageHandle memPage) {
    return readBlock(fileHandle->curPagePos, fileHandle, memPage);
}

// Read next block
RC readNextBlock(SM_FileHandle *fileHandle, SM_PageHandle memPage) {
    if (fileHandle->curPagePos + 1 >= fileHandle->totalNumPages) return RC_READ_NON_EXISTING_PAGE;
    return readBlock(fileHandle->curPagePos + 1, fileHandle, memPage);
}


/***************************************
*    Writing blocks to a page file 
****************************************/

// Write data to a specified block in the page file
RC writeBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
     if (pageNum < 0) return RC_WRITE_FAILED;

    FILE *file = fHandle->mgmtInfo;
    fseek(file, (pageNum + 1) * PAGE_SIZE, SEEK_SET);
    if (fwrite(memPage, sizeof(char), PAGE_SIZE, file) != PAGE_SIZE) return RC_WRITE_FAILED;

    fHandle->curPagePos = pageNum;
    return RC_OK;
}

// Write data to the current block in the page file
RC writeCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    return writeBlock(fHandle->curPagePos, fHandle, memPage);
}

// Append a new empty block at the end of the file
RC appendEmptyBlock(SM_FileHandle *fHandle) {
    char *emptyPage = (char *)calloc(PAGE_SIZE, sizeof(char));
    if (!emptyPage) return RC_MEMORY_ALLOCATION_FAIL;

    fseek(fHandle->mgmtInfo, 0, SEEK_END);
    if (fwrite(emptyPage, sizeof(char), PAGE_SIZE, fHandle->mgmtInfo) != PAGE_SIZE) {
        free(emptyPage);
        return RC_WRITE_FAILED;
    }

    free(emptyPage);
    fHandle->totalNumPages++;
    fHandle->curPagePos = fHandle->totalNumPages - 1;
    return RC_OK;
}

// Ensure the file has at least a certain number of pages by appending empty blocks
RC ensureCapacity(int numberOfPages, SM_FileHandle *fHandle) {
    while (fHandle->totalNumPages < numberOfPages) {
        RC status = appendEmptyBlock(fHandle);
        if (status != RC_OK) return status;
    }
    return RC_OK;
}

