#include "storage_mgr.h"
#include "dberror.h"
#include <stdio.h>
#include <stdlib.h>

int curPagePos;

/* Manipulating page files - Begin */

/* Initialize storage manager */
void initStorageManager(void) {
    printf("Begin Execution");
}

/**
 * Create a new page file with an initial size of one page, filled with '\0' bytes.
 */
RC createPageFile(char *fileName) {
    FILE *fPtr = fopen(fileName, "w"); // Open the file in write mode to create it

    if (fPtr == NULL) {
        return RC_FILE_NOT_FOUND;
    }

    // Fill the single page with '\0' bytes
    for (int i = 0; i < PAGE_SIZE; i++) {
        fputc('\0', fPtr);
    }

    fclose(fPtr); // Close the file
    return RC_OK;
}

/**
 * Open an existing page file. Initialize the file handle with information about the file.
 */
RC openPageFile(char *fileName, SM_FileHandle *fHandle) {
    // Input guards
    if (fileName == NULL) return RC_FILE_NOT_FOUND;
    if (fHandle == NULL) return RC_FILE_HANDLE_NOT_INIT;

    FILE *fPtr = fopen(fileName, "r"); // Open the file in read mode

    if (fPtr == NULL) {
        printError(RC_FILE_NOT_FOUND);
        return RC_FILE_NOT_FOUND;
    }

    fHandle->fileName = fileName;
    fHandle->mgmtInfo = fPtr;

    fseek(fPtr, 0, SEEK_END);             // Move the pointer to the end of the file
    long fileSize = ftell(fPtr);          // Get the total file size
    fHandle->totalNumPages = fileSize / PAGE_SIZE;

    fseek(fPtr, 0, SEEK_SET);             // Move the pointer back to the beginning of the file
    fHandle->curPagePos = 0;              // Initialize current page position to the start

    return RC_OK;
}

/**
 * Close an open page file.
 */
RC closePageFile(SM_FileHandle *fHandle) {
    if (fHandle == NULL) return RC_FILE_HANDLE_NOT_INIT;

    if (fHandle->mgmtInfo != NULL) {
        fclose(fHandle->mgmtInfo);  // Close the file
        fHandle->mgmtInfo = NULL;
        fHandle->fileName = NULL;
        return RC_OK;
    }

    printError(RC_FILE_NOT_FOUND);
    return RC_FILE_NOT_FOUND;  // If the file handle's file pointer is NULL, return an error
}

/**
 * Destroy (delete) a page file.
 */
RC destroyPageFile(char *fileName) {
    if (remove(fileName) == 0) {
        return RC_OK;  // File successfully deleted
    }

    printError(RC_FILE_NOT_FOUND);
    return RC_FILE_NOT_FOUND;
}

/* Manipulating page files - End */

/* Reading blocks from disk - Begin */

/**
 * Read the block at position pageNum from a file and store its content in the memory pointed to by memPage.
 */
RC readBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
    if (fHandle == NULL) return RC_FILE_HANDLE_NOT_INIT;

    if (pageNum < 0 || pageNum >= fHandle->totalNumPages) {
        printError(RC_READ_NON_EXISTING_PAGE);
        return RC_READ_NON_EXISTING_PAGE;
    }

    fseek(fHandle->mgmtInfo, pageNum * PAGE_SIZE, SEEK_SET);

    if (fread(memPage, sizeof(char), PAGE_SIZE, fHandle->mgmtInfo) != PAGE_SIZE) {
        printError(RC_READ_NON_EXISTING_PAGE);
        return RC_READ_NON_EXISTING_PAGE;
    }

    fHandle->curPagePos = pageNum;
    return RC_OK;
}

/**
 * Retrieve the current page position in a file.
 */
int getBlockPos(SM_FileHandle *fHandle) {
    if (fHandle == NULL) return RC_FILE_HANDLE_NOT_INIT;

    return fHandle->curPagePos;
}

/**
 * Read the first page in a file.
 */
RC readFirstBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    return readBlock(0, fHandle, memPage);
}

/**
 * Read the previous page relative to the curPagePos of the file.
 */
RC readPreviousBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    return readBlock(getBlockPos(fHandle) - 1, fHandle, memPage);
}

/**
 * Read the current page relative to the curPagePos of the file.
 */
RC readCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    return readBlock(getBlockPos(fHandle), fHandle, memPage);
}

/**
 * Read the next page relative to the curPagePos of the file.
 */
RC readNextBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    return readBlock(getBlockPos(fHandle) + 1, fHandle, memPage);
}

/**
 * Read the last page in a file.
 */
RC readLastBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    return readBlock(fHandle->totalNumPages - 1, fHandle, memPage);
}

/* Reading blocks from disk - End */

/* Writing blocks to a page file - Begin */

/**
 * Write a page to disk at an absolute position.
 */
RC writeBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
    if (fHandle == NULL) return RC_FILE_HANDLE_NOT_INIT;

    if (pageNum < 0 || pageNum > fHandle->totalNumPages) {
        printError(RC_WRITE_FAILED);
        return RC_WRITE_FAILED;
    }

    FILE *fPtr = fopen(fHandle->fileName, "r+");
    if (fPtr == NULL) return RC_FILE_NOT_FOUND;

    fseek(fPtr, pageNum * PAGE_SIZE, SEEK_SET);
    fwrite(memPage, sizeof(char), PAGE_SIZE, fPtr);

    fHandle->mgmtInfo = fPtr;
    fHandle->curPagePos = pageNum;

    fclose(fPtr);
    return RC_OK;
}

/**
 * Write a page to disk at the current position.
 */
RC writeCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    return writeBlock(getBlockPos(fHandle), fHandle, memPage);
}

/**
 * Increase the number of pages in the file by one. The new last page is filled with zero bytes.
 */
RC appendEmptyBlock(SM_FileHandle *fHandle) {
    if (fHandle == NULL) return RC_FILE_HANDLE_NOT_INIT;

    FILE *fPtr = fopen(fHandle->fileName, "r+");
    if (fPtr == NULL) return RC_FILE_NOT_FOUND;

    fseek(fPtr, 0, SEEK_END);

    for (int i = 0; i < PAGE_SIZE; i++) {
        fputc('\0', fPtr);  // Fill the new page with zero bytes
    }

    fHandle->totalNumPages++;
    fHandle->curPagePos = fHandle->totalNumPages - 1;

    fclose(fPtr);
    return RC_OK;
}

/**
 * Ensure the file has at least numberOfPages pages.
 */
RC ensureCapacity(int numberOfPages, SM_FileHandle *fHandle) {
    if (fHandle == NULL) return RC_FILE_HANDLE_NOT_INIT;

    if (fHandle->totalNumPages < numberOfPages) {
        int additionalPages = numberOfPages - fHandle->totalNumPages;
        for (int i = 0; i < additionalPages; i++) {
            appendEmptyBlock(fHandle);
        }
    }

    return RC_OK;
}

/* Writing blocks to a page file - End */
