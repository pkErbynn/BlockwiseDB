#include "storage_mgr.h"
#include "dberror.h"
#include <stdio.h>
#include <stdlib.h>

int curPagePos;

/* manipulating page files - Begin */

/*
Initialize storage manager
*/
void initStorageManager(void)
{
    printf("Begin Execution");
}

/**
 * Create a new page file with an initial size of one page, filled with '\0' bytes.
 **/
RC createPageFile(char *fileName)
{
    FILE *fPtr; // pointer for the file

    fPtr = fopen(fileName, "r"); // Open the file in read mode

    if (fPtr == NULL) // Condition to check if file exists
    {
        fPtr = fopen(fileName, "w");        // Creates a new file to write
        for (int i = 0; i < PAGE_SIZE; i++) // Iterates through the size of the page
            fputc('\0', fPtr);              // Writes single \0 at a time. Fills the entire single page in a file with \0
    }
    else // If the file already exists
    {
        for (int i = 0; i < PAGE_SIZE; i++) // Iterates through the size of the page
            fputc('\0', fPtr);              // Writes single \0 at a time. Overwrites the entire single page with \0
    }

    fclose(fPtr); // closes the file

    return RC_OK;
}

/**
 * Opens an existing page file.Should return RC FILE NOT FOUND if the file does not exist.
 * – The second parameter is an existing file handle.
 * – If opening the file is successful, then the fields of this file handle should be initialized with
 * the information about the opened file.
 * For instance, you would have to read the total number of pages that are stored in the file from disk.
 * */

RC openPageFile(char *fileName, SM_FileHandle *fHandle)
{
    // input guard
    if (fileName == NULL) {
        return RC_FILE_NOT_FOUND;
    }
    if (fHandle == NULL) {
        return RC_FILE_HANDLE_NOT_INIT;
    }
    
    FILE *fptr;

    fptr = fopen(fileName, "r"); // Open the file in read mode

    if (fptr != NULL) // If file exists
    {
        fHandle->fileName = fileName; // assigning filename to the filename in file handle

        long position = ftell(fptr);    // fetches the current position of the pointer
        fHandle->curPagePos = position; // assigning the current position of the pointer to the curPagePos in file handle

        fseek(fptr, 0, SEEK_END);            // moves the pointer to the end of the file
        long size = ftell(fptr);             // fetches the current position of the pointer which gives the total file size
        long totalpages = size / PAGE_SIZE;  // dividing the file size with the page size
        fHandle->totalNumPages = totalpages; // assigning the total number if pages stored in the file from disk to totalNumPages in file handle

        fHandle->mgmtInfo = fptr; // assigning the file pointer information to mgmtInfo in file handle

        return RC_OK;
    }
    printError(RC_FILE_NOT_FOUND);
    // perror("[ERROR] File not found\n");
    return RC_FILE_NOT_FOUND; // returns RC_FILE_NOT_FOUND error code of the file does not exists

}

/**
 * Close an open page file
 **/
RC closePageFile(SM_FileHandle *fHandle)
{
    if (fHandle != NULL) // condition to check if file handle is initialized
    {
        if (fHandle->mgmtInfo != NULL) // checks if file handle file pointer is not null. If its not null, the file is open
        {
            //==============
            fclose(fHandle->mgmtInfo); // closes the file
            fHandle->fileName = NULL;
            return RC_OK;
            //============
        }
        else
        {
            printError(RC_FILE_NOT_FOUND);
            // perror("[ERROR] File not found\n");
            return RC_FILE_NOT_FOUND; // If file handle file pointer is null, file is not open or already closed, then retuns error code
        }
    }

    printError(RC_FILE_HANDLE_NOT_INIT);
    // perror("[ERROR] File handle is not initialized\n");
    return RC_FILE_HANDLE_NOT_INIT; // returns error code if file handle is not initialized
}

/**
 *  Destroy (delete) a page file
 **/
RC destroyPageFile(char *fileName)
{
    if (remove(fileName) == 0) // Deletes the file
    {
        return RC_OK; // returns 0 when file is successfully deleted
    }
    printError(RC_FILE_NOT_FOUND);
    // perror("[ERROR] File not found\n");
    return RC_FILE_NOT_FOUND; // returns error code otherwise
}

/* manipulating page files - End */

/* reading blocks from disc - Begin */

/**
 * Method to reads the block at position pageNum from a file and stores its content in the memory pointed
 * to by the memPage page handle.
 * If the file has less than pageNum pages, the method should return RC READ NON EXISTING PAGE.
 **/
RC readBlock(int pageNum, SM_FileHandle *filehandle, SM_PageHandle memPage)
{
    if (filehandle == NULL)
    {
        printError(RC_FILE_HANDLE_NOT_INIT);
        // perror("[ERROR] File handle is not initialized\n");
        return RC_FILE_HANDLE_NOT_INIT;
    }

    if (pageNum > filehandle->totalNumPages || pageNum < 0)
    {
        printError(RC_READ_NON_EXISTING_PAGE);
        // perror("[ERROR] Invalid request to read non existing page\n");
        return RC_READ_NON_EXISTING_PAGE;
    }

    fseek(filehandle->mgmtInfo, (((pageNum)*PAGE_SIZE)), SEEK_SET);

    if (fread(memPage, 1, PAGE_SIZE, filehandle->mgmtInfo) != PAGE_SIZE)
    {
        printError(RC_READ_NON_EXISTING_PAGE);
        // perror("[ERROR] Invalid request to read non existing page\n");
        return RC_READ_NON_EXISTING_PAGE;
    }

    filehandle->curPagePos = (ftell(filehandle->mgmtInfo) / PAGE_SIZE);
    printf("[INFO] Current page pos : %d\n", filehandle->curPagePos);
    fclose(filehandle->mgmtInfo);
    return RC_OK;
}

/**
 * Method to retrieve the current page position in a file.
 **/
int getBlockPos(SM_FileHandle *filehandle)
{
    if (filehandle == NULL)
    {
        printError(RC_FILE_HANDLE_NOT_INIT);
        // printError("[ERROR] File handle is not initialized\n");
        return RC_FILE_HANDLE_NOT_INIT;
    }

    if ((fopen(filehandle->fileName, "r")) == NULL)
    {
        printError(RC_FILE_HANDLE_NOT_INIT);
        // perror("[ERROR] File handle is not initialized\n");
        return RC_FILE_NOT_FOUND;
    }

    printf("%d ", filehandle->curPagePos);
    return filehandle->curPagePos;
}

/**
 * Method to read the first respective last page in a file.
 **/
RC readFirstBlock(SM_FileHandle *filehandle, SM_PageHandle memPage)
{
    return readBlock(0, filehandle, memPage);
}

/**
 * Method to read the previous page relative to the curPagePos of the file.
 **/
RC readPreviousBlock(SM_FileHandle *filehandle, SM_PageHandle memPage)
{
    curPagePos = getBlockPos(filehandle) - 1;
    return readBlock(curPagePos, filehandle, memPage);
}

/**
 * Method to read the current page relative to the curPagePos of the file.
 **/
RC readCurrentBlock(SM_FileHandle *filehandle, SM_PageHandle memPage)
{
    curPagePos = getBlockPos(filehandle);
    return readBlock(curPagePos, filehandle, memPage);
}

// Method to read the next page relative to the curPagePos of the file.
RC readNextBlock(SM_FileHandle *filehandle, SM_PageHandle memPage)
{
    curPagePos = getBlockPos(filehandle) + 1;
    return readBlock(curPagePos, filehandle, memPage);
}

/**
 * Method to read the last page of the file.
 **/
RC readLastBlock(SM_FileHandle *filehandle, SM_PageHandle memPage)
{
    curPagePos = (filehandle->totalNumPages) - 1;
    return readBlock(curPagePos, filehandle, memPage);
}

/* reading blocks from disc - End */

/* writing blocks to a page file - Begin */

/**
 * Method to write a page to disk at an absolute position
 **/
RC writeBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    if (fHandle != NULL) // checking if file handle is initialized
    {
        FILE *fptr;
        fptr = fopen(fHandle->fileName, "r+");                                    // opening the file in read and write mode
        if (pageNum >= 0 && pageNum <= fHandle->totalNumPages && memPage != NULL) // checking if page number is in range and mempage is initialized
        {
            long absPos = pageNum * PAGE_SIZE;   // calculating the absolute position
            fseek(fptr, absPos, SEEK_SET);       // pointer is moved to the begginning of the specified pages
            fwrite(memPage, PAGE_SIZE, 1, fptr); // writes block of data of size memPage to the file specified

            fHandle->mgmtInfo = fptr;                        // updating fhandle pointer information
            fseek(fptr, curPagePos * PAGE_SIZE, SEEK_SET);   // pointer is moved to the beginning of the specified page
            fHandle->curPagePos = (ftell(fptr) / PAGE_SIZE); // calculates the currentPage position and updates teh fhandle
            return RC_OK;                                    // returns successful response
        }
        else
        {
            printError(RC_WRITE_FAILED);
            // perror("[ERROR] PageNum out of bound\n");
            return RC_WRITE_FAILED; // retuns error code when pagenumber is out of bound
        }
    }
    else
    {
        printError(RC_FILE_HANDLE_NOT_INIT);
        // perror("[ERROR] File handle is not initialized\n");
        return RC_FILE_HANDLE_NOT_INIT; // returns error code if the file is not initialized
    }
}

/**
 * Method to write a page to disk at the current position.
 **/
RC writeCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    return writeBlock(getBlockPos(fHandle), fHandle, memPage); // passing the current page position to write block
}

/**
 * Method to increase the number of pages in the file by one.
 * The new last page should be filled with zero bytes.
 **/
RC appendEmptyBlock(SM_FileHandle *fHandle)
{
    if (fHandle != NULL) // checks if file handle is initialized
    {
        FILE *fPtr;
        fPtr = fopen(fHandle->fileName, "r+"); // opens the file in read write mode
        if (fPtr != NULL)                      // if file is present
        {
            fseek(fPtr, 0, SEEK_END);           // moves the cursor to end of the page
            for (int i = 0; i < PAGE_SIZE; i++) // Iterates through the size of the page to create empty page
            {
                fputc('\0', fPtr); // fills the entire page with zero bytes
            }

            fHandle->mgmtInfo = fPtr;                           // file pointer information is moved to file handle
            fHandle->totalNumPages = (ftell(fPtr) / PAGE_SIZE); // number of pages is increased by 1
            fHandle->curPagePos = fHandle->totalNumPages;       // updating current page position
            return RC_OK;                                       // return successful response
        }
        else
        {
            printError(RC_FILE_NOT_FOUND);
            // perror("[ERROR] File not found\n");
            return RC_FILE_NOT_FOUND; // returns error code when file is not found
        }
    }
    printError(RC_FILE_HANDLE_NOT_INIT);
    // perror("[ERROR] File handle is not initialized\n");
    return RC_FILE_HANDLE_NOT_INIT; // returns error code when file is not initialized
}

/**
 * Method to increase the size to numberOfPages if the file has less than numberOfPages pages.
 **/
RC ensureCapacity(int numberOfPages, SM_FileHandle *fHandle)
{
    if (fHandle != NULL)
    {
        if (fHandle->totalNumPages < numberOfPages) // checks if the number of pages are greater than file total number of pages
        {
            int additionalPages = numberOfPages - fHandle->totalNumPages; // calculate required additional pages
            for (int i = 0; i < additionalPages; i++)                     // iterate through the additional pages and append an empty block
            {
                appendEmptyBlock(fHandle); // appends empty page
            }
        }
        return RC_OK; // returns successful code
    }
    printError(RC_FILE_HANDLE_NOT_INIT);
    // perror("[ERROR] File handle is not initialized\n");
    return RC_FILE_HANDLE_NOT_INIT; // returns error code
}
/* writing blocks to a page file - End */