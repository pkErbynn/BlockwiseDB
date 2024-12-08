#include "storage_mgr.h"
#include "dberror.h"
#include <stdio.h>
#include <stdlib.h>


/************************************************************
*               Manipulating page files     
************************************************************/

// Initialize storage manager
void initStorageManager(void)
{
    printf("Start execution with storage manager initialization");
}

// Create a new page file with an initial size of one page, filled with '\0' bytes as default value
RC createPageFile(char *fileName)
{
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


/**
 * Opens an existing page file.
 * 
 * - If the file does not exist, it should return RC_FILE_NOT_FOUND.
 * - The second parameter is a file handle that references the existing file.
 * - On successful file opening, the fields of the file handle should be initialized with 
 *   information about the opened file (e.g., total number of pages stored in the file).
 * - This requires reading file metadata, such as the total number of pages, from disk.
 */
RC openPageFile(char *fileName, SM_FileHandle *fileHandle)
{
    // input validation/guard clause
    if (fileName == NULL)
        return RC_FILE_NOT_FOUND;

    if (fileHandle == NULL)
        return RC_FILE_HANDLE_NOT_INIT;

    FILE *filePointer;

    filePointer = fopen(fileName, "r");     // open file in read mode

    if (filePointer == NULL)    // early return with error message if file not found
    {
        printError(RC_FILE_NOT_FOUND);
        return RC_FILE_NOT_FOUND;
    }
        
    fileHandle->fileName = fileName;     // initialize file handle with file information

    // Get current file position
    long position = ftell(filePointer);
    fileHandle->curPagePos = position;
    
    // Calculate total number of pages in the file
    fseek(filePointer, 0, SEEK_END);    // move pointer to the end of the file as the current position
    long fileSize = ftell(filePointer);    // get current filePointer position with is represents the current file size 
    long totalPagesInTheFile = fileSize / PAGE_SIZE;
    fileHandle->totalNumPages = totalPagesInTheFile;

    // Set file pointer in management info
    fileHandle->mgmtInfo = filePointer;

    return RC_OK;
}

// Close an open page file
RC closePageFile(SM_FileHandle *fileHandle)
{
    // early return if fileHandle object is invalid, before proceeding
    if (fileHandle == NULL)
    {
        printError(RC_FILE_HANDLE_NOT_INIT);
        return RC_FILE_HANDLE_NOT_INIT;
    }

    // Check if the file is already closed or not open
    if (fileHandle->mgmtInfo == NULL)
    {
        printError(RC_FILE_NOT_FOUND);
        return RC_FILE_NOT_FOUND;
    }

    // Close the file and release resources
    fclose(fileHandle->mgmtInfo);

    // Reset the filename to indicate the file is no longer associated with the handle
    fileHandle->fileName = NULL;

    return RC_OK;
}

// Dispose/delete a page file
RC destroyPageFile(char *fileName)
{
    // Guard clause
    if (fileName == NULL)
    {
        return RC_FILE_NOT_FOUND;
    }

    // Attempt to delete the file using the standard remove() function
    if (remove(fileName) == 0)
    {
        return RC_OK;
    }

    // If remove() fails, log the error and return RC_FILE_NOT_FOUND
    perror("[ERROR] File not found\n");
    printError(RC_FILE_NOT_FOUND);

    return RC_FILE_NOT_FOUND;
}


/*****************************************************************************
*                       Reading blocks from disc     
******************************************************************************/

/**
 * Reads the block at the specified page number (pageNumber) from the file associated with the given file handle (fileHandle) 
 * and stores its content in the memory pointed to by memoryPage.
 * 
 * @param pageNumber The page number to read from the file.
 * @param fileHandle A handle to the file from which the block is to be read.
 * @param memoryPage A pointer to the memory location where the block's content will be stored.
 * 
 * @return RC_OK on successful reading of the block.
 *         RC_FILE_HANDLE_NOT_INIT if the file handle is not initialized or if the page number is invalid.
 *         RC_WRITE_FAILED if the memoryPage is NULL.
 *         RC_FILE_NOT_FOUND if the file is not open or not found.
 *         RC_READ_NON_EXISTING_PAGE if the page number exceeds the total number of pages or if reading fails.
 * 
 * The function moves the file pointer to the correct page position, reads the block of data into the memory page,
 * and updates the current page position in the file handle. It closes the file after reading.
 */


RC readBlock(int pageNumber, SM_FileHandle *fileHandle, SM_PageHandle memoryPage)
{
    // Check if the file handle is properly initialized
    if(fileHandle == NULL)
        return RC_FILE_HANDLE_NOT_INIT;

    // Check if the page number is valid (not negative or greater than the total number of pages in the file)
    if( (pageNumber < 0) || (pageNumber > fileHandle->totalNumPages) )
        return RC_READ_NON_EXISTING_PAGE;

    // Check if the memory page is a valid pointer
    if(memoryPage == NULL)
        return RC_WRITE_FAILED;

    // Check if the file is open and file pointer is available
    if(fileHandle->mgmtInfo == NULL)
        return RC_FILE_NOT_FOUND;
    
    // Calculate the offset based on page number and page size
    int offset = pageNumber * PAGE_SIZE;

    if(fseek(fileHandle->mgmtInfo, offset, SEEK_SET) != 0)
        return RC_READ_NON_EXISTING_PAGE;
    
    if(fread(memoryPage, sizeof(char), PAGE_SIZE, fileHandle->mgmtInfo) != PAGE_SIZE)
        return RC_READ_NON_EXISTING_PAGE;
    
    // Update the current page position in the file handle
    fileHandle->curPagePos = ftell(fileHandle->mgmtInfo) / PAGE_SIZE;

    printf("[DEBUG_INFO] Current block position : %d\n", (int)ftell(fileHandle->mgmtInfo) / PAGE_SIZE);
    printf("[DEBUG_INFO] Current block position2 : %d\n", pageNumber);

    fclose(fileHandle->mgmtInfo);

    return RC_OK;
}

// Get the current page or block position in a file.
int getBlockPos(SM_FileHandle *fileHandle)
{
    if (fileHandle == NULL)
        return RC_FILE_HANDLE_NOT_INIT;
    
    return fileHandle->curPagePos;
}

// Read the first page or block in the file
RC readFirstBlock(SM_FileHandle *fileHandle, SM_PageHandle memoryPage)
{
    return readBlock(0, fileHandle, memoryPage);
}


/*****************************************************************************
*                       Reading blocks from disc 
******************************************************************************/

/* Read the previous block in a page file */
RC readPreviousBlock(SM_FileHandle *fileHandle, SM_PageHandle memPage)
{
    int currentPagePosition = getBlockPos(fileHandle) - 1;
    return readBlock(currentPagePosition, fileHandle, memPage);
}

/* Read the current block in a page file */
RC readCurrentBlock(SM_FileHandle *fileHandle, SM_PageHandle memPage)
{
    int currentPagePosition = getBlockPos(fileHandle);
    return readBlock(currentPagePosition, fileHandle, memPage);
}

/* Read the next block in a page file */
RC readNextBlock(SM_FileHandle *fileHandle, SM_PageHandle memPage)
{
    int currentPagePosition = getBlockPos(fileHandle) + 1;
    return readBlock(currentPagePosition, fileHandle, memPage);
}

/* Read the last block in a page file */
RC readLastBlock(SM_FileHandle *filehandle, SM_PageHandle memPage)
{
    int currentPagePosition = filehandle->totalNumPages - 1;
    return readBlock(currentPagePosition, filehandle, memPage);
}


/*****************************************************************************
*                   Writing blocks to a page file 
******************************************************************************/

RC writeBlock(int pageNumber, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    if(fHandle == NULL)
    {
        return RC_FILE_HANDLE_NOT_INIT;
    }

    if (memPage == NULL) 
    {
        return RC_WRITE_FAILED;
    }

    FILE *filePointer;
    filePointer = fopen(fHandle->fileName, "r+");  
    
    if (filePointer == NULL)
    {
        return RC_FILE_NOT_FOUND;
    }
    
    // check valid page number to make sure it is not out of bound
    if ( (pageNumber < 0) || (pageNumber > fHandle->totalNumPages) ) 
    {
        return RC_READ_NON_EXISTING_PAGE;
    }

    long absoluteOffsetPosition = pageNumber * PAGE_SIZE;   // calculating the absolute position
    fseek(filePointer, absoluteOffsetPosition, SEEK_SET);       // pointer is moved to the beginning of specified pages
    fwrite(memPage, PAGE_SIZE, 1, filePointer);     // writes block of data of size memPage to the file specified

    fHandle->mgmtInfo = filePointer;

    int curPagePos = getBlockPos(fHandle);
    
    fseek(filePointer, curPagePos * PAGE_SIZE, SEEK_SET);   // pointer is moved to the beginning of the specified page
    fHandle->curPagePos = ftell(filePointer) / PAGE_SIZE;   // calculates the currentPage position
    
    return RC_OK;
}

// write a page to disk at the current position.
RC writeCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{   
    if (fHandle == NULL) 
    {
        return RC_FILE_HANDLE_NOT_INIT;
    }

    if (memPage == NULL) 
    {
        return RC_WRITE_FAILED;
    }

    int currentPageNumber = getBlockPos(fHandle);
    
    return writeBlock(currentPageNumber, fHandle, memPage); // passing the current page position to write block
}

// Appends an empty block (filled with zero bytes) to the end of the file, effectively increasing the number of pages in the file by one.
RC appendEmptyBlock(SM_FileHandle *fHandle)
{
    if (fHandle == NULL)
    {
        return RC_FILE_HANDLE_NOT_INIT;
    }

    FILE *filePointer;
    filePointer = fopen(fHandle->fileName, "r+"); // opens the file in read write mode
    
    if (filePointer != NULL)
    {
        fseek(filePointer, 0, SEEK_END);   // moves the cursor to end of the page
        
        // Write zero bytes to the file to create an empty page
        for (int i = 0; i < PAGE_SIZE; i++)
        {
            fputc('\0', filePointer);
        }

        fHandle->mgmtInfo = filePointer;
        fHandle->totalNumPages = ftell(filePointer) / PAGE_SIZE;    // increase number of pages by 1
        fHandle->curPagePos = fHandle->totalNumPages;
        
        return RC_OK;
    }
    
    printError(RC_FILE_NOT_FOUND);
    return RC_FILE_NOT_FOUND; // returns error code when file is not found
}

// Expand the size to numberOfPages if the file has less than numberOfPages pages.
RC ensureCapacity(int numberOfPages, SM_FileHandle *fHandle)
{
    if(fHandle == NULL)
    {
        return RC_FILE_HANDLE_NOT_INIT;
    }
    
    if (numberOfPages < 0)  // Check for a valid number of pages (cannot be negative)
    {
        return RC_READ_NON_EXISTING_PAGE;
    }

    // If the file has fewer pages than required, expand its capacity
    if (fHandle->totalNumPages < numberOfPages)
    {
        int additionalPages = numberOfPages - fHandle->totalNumPages; // Calculate the number of additional pages needed
        for (int i = 0; i < additionalPages; i++)
        {
            appendEmptyBlock(fHandle); // Append the necessary number of empty pages
        }
    }

    return RC_OK;
}
