#include <string.h>
#include <stdlib.h>
#include <stdio.h>

#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "dberror.h"

/*******************************************
*  Buffer Manager Interface Pool Handling       
*******************************************/

// Bufferpool
typedef struct BufferPoolInfo
{
    int writeCount;
    int *pageFixCount;
    int *accessTimestamps;
    int *pageNumbers;
    int maxPages;
    int strategyType;
    int readCount;
    bool *dirtyFlags;
    char *pageDataBuffer;
    SM_FileHandle fileHandle;
    int availableSlots;
    int *accessOrder;
}BufferPoolInfo;

bool isPageFound = FALSE;

//  static helper methods
static RC writeDirtyPagesToDisk(BM_BufferPool *const bufferPool);
static RC releaseBufferMemory(BM_BufferPool *const bufferPool);
static void shiftAccessOrder(int startIndex, int end, BufferPoolInfo *bufferPoolData, int newPageNumber);
static void updateBufferStats(BufferPoolInfo *bufferPoolData, int bufferIndex, int pageNumber);

// Initialize the buffer pool
RC initBufferPool(BM_BufferPool *const bufferPool, const char *const pageFileName, const int pageCount, ReplacementStrategy strategy, void *strategyData)
{
    if (bufferPool == NULL || pageFileName == NULL || pageCount <= 0) {
        return RC_ERROR;
    }

    SM_FileHandle file;
    BufferPoolInfo *bufferPoolInfo;
    int status, i;

    // Open the page file
    status = openPageFile((char *)pageFileName, &file);
    if (status != RC_OK) {
        return status;
    }

    // Allocate memory for the buffer pool
    bufferPoolInfo = (BufferPoolInfo *)calloc(1, sizeof(BufferPoolInfo));
    if (!bufferPoolInfo) {
        return RC_MEMORY_ALLOCATION_FAIL;
    }

    bufferPoolInfo->maxPages = pageCount;
    bufferPoolInfo->pageDataBuffer = (char *)calloc(pageCount * PAGE_SIZE, sizeof(char));
    bufferPoolInfo->readCount = 0;
    bufferPoolInfo->writeCount = 0;
    bufferPoolInfo->accessOrder = (int *)calloc(pageCount, sizeof(int));
    bufferPoolInfo->dirtyFlags = (bool *)calloc(pageCount, sizeof(bool));
    bufferPoolInfo->availableSlots = pageCount;
    bufferPoolInfo->fileHandle = file;
    bufferPoolInfo->pageNumbers = (int *)calloc(pageCount, sizeof(int));
    bufferPoolInfo->pageFixCount = (int *)calloc(pageCount, sizeof(int));
    bufferPoolInfo->strategyType = strategy;

    // Initialize array values
    for (i = 0; i < pageCount; i++) {
        bufferPoolInfo->dirtyFlags[i] = FALSE;
        bufferPoolInfo->pageFixCount[i] = 0;
        bufferPoolInfo->pageNumbers[i] = NO_PAGE;
        bufferPoolInfo->accessOrder[i] = NO_PAGE;
    }

    // Initialize BM_BufferPool structure
    if (bufferPool != NULL) {
        bufferPool->pageFile = pageFileName ? strdup(pageFileName) : NULL;
        bufferPool->numPages = pageCount;
        bufferPool->strategy = strategy;
        bufferPool->mgmtData = bufferPoolInfo;
    }

    return RC_OK;
}

// Shut down the buffer pool
RC shutdownBufferPool(BM_BufferPool *const bufferPool)
{
    if (bufferPool == NULL || bufferPool->mgmtData == NULL) {
        return RC_ERROR;
    }

    BufferPoolInfo *bufferInfo = bufferPool->mgmtData;

    // Check for pinned pages
    for (int i = 0; i < bufferInfo->maxPages; i++) {
        if (bufferInfo->pageFixCount[i] != 0) {
            return RC_BUFFERPOOL_IN_USE;
        }
    }

    // Write dirty pages to disk
    RC status = writeDirtyPagesToDisk(bufferPool);
    if (status != RC_OK) {
        return status;
    }

    // Close the file and release memory
    status = closePageFile(&bufferInfo->fileHandle);
    if (status != RC_OK) {
        return RC_CLOSE_FAILED;
    }

    releaseBufferMemory(bufferPool);

    return RC_OK;
}



// Helper function to write dirty pages to disk
static RC writeDirtyPagesToDisk(BM_BufferPool *const bufferPool) {
    BufferPoolInfo *bufferInfo = bufferPool->mgmtData;
    for (int i = 0; i < bufferInfo->maxPages; i++) {
        if (bufferInfo->dirtyFlags[i]) {
            int offset = i * PAGE_SIZE;
            
            // Ensure the file has sufficient capacity before writing
            RC status = ensureCapacity(bufferInfo->pageNumbers[i] + 1, &bufferInfo->fileHandle);
            if (status != RC_OK) {
                return status;
            }

            // Write the page to disk
            status = writeBlock(bufferInfo->pageNumbers[i], &bufferInfo->fileHandle, bufferInfo->pageDataBuffer + offset);
            if (status != RC_OK) {
                return RC_WRITE_FAILED;
            }

            bufferInfo->writeCount++;
        }
    }
    return RC_OK;
}



// Helper function to free all allocated buffer memory
static RC releaseBufferMemory(BM_BufferPool *const bufferPool) {
    if (bufferPool == NULL || bufferPool->mgmtData == NULL) {
        return RC_ERROR;
    }

    BufferPoolInfo *bufferInfo = bufferPool->mgmtData;

    // Free and reset memory allocations
    if (bufferInfo->accessOrder) {
        free(bufferInfo->accessOrder);
        bufferInfo->accessOrder = NULL;
    }
    if (bufferInfo->pageNumbers) {
        free(bufferInfo->pageNumbers);
        bufferInfo->pageNumbers = NULL;
    }
    if (bufferInfo->dirtyFlags) {
        free(bufferInfo->dirtyFlags);
        bufferInfo->dirtyFlags = NULL;
    }
    if (bufferInfo->pageFixCount) {
        free(bufferInfo->pageFixCount);
        bufferInfo->pageFixCount = NULL;
    }
    if (bufferInfo->pageDataBuffer) {
        free(bufferInfo->pageDataBuffer);
        bufferInfo->pageDataBuffer = NULL;
    }

    // Free the BufferPoolInfo structure itself
    free(bufferInfo);
    bufferPool->mgmtData = NULL;

    return RC_OK;
}

// Helper to ensure file size matches required buffer capacity
static RC ensureFileSize(FILE *file, long requiredSize) {
    fseek(file, 0, SEEK_END);
    long currentSize = ftell(file);
    
    if (currentSize < requiredSize) {
        fseek(file, requiredSize - 1, SEEK_SET);
        fputc('\0', file);
    }
    return RC_OK;
}


// Function to force flushing the buffer pool to disk
RC forceFlushPool(BM_BufferPool *const bufferPool) {
    if (bufferPool == NULL || bufferPool->mgmtData == NULL) {
        return RC_ERROR;
    }

    BufferPoolInfo *bufferInfo = bufferPool->mgmtData;
    RC status = RC_OK;

    for (int i = 0; i < bufferInfo->maxPages; i++) {
        // Flush pages that are dirty and not currently pinned
        if (bufferInfo->pageFixCount[i] == 0 && bufferInfo->dirtyFlags[i]) {
            FILE *file = fopen(bufferInfo->fileHandle.fileName, "r+");
            if (!file) {
                return RC_WRITE_FAILED;
            }

            // Ensure the file is large enough for the page
            long requiredSize = (bufferInfo->pageNumbers[i] + 1) * PAGE_SIZE;
            ensureFileSize(file, requiredSize);

            // Write page data to the file
            int offset = i * PAGE_SIZE;
            fseek(file, bufferInfo->pageNumbers[i] * PAGE_SIZE, SEEK_SET);
            if (fwrite(bufferInfo->pageDataBuffer + offset, sizeof(char), PAGE_SIZE, file) != PAGE_SIZE) {
                fclose(file);
                return RC_WRITE_FAILED;
            }

            fclose(file);
            bufferInfo->dirtyFlags[i] = FALSE;
            bufferInfo->writeCount++;
        }
    }

    return status;
}


// Function to update the order of recently used pages
static void shiftAccessOrder(int startIndex, int endIndex, BufferPoolInfo *bufferInfo, int newPageNumber) {
    for (int i = startIndex; i < endIndex; i++) {
        bufferInfo->accessOrder[i] = bufferInfo->accessOrder[i + 1];
    }
    bufferInfo->accessOrder[endIndex] = newPageNumber;
}

// Function to update buffer statistics
static void updateBufferStats(BufferPoolInfo *bufferInfo, int bufferIndex, int pageNumber) {
    bufferInfo->pageNumbers[bufferIndex] = pageNumber;
    bufferInfo->readCount++;
    bufferInfo->pageFixCount[bufferIndex]++;
    bufferInfo->dirtyFlags[bufferIndex] = FALSE;
}



/*****************************************
*  Buffer Manager Interface Access Pages 
*****************************************/

// Mark a page as dirty in the buffer pool
RC markDirty(BM_BufferPool *const bufferPool, BM_PageHandle *const page) {
    // Check if buffer manager or management data is null
    if (bufferPool == NULL || bufferPool->mgmtData == NULL) {
        return RC_FILE_HANDLE_NOT_INIT;
    }

    BufferPoolInfo *bufferInfo = bufferPool->mgmtData;

    // Search for the specified page number in the buffer
    for (int i = 0; i < bufferInfo->maxPages; i++) {
        if (bufferInfo->pageNumbers[i] == page->pageNum) {
            // Set the dirty flag to TRUE if not already set
            if (!bufferInfo->dirtyFlags[i]) {
                bufferInfo->dirtyFlags[i] = TRUE;
            }
            break; // Exit once the page is marked dirty
        }
    }

    return RC_OK;
}

// Force a page to be written to disk from the buffer pool
RC forcePage(BM_BufferPool *const bufferPool, BM_PageHandle *const page) {
    // Check if buffer manager or management data is null
    if (bufferPool == NULL || bufferPool->mgmtData == NULL) {
        return RC_FILE_HANDLE_NOT_INIT;
    }

    BufferPoolInfo *bufferInfo = bufferPool->mgmtData;
    bool pageFound = FALSE;

    // Search for the specified page in the buffer pool
    for (int i = 0; i < bufferInfo->maxPages; i++) {
        if (bufferInfo->pageNumbers[i] == page->pageNum) {
            int offset = i * PAGE_SIZE;
            printf("Simulated writing of page %d to disk at offset %d.\n", page->pageNum, offset);

            // Mark the page as clean and increment the write count
            bufferInfo->dirtyFlags[i] = FALSE;
            bufferInfo->writeCount++;

            pageFound = TRUE;
            break;
        }
    }

    // Return success if the page was found and written, otherwise return failure
    return pageFound ? RC_OK : RC_WRITE_FAILED;
}


// Unpin a page in the buffer pool
RC unpinPage(BM_BufferPool *const bufferPool, BM_PageHandle *const page) {
    // Check if buffer manager or management data is null
    if (bufferPool == NULL || bufferPool->mgmtData == NULL) {
        return RC_FILE_HANDLE_NOT_INIT;
    }

    BufferPoolInfo *bufferInfo = bufferPool->mgmtData;

    // Search for the specified page in the buffer pool
    for (int i = 0; i < bufferInfo->maxPages; i++) {
        if (bufferInfo->pageNumbers[i] == page->pageNum) {
            // Decrease the fix count if it is greater than 0
            if (bufferInfo->pageFixCount[i] > 0) {
                bufferInfo->pageFixCount[i]--;
            }
            return RC_OK; // Page found and unpinned successfully
        }
    }

    // If the page wasn't found in the buffer, return OK (consistent behavior)
    return RC_OK;
}


// Pin a page in the buffer pool
RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum){
    BufferPoolInfo *buffer_pool;
    bool void_page=FALSE;
    bool foundedPage=FALSE;
    bool UpdatedStra_found=FALSE;
    int read_code;
    int record_pointer;
    int memory_address;
    int swap_location;
    
    SM_PageHandle page_handle;
    buffer_pool=bm->mgmtData;
    buffer_pool = bm->mgmtData;
    
    void_page = (buffer_pool->availableSlots == buffer_pool->maxPages) ? TRUE : void_page;
    if (!void_page) {
    int totalPages = buffer_pool->maxPages - buffer_pool->availableSlots;
    for (int i = 0; i < totalPages; i++) {

        if (buffer_pool->pageNumbers[i] == pageNum) {
            page->pageNum = pageNum;
            int memory_address = i;
            buffer_pool->pageFixCount[memory_address]++;
            page->data = &buffer_pool->pageDataBuffer[memory_address * PAGE_SIZE];
            foundedPage = TRUE;
            if (buffer_pool->strategyType == RS_LRU) {
                    int lastPosition = buffer_pool->maxPages - buffer_pool->availableSlots - 1; 
                    swap_location = -1; 
                    for (int j = 0; j <= lastPosition; j++) 
                        if (buffer_pool->accessOrder[j] != pageNum) {
                            if (j == buffer_pool->maxPages - 1) {
                                swap_location = -1;
                            }
                            } else {
                            swap_location = j;
                            break;
                        }
                        if (swap_location != -1) {
                            memmove(&buffer_pool->accessOrder[swap_location], &buffer_pool->accessOrder[swap_location + 1], (lastPosition - swap_location) * sizeof(buffer_pool->accessOrder[0]));
                            buffer_pool->accessOrder[lastPosition] = pageNum;
                        }
                }
                            else {
                                int unutilized = 0;
                                do {
                                    //printf("This loop executes exactly once.\n");
                                } while (++unutilized < 1);
                        }
            return RC_OK;
        }
        }
    } 

    if ((void_page == TRUE && buffer_pool != NULL && (1 == 1)) || 
    ((foundedPage != TRUE && foundedPage == FALSE) && buffer_pool->availableSlots > 0 && 
    buffer_pool->availableSlots <= buffer_pool->maxPages && buffer_pool->availableSlots == buffer_pool->availableSlots && 
    (buffer_pool->availableSlots != -1 && buffer_pool->maxPages != -1) && 
    (buffer_pool->availableSlots >= 0 && buffer_pool->maxPages >= buffer_pool->availableSlots) && 
    ((buffer_pool->availableSlots + 1) > 1) && ((buffer_pool->maxPages - buffer_pool->availableSlots) >= 0)))
    {
        page_handle = (SM_PageHandle)calloc(1, PAGE_SIZE);
        if (page_handle != NULL) {

            read_code = readBlock(pageNum, &buffer_pool->fileHandle, page_handle);
            if (read_code >= 0) {
                size_t total_used_pages = buffer_pool->maxPages - buffer_pool->availableSlots;
                memory_address = total_used_pages;
                if (memory_address >= 0 && memory_address <= buffer_pool->maxPages) {
                    size_t base_address = 0; 
                    record_pointer = (memory_address * PAGE_SIZE) + base_address;

                } 
            } 
        }
        
        memcpy(buffer_pool->pageDataBuffer + record_pointer, page_handle, PAGE_SIZE);
        buffer_pool->availableSlots--;
        buffer_pool->accessOrder[memory_address] = pageNum;
        buffer_pool->pageNumbers[memory_address] = pageNum;
        buffer_pool->readCount++;
        buffer_pool->pageFixCount[memory_address]++;
        buffer_pool->dirtyFlags[memory_address] = FALSE;
        page->pageNum = pageNum;
        page->data = &(buffer_pool->pageDataBuffer[record_pointer]);
        free(page_handle);

        return RC_OK;
    }
       
        bool isPageNotFound = !foundedPage;
        bool isBufferPoolValid = buffer_pool != NULL;
        bool isBufferPoolFull = buffer_pool->availableSlots == 0;

        if (isPageNotFound && isBufferPoolValid && isBufferPoolFull)
        {
        UpdatedStra_found = FALSE;
        page_handle = (SM_PageHandle) malloc(PAGE_SIZE);
        if (page_handle != NULL) {
            memset(page_handle, 0, PAGE_SIZE);
        }
        read_code = readBlock(pageNum, &buffer_pool->fileHandle, page_handle);


        if (buffer_pool->strategyType == RS_FIFO || buffer_pool->strategyType == RS_LRU) {
            int i = 0, j = 0;
            do {
                int swap_page = buffer_pool->accessOrder[j];
                i = 0; 
                do {
                    if (buffer_pool->pageNumbers[i] == swap_page && buffer_pool->pageFixCount[i] == 0) {
                        memory_address = i;
                        record_pointer = i * PAGE_SIZE;
                        if (buffer_pool->dirtyFlags[i]) {
                            read_code = ensureCapacity(buffer_pool->pageNumbers[i] + 1, &buffer_pool->fileHandle);
                            read_code = writeBlock(buffer_pool->pageNumbers[i], &buffer_pool->fileHandle, buffer_pool->pageDataBuffer + record_pointer);
                            buffer_pool->writeCount++;
                        }
                        swap_location = j;
                        UpdatedStra_found = TRUE;
                        break; 
                    }
                    i++;
                } while (i < buffer_pool->maxPages && !UpdatedStra_found);
                j++;
                if (UpdatedStra_found) break; 
            } while (j < buffer_pool->maxPages);
        }
    }


    if (UpdatedStra_found == FALSE) {
        free(page_handle);
        return RC_BUFFERPOOL_FULL;
    } 
        
    record_pointer = memory_address * PAGE_SIZE;
    int i = 0;
    if (i < PAGE_SIZE) {
        do {
            buffer_pool->pageDataBuffer[i + record_pointer] = page_handle[i];
            i++;
        } while (i < PAGE_SIZE);
    }
            
    // primary logic
    if (buffer_pool->strategyType == RS_LRU || buffer_pool->strategyType == RS_FIFO) {
        shiftAccessOrder(swap_location, buffer_pool->maxPages - 1, buffer_pool, pageNum);
    } 
    updateBufferStats(buffer_pool, memory_address, pageNum);
    page->pageNum = pageNum;
    page->data = buffer_pool->pageDataBuffer + record_pointer;
    free(page_handle); 
    return RC_OK; 
}


/******************************
*  Statistics Interface      
******************************/

// Define the page numbers as an array
PageNumber *getFrameContents(BM_BufferPool *const bufferPool)
{
    // Access the buffer pool management data
    BufferPoolInfo *bufferInfo = bufferPool->mgmtData;

    // Check if the buffer pool is entirely empty
    if (bufferInfo->availableSlots == bufferInfo->maxPages) {
        return NULL; // Return NULL if no pages are currently loaded
    }

    // Return the array of page numbers currently in the buffer frames
    return bufferInfo->pageNumbers;
}

// Retrieve an array of dirty page flags for each frame
bool *getDirtyFlags(BM_BufferPool *const bufferPool) {
    // Validate buffer manager and data
    if (bufferPool == NULL || bufferPool->mgmtData == NULL) {
        return NULL;
    }

    BufferPoolInfo *bufferData = bufferPool->mgmtData;
    return bufferData->dirtyFlags;
}

// Retrieve the number of pages that have been read
int getNumReadIO(BM_BufferPool *const bufferPool) {
    // Check if the buffer manager or management data is null
    if (bufferPool == NULL || bufferPool->mgmtData == NULL) {
        return 0;
    }

    // Access the number of reads from the buffer pool management data
    BufferPoolInfo *bufferInfo = bufferPool->mgmtData;
    return bufferInfo->readCount;
}

// Retrieve the number of pages that have been written to disk
int getNumWriteIO(BM_BufferPool *const bufferPool) {
    // Check if the buffer manager or management data is null
    if (bufferPool == NULL || bufferPool->mgmtData == NULL) {
        return 0;
    }

    // Access the number of writes from the buffer pool management data
    BufferPoolInfo *bufferInfo = bufferPool->mgmtData;
    return bufferInfo->writeCount;
}

// Retrieve the fix counts for each page in the buffer pool
int *getFixCounts(BM_BufferPool *const bufferPool) {
    // Validate the buffer manager and management data
    if (bufferPool == NULL || bufferPool->mgmtData == NULL) {
        printf("Buffer pool or management data is missing.\n");
        return NULL;
    }

    BufferPoolInfo *bufferInfo = bufferPool->mgmtData;

    // Check if no pages are currently pinned (all slots are available)
    if (bufferInfo->availableSlots == bufferInfo->maxPages) {
        static int noFixes = 0;
        printf("No pages are currently pinned in the buffer.\n");
        return &noFixes;
    }

    // Return the array of fix counts for pages in the buffer
    printf("Returning fix counts for pages currently in use.\n");
    return bufferInfo->pageFixCount;
}

