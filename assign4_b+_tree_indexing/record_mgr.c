#include <string.h>
#include <stdlib.h>
#include <stdio.h>

#include "tables.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "record_mgr.h"
#include "dberror.h"

#define MAX_ATTR_NAME_LEN 15

extern int getAttrPos (Schema *schema, int attrNum);
static void prepareTableHeader(char **tableHeaderPtr, TableManager *tableManager, Schema *schema);
static void populateSchemaDetails(char **tableHeaderPtr, Schema *schema);
static void handleCleanup(BM_BufferPool *bufferPool, BM_PageHandle *pageHandle, TableManager *tableManager); 


/*************************
*  Table and manager 
**************************/

RC initRecordManager(void *mgmtData) {
    printf("Starting Record Manager...\n");
    return RC_OK;
}

RC shutdownRecordManager() {
    printf("Record Manager closed successfully.\n");
    return RC_OK;
}

void handleCleanup(BM_BufferPool *bufferPool, BM_PageHandle *pageHandle, TableManager *tableManager) {
    if (bufferPool == NULL || pageHandle == NULL || tableManager == NULL) return;
    
    if (bufferPool != NULL) {
        shutdownBufferPool(bufferPool);
        free(bufferPool);
    }
    if (pageHandle != NULL) {
        free(pageHandle);
    }
    if (tableManager != NULL) {
        free(tableManager);
    }
}

RC createTable(char *name, Schema *schema) {
    if (name == NULL || schema == NULL) {
        return RC_GENERAL_ERROR;
    }

    // Allocate memory for required structures
    BM_BufferPool *bufferPool = calloc(1, sizeof(BM_BufferPool));
    BM_PageHandle *pageHandle = calloc(1, sizeof(BM_PageHandle));
    TableManager *tableManager = calloc(1, sizeof(TableManager));

    if (bufferPool == NULL || pageHandle == NULL || tableManager == NULL) {
        handleCleanup(bufferPool, pageHandle, tableManager);
        return RC_MEMORY_ALLOCATION_FAIL;
    }

    // Step 1: Create a page file for the table
    RC result = createPageFile(name);
    if (result != RC_OK) {
        handleCleanup(bufferPool, pageHandle, tableManager);
        return result;
    }

    // Step 2: Initialize the buffer pool
    result = initBufferPool(bufferPool, name, 3, RS_FIFO, NULL);
    if (result != RC_OK) {
        handleCleanup(bufferPool, pageHandle, tableManager);
        return result;
    }

    // Step 3: Pin the first page in the buffer pool
    result = pinPage(bufferPool, pageHandle, 0);
    if (result != RC_OK) {
        handleCleanup(bufferPool, pageHandle, tableManager);
        return result;
    }

    // Step 4: Prepare and write the table header
    char *tableHeaderPtr = pageHandle->data;
    prepareTableHeader(&tableHeaderPtr, tableManager, schema);

    // Step 5: Mark the page as dirty
    result = markDirty(bufferPool, pageHandle);
    if (result != RC_OK) {
        handleCleanup(bufferPool, pageHandle, tableManager);
        return result;
    }

    // Step 6: Unpin the page
    result = unpinPage(bufferPool, pageHandle);
    if (result != RC_OK) {
        handleCleanup(bufferPool, pageHandle, tableManager);
        return result;
    }

    // Step 7: Shutdown the buffer pool
    result = shutdownBufferPool(bufferPool);
    if (result != RC_OK) {
        handleCleanup(bufferPool, pageHandle, tableManager);
        return result;
    }

    // Cleanup and return success
    handleCleanup(bufferPool, pageHandle, tableManager);
    return RC_OK;
}

void prepareTableHeader(char **tableHeaderPtr, TableManager *tableManager, Schema *schema) {
    int *headerInt = (int *)*tableHeaderPtr;

    // Initialize table manager values
    tableManager->totalTuples = 0;
    tableManager->recSize = getRecordSize(schema);
    tableManager->firstFreePageNum = 1;
    tableManager->firstFreeSlotNum = 0;
    tableManager->firstDataPageNum = -1;

    // Populate the table header with initial values
    *headerInt++ = tableManager->totalTuples;
    *headerInt++ = tableManager->recSize;
    *headerInt++ = tableManager->firstFreePageNum;
    *headerInt++ = tableManager->firstFreeSlotNum;
    *headerInt++ = tableManager->firstDataPageNum;
    *headerInt++ = schema->numAttr;
    *headerInt++ = schema->keySize;

    // Update the pointer to reflect the new position in the header
    *tableHeaderPtr = (char *)headerInt;

    // Populate schema details in the table header
    populateSchemaDetails(tableHeaderPtr, schema);
}

void populateSchemaDetails(char **tableHeaderPtr, Schema *schema) {
    if (tableHeaderPtr == NULL || *tableHeaderPtr == NULL || schema == NULL) {
        fprintf(stderr, "Error: Null pointer input in populateSchemaDetails.\n");
        return;
    }

    char *ptr = *tableHeaderPtr;

    // Populate attribute names, data types, and type lengths
    for (int i = 0; i < schema->numAttr; i++) {
        // Copy attribute name
        strncpy(ptr, schema->attrNames[i], MAX_ATTR_NAME_LEN);
        ptr += MAX_ATTR_NAME_LEN;

        // Copy data type
        *(DataType *)ptr = schema->dataTypes[i];
        ptr += sizeof(DataType);

        // Copy type length
        *(int *)ptr = schema->typeLength[i];
        ptr += sizeof(int);
    }

    // Populate key attribute indices
    for (int i = 0; i < schema->keySize; i++) {
        *(int *)ptr = schema->keyAttrs[i];
        ptr += sizeof(int);
    }

    // Update the table header pointer
    *tableHeaderPtr = ptr;
}

RC openTable(RM_TableData *rel, char *name) {
    if (rel == NULL || name == NULL) {
        return RC_GENERAL_ERROR;
    }

    RC resultCode;
    int attributeIndex;
    TableManager *tableManager = calloc(1, sizeof(TableManager));
    BM_BufferPool *bufferManager = calloc(1, sizeof(BM_BufferPool));
    BM_PageHandle *pageHandle = calloc(1, sizeof(BM_PageHandle));
    Schema *schema = calloc(1, sizeof(Schema));
    char *tableHeader;

    // Check memory allocation
    if (!tableManager || !bufferManager || !pageHandle || !schema) {
        resultCode = RC_MEMORY_ALLOCATION_FAIL;
        goto CLEANUP;
    }

    // Initialize buffer pool
    resultCode = initBufferPool(bufferManager, name, 3, RS_FIFO, NULL);
    if (resultCode != RC_OK) {
        goto CLEANUP;
    }

    // Pin the first page in the buffer pool
    resultCode = pinPage(bufferManager, pageHandle, 0);
    if (resultCode != RC_OK) {
        goto CLEANUP;
    }

    tableHeader = pageHandle->data;

    // Load table manager metadata from header
    tableManager->totalTuples = *(int *)tableHeader; tableHeader += sizeof(int);
    tableManager->recSize = *(int *)tableHeader; tableHeader += sizeof(int);
    tableManager->firstFreePageNum = *(int *)tableHeader; tableHeader += sizeof(int);
    tableManager->firstFreeSlotNum = *(int *)tableHeader; tableHeader += sizeof(int);
    tableManager->firstDataPageNum = *(int *)tableHeader; tableHeader += sizeof(int);

    // Load schema metadata
    schema->numAttr = *(int *)tableHeader; tableHeader += sizeof(int);
    schema->keySize = *(int *)tableHeader; tableHeader += sizeof(int);

    schema->attrNames = calloc(schema->numAttr, sizeof(char *));
    schema->dataTypes = calloc(schema->numAttr, sizeof(DataType));
    schema->typeLength = calloc(schema->numAttr, sizeof(int));
    schema->keyAttrs = calloc(schema->keySize, sizeof(int));

    if (!schema->attrNames || !schema->dataTypes || !schema->typeLength || !schema->keyAttrs) {
        resultCode = RC_MEMORY_ALLOCATION_FAIL;
        goto CLEANUP;
    }

    // Load attribute details
    for (attributeIndex = 0; attributeIndex < schema->numAttr; attributeIndex++) {
        schema->attrNames[attributeIndex] = strdup(tableHeader);
        tableHeader += MAX_ATTR_NAME_LEN;

        schema->dataTypes[attributeIndex] = *(DataType *)tableHeader;
        tableHeader += sizeof(DataType);

        schema->typeLength[attributeIndex] = *(int *)tableHeader;
        tableHeader += sizeof(int);
    }

    // Load key attribute indices
    for (attributeIndex = 0; attributeIndex < schema->keySize; attributeIndex++) {
        schema->keyAttrs[attributeIndex] = *(int *)tableHeader;
        tableHeader += sizeof(int);
    }

    // Unpin the page after reading
    resultCode = unpinPage(bufferManager, pageHandle);
    if (resultCode != RC_OK) {
        goto CLEANUP;
    }

    // Set up RM_TableData fields
    rel->name = strdup(name);
    rel->schema = schema;
    rel->mgmtData = tableManager;

    // Assign pointers for buffer manager and page handle
    tableManager->bufferManagerPtr = bufferManager;
    tableManager->pageHandlePtr = pageHandle;

    return RC_OK;

CLEANUP:
    free(tableManager);
    free(bufferManager);
    free(pageHandle);
    free(schema);
    if (resultCode == RC_MEMORY_ALLOCATION_FAIL) {
        fprintf(stderr, "Error: Memory allocation failed in openTable.\n");
    }
    return resultCode;
}

RC closeTable(RM_TableData *rel) {
    if (rel == NULL || rel->mgmtData == NULL || rel->schema == NULL) {
        return RC_GENERAL_ERROR;
    }

    TableManager *tableManager = rel->mgmtData;
    RC resultCode = RC_OK;

    // Pin the page to update the header
    RC pinStatus = pinPage(tableManager->bufferManagerPtr, tableManager->pageHandlePtr, 0);
    if (pinStatus == RC_OK) {
        // Update page header with table manager data
        int *pageHeader = (int *)tableManager->pageHandlePtr->data;
        *pageHeader++ = tableManager->totalTuples;
        *pageHeader++ = tableManager->recSize;
        *pageHeader++ = tableManager->firstFreePageNum;
        *pageHeader++ = tableManager->firstFreeSlotNum;
        *pageHeader = tableManager->firstDataPageNum;

        // Mark as dirty and unpin the page
        if ((resultCode = markDirty(tableManager->bufferManagerPtr, tableManager->pageHandlePtr)) == RC_OK) {
            resultCode = unpinPage(tableManager->bufferManagerPtr, tableManager->pageHandlePtr);
        }
    } else {
        resultCode = pinStatus;
    }

    // Shutdown buffer pool
    RC shutdownStatus = shutdownBufferPool(tableManager->bufferManagerPtr);
    if (shutdownStatus != RC_OK && resultCode == RC_OK) {
        resultCode = shutdownStatus;
    }

    // Free schema resources
    if (rel->schema) {
        for (int i = 0; i < rel->schema->numAttr; i++) {
            free(rel->schema->attrNames[i]);
        }
        free(rel->schema->attrNames);
        free(rel->schema->dataTypes);
        free(rel->schema->typeLength);
        free(rel->schema->keyAttrs);
        free(rel->schema);
    }

    // Free table manager
    free(tableManager);

    return resultCode;
}

RC deleteTable(char *name) {
    if (name == NULL || name[0] == '\0') {
        return RC_INVALID_HEADER;
    }

    RC resultCode = destroyPageFile(name);
    return resultCode;
}

RC getNumTuples(RM_TableData *rel) {
    if (rel == NULL || rel->mgmtData == NULL) {
        return -1; // Return -1 to indicate an error
    }

    TableManager *tableManager = (TableManager *)rel->mgmtData;
    if (tableManager == NULL) return -1;

    return tableManager->totalTuples;
}

RC insertRecord(RM_TableData *rel, Record *record) {
    if (rel == NULL || record == NULL || rel->mgmtData == NULL) {
        return RC_GENERAL_ERROR;
    }

    TableManager *tableMgmt = rel->mgmtData;
    BM_PageHandle *pageHandle = tableMgmt->pageHandlePtr;

    // Calculate available slots per page based on record size
    int slotsAvailableOnPage = (PAGE_SIZE - sizeof(PageHeader)) / (tableMgmt->recSize + 2);

    // Pin the page for inserting the record
    RC pagePinStatus = pinPage(tableMgmt->bufferManagerPtr, pageHandle, tableMgmt->firstFreePageNum);
    if (pagePinStatus != RC_OK) {
        return RC_ERROR;
    }

    char *currentPageData = pageHandle->data;
    PageHeader *currentPageHeader = (PageHeader *)currentPageData;

    // Initialize or update the page header
    if (currentPageHeader->pageIdentifier != 'Y') {
        currentPageHeader->pageIdentifier = 'Y';
        currentPageHeader->totalTuples = 0;
        currentPageHeader->freeSlotCnt = slotsAvailableOnPage - 1;
        currentPageHeader->nextFreeSlotInd = 1;
        currentPageHeader->prevFreePageIndex = -1;
        currentPageHeader->nextFreePageIndex = pageHandle->pageNum + 1;
        currentPageHeader->prevDataPageIndex = -1;
        currentPageHeader->nextDataPageIndex = 1;
    } else {
        currentPageHeader->totalTuples++;
        currentPageHeader->freeSlotCnt--;
        currentPageHeader->nextFreeSlotInd = (currentPageHeader->freeSlotCnt > 0)
                                              ? currentPageHeader->nextFreeSlotInd + 1
                                              : -currentPageHeader->nextFreeSlotInd;
    }

    // Calculate position to insert the new record
    int positionForNewData = sizeof(PageHeader) + (tableMgmt->firstFreeSlotNum * (tableMgmt->recSize + 2));
    currentPageData[positionForNewData] = 'Y';  // Mark slot as occupied
    memcpy(currentPageData + positionForNewData + 1, record->data, tableMgmt->recSize);  // Copy record data
    currentPageData[positionForNewData + tableMgmt->recSize + 1] = '|';  // Delimiter for end of record

    // Set the record's ID for tracking
    record->id.page = pageHandle->pageNum;
    record->id.slot = tableMgmt->firstFreeSlotNum;

    // Update the TableManager's free slot information
    if (currentPageHeader->freeSlotCnt == 0) {
        tableMgmt->firstFreePageNum++;
        tableMgmt->firstFreeSlotNum = 0;
    } else {
        tableMgmt->firstFreeSlotNum++;
    }
    tableMgmt->totalTuples++;

    // Mark the page as dirty and unpin it
    RC dirtyStatus = markDirty(tableMgmt->bufferManagerPtr, pageHandle);
    RC unpinStatus = unpinPage(tableMgmt->bufferManagerPtr, pageHandle);

    return (dirtyStatus == RC_OK && unpinStatus == RC_OK) ? RC_OK : RC_ERROR;
}


/*********************************
*  Handling records in a table
**********************************/
RC getRecord(RM_TableData *rel, RID id, Record *record) {
    if (rel == NULL || record == NULL || rel->mgmtData == NULL) {
        return RC_GENERAL_ERROR;
    }

    TableManager *tableManager = rel->mgmtData;
    int slotsPerRecord = (PAGE_SIZE - sizeof(PageHeader)) / (tableManager->recSize + 2);

    // Check if slot ID is within valid range
    if (id.slot >= slotsPerRecord) {
        return RC_RECORD_NOT_FOUND;
    }

    BM_PageHandle *pageHandler = tableManager->pageHandlePtr;
    RC pinPageStatus = pinPage(tableManager->bufferManagerPtr, pageHandler, id.page);
    if (pinPageStatus != RC_OK) {
        return RC_ERROR;
    }

    // Calculate location of the desired record in the page
    char *recordLocation = pageHandler->data + sizeof(PageHeader) + (id.slot * (tableManager->recSize + 2));
    if (*recordLocation != 'Y') {
        unpinPage(tableManager->bufferManagerPtr, pageHandler);
        return RC_RECORD_NOT_FOUND;
    }

    // Copy record data to the output structure
    memcpy(record->data, recordLocation + 1, tableManager->recSize);
    record->id = id;

    // Unpin the page and return the result
    return unpinPage(tableManager->bufferManagerPtr, pageHandler);
}

RC updateRecord(RM_TableData *rel, Record *record) {
    if (rel == NULL || record == NULL || rel->mgmtData == NULL) {
        return RC_GENERAL_ERROR;
    }

    TableManager *tableManager = (TableManager *)rel->mgmtData;
    int maxSlotsPerRecord = (PAGE_SIZE - sizeof(PageHeader)) / (tableManager->recSize + 2);

    // Check if the slot ID is within valid range
    if (record->id.slot >= maxSlotsPerRecord) {
        return RC_RECORD_NOT_FOUND;
    }

    BM_PageHandle *pageHandle = tableManager->pageHandlePtr;
    RC pinResult = pinPage(tableManager->bufferManagerPtr, pageHandle, record->id.page);
    if (pinResult != RC_OK) {
        return RC_ERROR;
    }

    // Locate the target slot and check if it's occupied
    char *targetSlot = pageHandle->data + sizeof(PageHeader) + (record->id.slot * (tableManager->recSize + 2));
    if (*targetSlot != 'Y') {
        unpinPage(tableManager->bufferManagerPtr, pageHandle);
        return RC_RECORD_NOT_FOUND;
    }

    // Update record data in the target slot
    memcpy(targetSlot + 1, record->data, tableManager->recSize);

    // Mark the page as dirty and unpin it
    RC dirtyFlag = markDirty(tableManager->bufferManagerPtr, pageHandle);
    RC unpinFlag = unpinPage(tableManager->bufferManagerPtr, pageHandle);

    // Check the result of marking dirty and unpinning
    if (dirtyFlag != RC_OK || unpinFlag != RC_OK) {
        return RC_ERROR;
    }

    return RC_OK;
}

RC deleteRecord(RM_TableData *rel, RID id) {
    if (rel == NULL || rel->mgmtData == NULL) {
        return RC_GENERAL_ERROR;
    }

    TableManager *tableMgmt = (TableManager *)rel->mgmtData;
    int maximumSlotsPerPage = (PAGE_SIZE - sizeof(PageHeader)) / (tableMgmt->recSize + 2);

    // Check if slot ID is within valid range
    if (id.slot >= maximumSlotsPerPage) {
        return RC_RECORD_NOT_FOUND;
    }

    BM_PageHandle *pageHandle = tableMgmt->pageHandlePtr;
    RC pinStatus = pinPage(tableMgmt->bufferManagerPtr, pageHandle, id.page);
    if (pinStatus != RC_OK) {
        return pinStatus;
    }

    // Locate the record's position in the page and check if it is marked as 'Y' (occupied)
    int recordPosition = sizeof(PageHeader) + (id.slot * (tableMgmt->recSize + 2));
    char *recordIndicator = &pageHandle->data[recordPosition];
    if (*recordIndicator != 'Y') {
        unpinPage(tableMgmt->bufferManagerPtr, pageHandle);
        return RC_RECORD_NOT_FOUND;
    }

    // Mark the record as deleted by setting indicator to 'N'
    *recordIndicator = 'N';

    // Update the page header for the new free slot count and tuple count
    PageHeader *header = (PageHeader *)pageHandle->data;
    header->totalTuples = header->totalTuples > 0 ? header->totalTuples - 1 : 0;
    header->freeSlotCnt++;

    // Update table manager's tuple count
    tableMgmt->totalTuples = tableMgmt->totalTuples > 0 ? tableMgmt->totalTuples - 1 : 0;

    // Mark page as dirty and unpin it
    if (markDirty(tableMgmt->bufferManagerPtr, pageHandle) != RC_OK) {
        unpinPage(tableMgmt->bufferManagerPtr, pageHandle);
        return RC_ERROR;
    }

    RC unpinStatus = unpinPage(tableMgmt->bufferManagerPtr, pageHandle);
    return (unpinStatus == RC_OK) ? RC_OK : unpinStatus;
}



Schema *createSchema(int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys) {
    // Allocate memory for the schema structure
    Schema *schema = (Schema *)malloc(sizeof(Schema));
    if (schema == NULL) {
        return NULL;
    }

    schema->numAttr = numAttr;

    // Allocate and copy attribute names
    schema->attrNames = (char **)malloc(numAttr * sizeof(char *));
    if (schema->attrNames == NULL) {
        free(schema);
        return NULL;
    }
    for (int i = 0; i < numAttr; i++) {
        schema->attrNames[i] = (char *)malloc(strlen(attrNames[i]) + 1);
        if (schema->attrNames[i] == NULL) {
            for (int j = 0; j < i; j++) {
                free(schema->attrNames[j]);
            }
            free(schema->attrNames);
            free(schema);
            return NULL;
        }
        strcpy(schema->attrNames[i], attrNames[i]);
    }

    // Allocate and copy data types
    schema->dataTypes = (DataType *)malloc(numAttr * sizeof(DataType));
    if (schema->dataTypes == NULL) {
        for (int i = 0; i < numAttr; i++) {
            free(schema->attrNames[i]);
        }
        free(schema->attrNames);
        free(schema);
        return NULL;
    }
    memcpy(schema->dataTypes, dataTypes, numAttr * sizeof(DataType));

    // Allocate and copy type lengths
    schema->typeLength = (int *)malloc(numAttr * sizeof(int));
    if (schema->typeLength == NULL) {
        for (int i = 0; i < numAttr; i++) {
            free(schema->attrNames[i]);
        }
        free(schema->attrNames);
        free(schema->dataTypes);
        free(schema);
        return NULL;
    }
    memcpy(schema->typeLength, typeLength, numAttr * sizeof(int));

    // Allocate and copy key attributes
    schema->keySize = keySize;
    schema->keyAttrs = (int *)malloc(keySize * sizeof(int));
    if (schema->keyAttrs == NULL) {
        for (int i = 0; i < numAttr; i++) {
            free(schema->attrNames[i]);
        }
        free(schema->attrNames);
        free(schema->dataTypes);
        free(schema->typeLength);
        free(schema);
        return NULL;
    }
    memcpy(schema->keyAttrs, keys, keySize * sizeof(int));

    return schema;
}

RC freeSchema(Schema *schema) {
    if (schema == NULL) {
        return RC_OK;
    }

    // Free attribute names
    for (int i = 0; i < schema->numAttr; i++) {
        if (schema->attrNames[i] != NULL) {
            free(schema->attrNames[i]);
            schema->attrNames[i] = NULL;
        }
    }
    free(schema->attrNames);
    schema->attrNames = NULL;

    // Free data types
    free(schema->dataTypes);
    schema->dataTypes = NULL;

    // Free type lengths
    free(schema->typeLength);
    schema->typeLength = NULL;

    // Free key attributes
    free(schema->keyAttrs);
    schema->keyAttrs = NULL;

    // Free the schema structure itself
    free(schema);
    
    return RC_OK;
}

/****************************
*  Dealing with schemas
*****************************/
int getRecordSize(Schema *schema) {
    int totalSize = 0;

    for (int i = 0; i < schema->numAttr; ++i) {
        switch (schema->dataTypes[i]) {
            case DT_STRING:
                totalSize += schema->typeLength[i] * sizeof(char);
                break;
            case DT_INT:
                totalSize += sizeof(int);
                break;
            case DT_FLOAT:
                totalSize += sizeof(float);
                break;
            case DT_BOOL:
                totalSize += sizeof(bool);
                break;
        }
    }

    // Apply padding to align total size to the nearest multiple of 4
    int padding = totalSize % 4;
    if (padding != 0) {
        totalSize += 4 - padding;
    }

    return totalSize;
}

RC createRecord(Record **record, Schema *schema) {
    if (record == NULL || schema == NULL) {
        return RC_GENERAL_ERROR;
    }

    Record *newRecord = (Record *)malloc(sizeof(Record));
    if (newRecord == NULL) {
        return RC_MEMORY_ALLOCATION_FAIL;
    }

    int recordSize = getRecordSize(schema);
    newRecord->data = (char *)malloc(recordSize + 1);
    if (newRecord->data == NULL) {
        free(newRecord);
        return RC_MEMORY_ALLOCATION_FAIL;
    }

    memset(newRecord->data, 0, recordSize + 1);
    *record = newRecord;

    return RC_OK;
}

RC freeRecord(Record *record) {
    if (record == NULL) {
        return RC_RECORD_NOT_FOUND;
    }

    free(record->data);
    record->data = NULL;

    free(record);
    return RC_OK;
}

RC startScan(RM_TableData *rel, RM_ScanHandle *scan, Expr *conditionExpression) {
    if (rel == NULL || scan == NULL) {
        return RC_GENERAL_ERROR;
    }

    ScanManager *scanManager = (ScanManager *)calloc(1, sizeof(ScanManager));
    if (scanManager == NULL) {
        return RC_MEMORY_ALLOCATION_FAIL;
    }

    TableManager *tableManager = (TableManager *)rel->mgmtData;
    *scanManager = (ScanManager){
        .totalEntries = tableManager->totalTuples,
        .currentPageNum = tableManager->firstDataPageNum,
        .currentSlotNum = -1,
        .scanIndex = 0,
        .conditionExpression = conditionExpression
    };

    scan->mgmtData = scanManager;
    scan->rel = rel;

    return RC_OK;
}
RC next(RM_ScanHandle *scan, Record *record) {
    if (scan == NULL || record == NULL || scan->mgmtData == NULL || scan->rel == NULL) {
        return RC_GENERAL_ERROR;
    }

    ScanManager *scanMgr = scan->mgmtData;
    RM_TableData *tableData = scan->rel;
    TableManager *tableMgr = tableData->mgmtData;

    int pageHeaderSize = sizeof(PageHeader);
    int sizePerRecord = tableMgr->recSize + 2 * sizeof(char);  // Extra bytes per record
    int slotsPerPage = (PAGE_SIZE - pageHeaderSize) / sizePerRecord;

    if (scanMgr->scanIndex >= scanMgr->totalEntries) {
        return RC_RM_NO_MORE_TUPLES;
    }

    Value *evalResult = (Value *)calloc(1, sizeof(Value));
    if (evalResult == NULL) {
        return RC_MEMORY_ALLOCATION_FAIL;
    }

    while (scanMgr->scanIndex < scanMgr->totalEntries) {
        scanMgr->currentSlotNum++;
        if (scanMgr->currentSlotNum >= slotsPerPage) {
            scanMgr->currentPageNum++;
            scanMgr->currentSlotNum = 0;
        }

        RID currentRID = {.page = scanMgr->currentPageNum, .slot = scanMgr->currentSlotNum};
        RC recordStatus = getRecord(tableData, currentRID, record);
        if (recordStatus == RC_OK) {
            scanMgr->scanIndex++;

            if (scanMgr->conditionExpression) {
                evalExpr(record, tableData->schema, scanMgr->conditionExpression, &evalResult);
                if (evalResult->v.boolV) {
                    free(evalResult);
                    return RC_OK;
                }
            } else {
                free(evalResult);
                return RC_OK;
            }
        }
    }

    free(evalResult);
    return RC_RM_NO_MORE_TUPLES;
}


RC closeScan(RM_ScanHandle *scan) {
    if (scan == NULL || scan->mgmtData == NULL) {
        return RC_RECORD_NOT_FOUND;
    }

    free(scan->mgmtData);
    scan->mgmtData = NULL;

    return RC_OK;
}

RC getAttr(Record *record, Schema *schema, int attrNum, Value **value) {
    if (record == NULL || schema == NULL || value == NULL || attrNum < 0 || attrNum >= schema->numAttr) {
        return RC_GENERAL_ERROR;
    }

    *value = (Value *)calloc(1, sizeof(Value));
    if (*value == NULL) {
        return RC_MEMORY_ALLOCATION_FAIL;
    }

    (*value)->dt = schema->dataTypes[attrNum];
    char *baseAddr = record->data + getAttrPos(schema, attrNum);

    switch (schema->dataTypes[attrNum]) {
        case DT_STRING:
            (*value)->v.stringV = (char *)calloc(1, schema->typeLength[attrNum] + 1);
            if ((*value)->v.stringV == NULL) {
                free(*value);
                *value = NULL;
                return RC_MEMORY_ALLOCATION_FAIL;
            }
            strncpy((*value)->v.stringV, baseAddr, schema->typeLength[attrNum]);
            break;

        case DT_INT:
            (*value)->v.intV = *(int *)baseAddr;
            break;

        case DT_FLOAT:
            (*value)->v.floatV = *(float *)baseAddr;
            break;

        case DT_BOOL:
            (*value)->v.boolV = *(bool *)baseAddr;
            break;
    }

    return RC_OK;
}

RC setAttr(Record *record, Schema *schema, int attrNum, Value *value) {
    if (record == NULL || schema == NULL || value == NULL || attrNum < 0 || attrNum >= schema->numAttr) {
        return RC_GENERAL_ERROR;
    }

    char *target = record->data + getAttrPos(schema, attrNum);

    switch (schema->dataTypes[attrNum]) {
        case DT_INT:
            *(int *)target = value->v.intV;
            break;

        case DT_FLOAT:
            *(float *)target = value->v.floatV;
            break;

        case DT_STRING:
            memcpy(target, value->v.stringV, schema->typeLength[attrNum]);
            break;

        case DT_BOOL:
            *(bool *)target = value->v.boolV;
            break;
    }

    return RC_OK;
}


int getAttrPos(Schema *schema, int attrNum) {
    if (schema == NULL || attrNum < 0 || attrNum >= schema->numAttr) {
        return -1;  // Return -1 to indicate an invalid attribute position
    }

    int attrPos = 0;
    int sizes[] = {sizeof(int), sizeof(float), 0, sizeof(bool)};  // Index corresponds to DataType enum

    for (int i = 0; i < attrNum; ++i) {
        if (schema->dataTypes[i] == DT_STRING) {
            attrPos += schema->typeLength[i] * sizeof(char);  // Size of string attribute
        } else {
            attrPos += sizes[schema->dataTypes[i]];  // Size of non-string attribute
        }
    }

    return attrPos;
}
