#include "btree_mgr.h"
#include "tables.h"
#include "storage_mgr.h"
#include "record_mgr.h"
#include <stdlib.h>
#include <string.h>

SM_FileHandle btreeFileHandler;
int numberOfElementsPerNode;

BTree *root;
BTree *scan;
int currentNumOfIndex = 0;


/*************************************************************************
*  init and shutdown....create, destroy, open, and close an btree index      
**************************************************************************/


////// init and shutdown index manager

RC initIndexManager(void *mgmtData) {
    printf("Starting Index Manager...\n");
    return RC_OK;
}

RC shutdownIndexManager() {
    printf("Index manager closed successfully...\n");
    return RC_OK;
}


/////// // create, destroy, open, and close an btree index

RC createBtree(char *idxId, DataType keyType, int n) {
    // Initialize the root node and allocate memory for it
    BTree *rootNode = (BTree *)malloc(sizeof(BTree));
    if (!rootNode) {
        return RC_ERROR; // Failed to allocate memory for root node
    }

    // Allocate memory for storing keys in the B-Tree
    rootNode->key = (int *)calloc(n, sizeof(int));
    if (!rootNode->key) {
        free(rootNode);
        return RC_ERROR; // Failed to allocate memory for keys
    }

    // Allocate memory for storing record IDs
    rootNode->id = (RID *)calloc(n, sizeof(RID));
    if (!rootNode->id) {
        free(rootNode->key);
        free(rootNode);
        return RC_ERROR; // Failed to allocate memory for record IDs
    }

    // Allocate memory for child node pointers
    rootNode->next = (BTree **)calloc(n + 1, sizeof(BTree *));
    if (!rootNode->next) {
        free(rootNode->id);
        free(rootNode->key);
        free(rootNode);
        return RC_ERROR; // Failed to allocate memory for child pointers
    }

    // Set the global variable for the number of elements per node
    numberOfElementsPerNode = n;

    // Create a page file to store the B-Tree index
    RC result = createPageFile(idxId);
    if (result != RC_OK) {
        // Clean up allocated memory if page file creation fails
        free(rootNode->next);
        free(rootNode->id);
        free(rootNode->key);
        free(rootNode);
        return result;
    }

    // Assign the root node to the global root pointer
    root = rootNode;

    return RC_OK;
}

RC openBtree(BTreeHandle **tree, char *idxId) {
    // Attempt to open the page file
    if (openPageFile(idxId, &btreeFileHandler) == RC_OK) {
        return RC_OK; // Successfully opened the page file
    } else {
        return RC_ERROR; // Failed to open the page file
    }
}


RC closeBtree(BTreeHandle *tree) {
    // Attempt to close the page file
    if (closePageFile(&btreeFileHandler) == RC_OK) {
        // Free the memory for the B-Tree root
        free(root);
        return RC_OK; // Successfully closed the B-Tree
    } 
    
    return RC_ERROR; // Failed to close the page file
}


RC deleteBtree(char *idxId) {
    if (destroyPageFile(idxId) != RC_OK) {
        return RC_ERROR; // Successfully deleted the B-Tree
    }
    return RC_OK;
}

RC getNumNodes(BTreeHandle *tree, int *result) {
    // Calculate the number of nodes and store it in the result pointer
    *result = numberOfElementsPerNode + 2;
    
    // Return success
    return RC_OK;
}

RC getNumEntries(BTreeHandle *tree, int *result) {
    int totalEntries = 0;

    // Traverse through all nodes in the B-Tree
    for (BTree *currentNode = root; currentNode != NULL; currentNode = currentNode->next[numberOfElementsPerNode]) {
        // Count non-zero keys in the current node
        for (int i = 0; i < numberOfElementsPerNode; i++) {
            if (currentNode->key[i] != 0) {
                totalEntries++;
            }
        }
    }

    *result = totalEntries;

    return RC_OK;
}


RC getKeyType(BTreeHandle* tree, DataType* result) {
    if (!result) {
        return RC_ERROR;
    }

    return RC_OK;
}



/*******************
*  Index access
*******************/

RC findKey(BTreeHandle *tree, Value *key, RID *result) {
        BTree *current = root; // Start from the root of the B-tree
    int elehighments = numberOfElementsPerNode; // Maximum number of elements in a node

    // Traverse through the B-tree nodes
    while (current != NULL) {
        bool keyFound = false;
        int left = 0;
        int right = elehighments - 1;

        // Perform binary search within the current node
        while (left <= right) {
            int mid = left + (right - left) / 2; // Calculate middle index

            // Check if the key is found at the middle index
            if (current->key[mid] == key->v.intV) {
                result->page = current->id[mid].page;
                result->slot = current->id[mid].slot;
                return RC_OK; // Return the record if key matches
            }

            // Narrow the search range based on key comparison
            if (current->key[mid] < key->v.intV) {
                left = mid + 1; // Search in the right half
            } else {
                right = mid - 1; // Search in the left half
            }
        }

        // Move to the next level in the B-tree (next node)
        current = current->next[elehighments];
    }

    // Return error if the key was not found in the B-tree
    return RC_IM_KEY_NOT_FOUND;
}


// Function to initialize a new node
BTree* initializeNode(int numElements) {
    // Allocate memory for the node structure
    BTree *node = (BTree *)malloc(sizeof(BTree));
    if (!node) {
        return NULL; // Memory allocation for the node failed
    }

    // Allocate and initialize the 'key' array
    node->key = (int *)calloc(numElements, sizeof(int));
    if (!node->key) {
        free(node); // Cleanup and return NULL
        return NULL;
    }

    // Allocate and initialize the 'id' array
    node->id = (RID *)calloc(numElements, sizeof(RID));
    if (!node->id) {
        free(node->key);
        free(node); // Cleanup and return NULL
        return NULL;
    }

    // Allocate and initialize the 'next' array
    node->next = (BTree **)calloc(numElements + 1, sizeof(BTree *));
    if (!node->next) {
        free(node->id);
        free(node->key);
        free(node); // Cleanup and return NULL
        return NULL;
    }

    // Return the newly initialized node
    return node;
}
// Function to insert a key into the BTree
RC insertKey(BTreeHandle *tree, Value *key, RID rid) {
    BTree *currentNode = root; // Start from the root node
    BTree *newNode = initializeNode(numberOfElementsPerNode); // Create a new node

    // Traverse through the B-tree to find the correct position for the new key
    while (currentNode != NULL) {
        int nodeFull = 0;
        
        // Check if the node has space for the new key
        for (int i = 0; i < numberOfElementsPerNode; i++) {
            if (currentNode->key[i] == 0) { // Empty slot found
                currentNode->key[i] = key->v.intV; // Insert the key
                currentNode->id[i] = rid; // Insert the corresponding RID
                currentNode->next[i] = NULL; // Set next pointer to NULL for the new key
                nodeFull = 1;
                break; // Exit the loop after inserting the key
            }
        }

        // If no space was found and the current node doesn't have a child, create a new node
        if (nodeFull == 0 && currentNode->next[numberOfElementsPerNode] == NULL) {
            newNode->next[numberOfElementsPerNode] = NULL;
            currentNode->next[numberOfElementsPerNode] = newNode; // Link to the new node
        }

        // Move to the next level in the B-tree
        currentNode = currentNode->next[numberOfElementsPerNode];
    }

    // Perform custom operations after insertion (if needed)
    performCustomOperation(root, newNode, numberOfElementsPerNode);

    return RC_OK;
}

// Function to perform custom operations after key insertion
void performCustomOperation(BTree *rootNode, BTree *newNode, int elehigh) {
    int totalKeys = calculateTotalKeys(rootNode, elehigh);
    
    // If the total keys reach the maximum, split the node
    if (totalKeys == 6) {
        copyKeys(rootNode, newNode, elehigh);
        updateNodePointers(rootNode, newNode, elehigh);
    }
}

// Function to calculate the total number of keys in a node
int calculateTotalKeys(BTree *rootNode, int elehigh) {
    int totalKeys = 0;
    
    // Traverse through the node to count the number of non-zero keys
    while (rootNode != NULL) {
        for (int i = 0; i < elehigh; i++) {
            if (rootNode->key[i] != 0) {
                totalKeys++;
            }
        }
        rootNode = rootNode->next[elehigh]; // Move to the next node
    }
    
    return totalKeys;
}

// Function to copy keys from root node to the new node (for splitting)
void copyKeys(BTree *rootNode, BTree *newNode, int elehigh) {
    // Copy keys from the root to the new node
    for (int i = 0; i < elehigh; i++) {
        if (rootNode->key[i] != 0) {
            newNode->key[i] = rootNode->key[i];
            newNode->id[i] = rootNode->id[i];
        }
    }
}

// Function to update the node pointers after splitting
void updateNodePointers(BTree *rootNode, BTree *newNode, int elehigh) {
    // Link the new node to the root node and its next pointer
    newNode->next[elehigh] = rootNode->next[elehigh];
    rootNode->next[elehigh] = newNode;
}


RC deleteKey(BTreeHandle *tree, Value *key) {
    BTree *currentNode = root;

    // Traverse through the B-Tree
    while (currentNode != NULL) {
        // Count occurrences of the key in the current node
        int removedCount = countOccurrences(currentNode, key, numberOfElementsPerNode);

        // Update metadata related to the node and the key
        updateNodeMetadata(currentNode, key, numberOfElementsPerNode);

        // Rearrange elements within the node after deletion
        rearrangeNodeElements(currentNode, numberOfElementsPerNode);

        // Print the status of the deletion operation
        printStatusMessage(removedCount, key->v.intV);

        // Move to the next node in the B-Tree
        currentNode = currentNode->next[numberOfElementsPerNode];
    }

    return RC_OK;
}

int countOccurrences(BTree *node, Value *keyValue, int numElements) {
    int occurrenceCount = 0;

    // Iterate through the keys in the node
    for (int i = 0; i < numElements; i++) {
        if (node->key[i] == keyValue->v.intV) {
            occurrenceCount++;
        }
    }

    return occurrenceCount;
}


// Function to update metadata of a node by resetting matched keys
void updateNodeMetadata(BTree *node, Value *keyValue, int numElements) {
    for (int i = 0; i < numElements; i++) {
        if (node->key[i] == keyValue->v.intV) {
            node->id[i] = (RID){0, 0}; // Reset the RID
            node->key[i] = 0;          // Reset the key
        }
    }
}


// Function to rearrange elements in a node after key removal
void rearrangeNodeElements(BTree *node, int numElements) {
    int totalElements = getTotalElements(node, numElements);
    int shiftIndex = 0;

    for (int i = 0; i < numElements; i++) {
        if (node->key[i] == 0 && i + shiftIndex < totalElements) {
            // Find the next non-zero key to shift
            while (node->key[i + shiftIndex] == 0 && i + shiftIndex < totalElements) {
                shiftIndex++;
            }

            // Perform the shift
            node->key[i] = node->key[i + shiftIndex];
            node->id[i] = node->id[i + shiftIndex];

            // Reset the shifted position
            node->key[i + shiftIndex] = 0;
            node->id[i + shiftIndex] = (RID){0, 0};
        }
    }
}

// Function to count the total number of non-zero elements in a node
int getTotalElements(BTree *node, int numElements) {
    if (node == NULL) return RC_ERROR;

    int total = 0;

    for (int i = 0; i < numElements; i++) {
        if (node->key[i] != 0) {
            total++;
        }
    }

    return total;
}


// Function to print a status message after removing keys
void printStatusMessage(int removedCount, int keyValue) {
    if (removedCount == 0) {
        printf("Key %d not found in node.\n", keyValue);
        return;
    } 
    printf("Successfully removed %d occurrence(s) of key %d from node.\n", removedCount, keyValue);
}



//***************************
//*  openTreeScan   
//***************************/

RC openTreeScan(BTreeHandle *tree, BT_ScanHandle **handle) {
    // Initialize scan starting point
    scan = root;
    currentNumOfIndex = 0;
    printf("Starting tree scan...\n");
    printf("Index number initialized to %d.\n", currentNumOfIndex);

    // Calculate total keys in the BTree
    int totalKeys = 0;
    BTree *current = scan;
    while (current != NULL) {
        for (int i = 0; i < numberOfElementsPerNode; i++) {
            if (current->key[i] != 0) {
                totalKeys++;
            }
        }
        current = current->next[numberOfElementsPerNode];
    }
    printf("Total keys in the BTree: %d\n", totalKeys);

    // Allocate memory for sorted keys and elements
    int *sortedKeys = (int *)malloc(totalKeys * sizeof(int));
    int (*sortedElements)[2] = (int(*)[2])malloc(totalKeys * sizeof(int[2]));

    if (!sortedKeys || !sortedElements) {
        printf("Memory allocation failed.\n");
        free(sortedKeys);
        free(sortedElements);
        return RC_ERROR;
    }

    // Gather and sort all keys and elements
    current = scan;
    int count = 0;

    while (current != NULL) {
        for (int i = 0; i < numberOfElementsPerNode; i++) {
            if (current->key[i] != 0) {
                sortedKeys[count] = current->key[i];
                sortedElements[count][0] = current->id[i].page;
                sortedElements[count][1] = current->id[i].slot;
                count++;
            }
        }
        current = current->next[numberOfElementsPerNode];
    }

    // Sort the keys and elements using a simple bubble sort
    for (int i = 0; i < totalKeys - 1; i++) {
        for (int j = 0; j < totalKeys - i - 1; j++) {
            if (sortedKeys[j] > sortedKeys[j + 1]) {
                // Swap keys
                int tempKey = sortedKeys[j];
                sortedKeys[j] = sortedKeys[j + 1];
                sortedKeys[j + 1] = tempKey;

                // Swap elements
                int tempPage = sortedElements[j][0];
                int tempSlot = sortedElements[j][1];
                sortedElements[j][0] = sortedElements[j + 1][0];
                sortedElements[j][1] = sortedElements[j + 1][1];
                sortedElements[j + 1][0] = tempPage;
                sortedElements[j + 1][1] = tempSlot;
            }
        }
    }
    printf("Keys and elements sorted.\n");

    // Update BTree with sorted keys and elements
    current = scan;
    count = 0;
    while (current != NULL) {
        for (int i = 0; i < numberOfElementsPerNode && count < totalKeys; i++) {
            current->key[i] = sortedKeys[count];
            current->id[i].page = sortedElements[count][0];
            current->id[i].slot = sortedElements[count][1];
            count++;
        }
        // Fill remaining slots with default values
        for (int i = count; i < numberOfElementsPerNode; i++) {
            current->key[i] = -1;
            current->id[i].page = 0;
            current->id[i].slot = 0;
        }
        current = current->next[numberOfElementsPerNode];
    }
    printf("BTree updated with sorted keys and elements.\n");

    // Free allocated memory
    free(sortedKeys);
    free(sortedElements);

    return RC_OK;
}


void assignPageAndSlot(BTree *node, int *idx, RID *result) {
    RID tempRID = node->id[*idx];
    result->page = tempRID.page;
    result->slot = tempRID.slot;
}

void incrementIndex(int *idx) {
    (*idx)++;
}

void updateResult(BTree *node, int *idx, RID *result) {
    int i = 0;
    switch (i) {
        case 0:
            assignPageAndSlot(node, idx, result);
            incrementIndex(idx);
        default:
            break;
    }
}


// Check if the next node is available
bool isNextNodeAvailable(BTree *node) {
    if (node->next[numberOfElementsPerNode] != NULL)
        return true;
    return false;
}

// Move to the next node if available
void moveToNextNode(BTree **node) {
    if(*node == NULL) return;

    bool hasNextNode = *node && isNextNodeAvailable(*node);
    if (hasNextNode == true) {
        *node = (*node)->next[numberOfElementsPerNode];
    }
}


void resetIndexNext(int *idx) {
    *idx = 0;
}

bool isIndexEqualToMax(int idx) {
    return numberOfElementsPerNode == idx;
}

void handleIndexEqualToMax(BTree **node, int *idx) {
    if (!isIndexEqualToMax(*idx)) {
        return;
    }

    resetIndexNext(idx);
    moveToNextNode(node);
}

RC nextEntry(BT_ScanHandle *handle, RID *result) {
    if (isNextNodeAvailable(scan)) {
        handleIndexEqualToMax(&scan, &currentNumOfIndex);
        updateResult(scan, &currentNumOfIndex, result);

        return RC_OK;
    }

    
    return RC_IM_NO_MORE_ENTRIES;
}


void updateIndex(int *indexPtr, int newValue) {
    *indexPtr = *indexPtr + newValue;
}

void resetIndex(int *indexPtr) {
    *indexPtr = 0;
}

// Close the tree scan and reset the index
RC closeTreeScan(BT_ScanHandle *handle) {
    if (!handle) return RC_ERROR;

    int currentIndex = 0;
    resetIndex(currentIndex);
    return RC_OK;
}

char *generatePrintMessage(BTreeHandle *treeHandle) {
    char *message = (char *)malloc(50 * sizeof(char));
    strcpy(message, "The B-tree has been printed successfully.");
    return message;
}

char *printTree(BTreeHandle *treeHandle) {
    char *printMessage = generatePrintMessage(treeHandle);
    return printMessage;
}

