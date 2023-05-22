///////////////////////////////////////////////////////////////////////////////
// Member:      Gabriel Selzer : 9076571836
// Member:      Ana Klabjan : 9081077043
// Member:      Salman Munaf : 9073929474
// ----------------------------------------------------------------------------
// Description: Buffer pool manager for a DBMS.
///////////////////////////////////////////////////////////////////////////////

#include <memory.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <fcntl.h>
#include <iostream>
#include <stdio.h>
#include "page.h"
#include "buf.h"

#define ASSERT(c)  { if (!(c)) { \
		       cerr << "At line " << __LINE__ << ":" << endl << "  "; \
                       cerr << "This condition should hold: " #c << endl; \
                       exit(1); \
		     } \
                   }

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(const int bufs)
{
    numBufs = bufs;

    bufTable = new BufDesc[bufs];
    memset(bufTable, 0, bufs * sizeof(BufDesc));
    for (int i = 0; i < bufs; i++) 
    {
        bufTable[i].frameNo = i;
        bufTable[i].valid = false;
    }

    bufPool = new Page[bufs];
    memset(bufPool, 0, bufs * sizeof(Page));

    int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
    hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

    clockHand = bufs - 1;
}


BufMgr::~BufMgr() {

    // flush out all unwritten pages
    for (int i = 0; i < numBufs; i++) 
    {
        BufDesc* tmpbuf = &bufTable[i];
        if (tmpbuf->valid == true && tmpbuf->dirty == true) {

#ifdef DEBUGBUF
            cout << "flushing page " << tmpbuf->pageNo
                 << " from frame " << i << endl;
#endif

            tmpbuf->file->writePage(tmpbuf->pageNo, &(bufPool[i]));
        }
    }

    delete [] bufTable;
    delete [] bufPool;
}


// Finds an empty frame in the buffer pool using the clock algorithm. 
// Only unpinned frames are considered for replacement.
// Dirty frames are flushed before being removed.
//
// Parameters:
//   frame - stores the index of the freed frame
// Return:
//   Execution status of the function.
//      UNIXERR if dirty frame could not be flushed.
//      BUFFEREXCEEDED if all buffer frames are pinned.
//      OK otherwise.

const Status BufMgr::allocBuf(int & frame) 
{
    // Find a spot in the buffer
    for(int count = 0; count < 2 * numBufs; count++) {
        advanceClock();
        if (!bufTable[clockHand].valid) {
            frame = clockHand;
            return OK;
        }
        if (bufTable[clockHand].refbit) {
            bufTable[clockHand].refbit = false;
            continue;
        }
        if (bufTable[clockHand].pinCnt > 0) {
            continue;
        }
        // All of the conditions are met - we will remove this page.
        frame = clockHand;
        // Flush out page if dirty
        if (bufTable[clockHand].dirty) {
            Status writeStatus = bufTable[clockHand].file->writePage(bufTable[clockHand].pageNo, &(bufPool[clockHand]));
            if (writeStatus != OK) {
                return UNIXERR;
            }
            bufStats.diskwrites++;
        }
        // Remove the old page from the hash table
        Status removeStatus = hashTable->remove(bufTable[clockHand].file, bufTable[clockHand].pageNo);
        if (removeStatus != OK) {
            return removeStatus;
        }

        // Return OK
        return OK;
    }
    // We've gone around twice, and didn't find a page - all pages must be pinned
    return BUFFEREXCEEDED;
}

// Reads in a page from a file, caching it in the buffer if necessary.
//
// Parameters:
//   file - the file to read from
//   pageNo - the page number corresponding to the needed page
//   page - the page read in from the file
// Return:
//   Execution status of the function.
//      UNIXERR if the page could not be read from the file
//      BUFFEREXCEEDED if all buffer frames are pinned.
//      HASHTBLERROR if a hash table error occurred.
//      OK otherwise.
	
const Status BufMgr::readPage(File* file, const int PageNo, Page*& page)
{

    // Look up the frame of corresponding to file, pageNo
    int frameNo;
    Status lookupStatus = hashTable->lookup(file, PageNo, frameNo);
    if (lookupStatus != OK) {
        // Page not in the buffer pool (Case 1)

        // Allocate a buffer frame
        Status allocBufStatus = allocBuf(frameNo);
        if (allocBufStatus != OK) {
            return allocBufStatus;
        }

        // Insert the page into the hash table
        Status insertStatus = hashTable->insert(file, PageNo, frameNo);
        if (insertStatus != OK) {
            return insertStatus;
        }

        // Invoke Set() on the frame
        bufTable[frameNo].Set(file, PageNo);

        // Read the page in from disk
        Status readStatus = file->readPage(PageNo, &bufPool[frameNo]);
        if (readStatus != OK) {
            // If we couldn't read in the page, remove it from the manager
            disposePage(file, PageNo);
            return UNIXERR;
        }
        bufStats.diskreads++;
    }
    else {
        // Page in the buffer pool (Case 2)

        // Set the reference bit for the frame
        bufTable[frameNo].refbit = true;

        // Increment the pin count
        bufTable[frameNo].pinCnt++;
    }

    // Return the page from the buffer pool
    page = &bufPool[frameNo];
    return OK;
}

// Unpins a page, setting the dirty bit if necessary.
//
// Parameters:
//   file - the file containing the page
//   pageNo - the page number of the page
//   dirty - determines whether the dirty bit should be set
// Return:
//   Execution status of the function.
//      HASHNOTFOUND if the page was not in the buffer.
//      PAGENOTPINNED if the page is not pinned in the buffer.
//      OK otherwise.

const Status BufMgr::unPinPage(File* file, const int PageNo, 
			       const bool dirty) 
{
    // Look up the frame of corresponding to file, pageNo
    int frameNo;
    Status lookupStatus = hashTable->lookup(file, PageNo, frameNo);
    if (lookupStatus != OK) {
        return HASHNOTFOUND;
    }

    // If page count is zero, return PAGENOTPINNED
    if (bufTable[frameNo].pinCnt == 0) {
        return PAGENOTPINNED;
    }

    // Decrement pinCnt
    bufTable[frameNo].pinCnt--;

    // Set dirty bit if needed
    bufTable[frameNo].dirty |= dirty;

    return OK;
}

// Allocates a page in a file, and inserts it into the buffer pool
//
// Parameters:
//   file - the file where the new page will be inserted
//   pageNo - the page number of the newly allocated page
//   page - a pointer to the newly allocated page in the buffer pool
// Return:
//   Execution status of the function.
//      UNIXERR if dirty frame could not be flushed.
//      BUFFEREXCEEDED if all buffer frames are pinned.
//      HASHTBLERROR if a hash table error occurred.
//      OK otherwise.

const Status BufMgr::allocPage(File* file, int& pageNo, Page*& page) 
{
    // Allocate an empty page
    Status allocPageStatus = file->allocatePage(pageNo);
    if (allocPageStatus != OK) {
        return UNIXERR;
    }

    // Allocate an empty buffer frame
    int frameNo;
    Status allocBufStatus = allocBuf(frameNo);
    if (allocBufStatus != OK) {
        return allocBufStatus;
    }

    // Insert the entry into the hash table
    Status insertStatus = hashTable->insert(file, pageNo, frameNo);
    if (insertStatus != OK) {
        return HASHTBLERROR;
    }

    // Invoke Set() on the frame
    bufTable[frameNo].Set(file, pageNo);
    page = &bufPool[frameNo];
    return OK;
}

const Status BufMgr::disposePage(File* file, const int pageNo) 
{
    // see if it is in the buffer pool
    Status status = OK;
    int frameNo = 0;
    status = hashTable->lookup(file, pageNo, frameNo);
    if (status == OK)
    {
        // clear the page
        bufTable[frameNo].Clear();
    }
    status = hashTable->remove(file, pageNo);

    // deallocate it in the file
    return file->disposePage(pageNo);
}

const Status BufMgr::flushFile(const File* file) 
{
  Status status;

  for (int i = 0; i < numBufs; i++) {
    BufDesc* tmpbuf = &(bufTable[i]);
    if (tmpbuf->valid == true && tmpbuf->file == file) {

      if (tmpbuf->pinCnt > 0)
	  return PAGEPINNED;

      if (tmpbuf->dirty == true) {
#ifdef DEBUGBUF
	cout << "flushing page " << tmpbuf->pageNo
             << " from frame " << i << endl;
#endif
	if ((status = tmpbuf->file->writePage(tmpbuf->pageNo,
					      &(bufPool[i]))) != OK)
	  return status;

	tmpbuf->dirty = false;
      }

      hashTable->remove(file,tmpbuf->pageNo);

      tmpbuf->file = NULL;
      tmpbuf->pageNo = -1;
      tmpbuf->valid = false;
    }

    else if (tmpbuf->valid == false && tmpbuf->file == file)
      return BADBUFFER;
  }
  
  return OK;
}


void BufMgr::printSelf(void) 
{
    BufDesc* tmpbuf;
  
    cout << endl << "Print buffer...\n";
    for (int i=0; i<numBufs; i++) {
        tmpbuf = &(bufTable[i]);
        cout << i << "\t" << (char*)(&bufPool[i]) 
             << "\tpinCnt: " << tmpbuf->pinCnt;
    
        if (tmpbuf->valid == true)
            cout << "\tvalid\n";
        cout << endl;
    };
}


