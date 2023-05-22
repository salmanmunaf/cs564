///////////////////////////////////////////////////////////////////////////////
// Member:      Gabriel Selzer : 9076571836
// Member:      Ana Klabjan : 9081077043
// Member:      Salman Munaf : 9073929474
// ----------------------------------------------------------------------------
// Description: Heap File storing database records, along with a scan mechanism
//              allowing filtered search queries.
///////////////////////////////////////////////////////////////////////////////

#include "heapfile.h"
#include "error.h"


// creates a new heapFile with a given name
// allocates space in the disk for header, initializes the values
// allocates space in the disk for the first page
// does nothing if the file already exists
//
// Parameters:
//   fileName - the name of the File to be created
// Return:
//   Execution status of the function.
//   FILEEXISTS if a file with the same name already exists
const Status createHeapFile(const string fileName)
{
    File* 		file;
    Status 		status;
    FileHdrPage*	hdrPage;
    int			hdrPageNo;
    int			newPageNo;
    Page*		newPage;

    // try to open the file. This should return an error

    // 1) Check fileName doesn’t exist
    status = db.openFile(fileName, file);
    if (status != OK)
    {
		// file doesn't exist. First create it and allocate
		// an empty header page and data page.
        
        // 2) Create file db.createFile(fileName) and open
		status = db.createFile(fileName);
        if (status != OK){
            return status;
        }
        status = db.openFile(fileName, file);
        if (status != OK){
            return status;
        }

        // 3) Allocate a header page
		status = bufMgr->allocPage(file,hdrPageNo,newPage);
        if (status != OK){
            return status;
        }
        FileHdrPage* hdrPage = (FileHdrPage *) newPage;

        // 4) Allocate an empty, data page
        status = bufMgr->allocPage(file,newPageNo,newPage);
        if (status != OK){
            return status;
        } 
        newPage->init(newPageNo);

        // set header values
        strcpy(hdrPage->fileName, fileName.c_str());
        hdrPage->firstPage = newPageNo;
        hdrPage->lastPage = newPageNo;
        hdrPage->recCnt = 0;
        hdrPage->pageCnt = 1;

        // 5) Unpin, set dirty bit
        status = bufMgr->unPinPage(file,hdrPageNo,true);
        if (status != OK){
            return status;
        } 
        status = bufMgr->unPinPage(file,newPageNo,true);
        if (status != OK){
            return status;
        } 

        // 6) Flush and close file
        // I believe closeFile flushes it
        status = db.closeFile(file);
        return status;
    }
    return (FILEEXISTS);
}

// routine to destroy a heapfile
const Status destroyHeapFile(const string fileName)
{
	return (db.destroyFile (fileName));
}

// Constructor opens the file with the given fileName
// creates a heapFile by extracting the header and first page
// from the File and initalizing the heapFile values.
// returns error if the fileName does not exist
// Parameters:
//   fileName - the name of the File to be openned
// Return:
//   returnStatus the status of the execution
HeapFile::HeapFile(const string & fileName, Status& returnStatus)
{
    Status 	status;
    Page*	pagePtr;

    // open the file and read in the header page and the first data page
    // 1) check if fileName exists and open
    if ((status = db.openFile(fileName, filePtr)) == OK)
    {
        // 2) assign header-page
        status = filePtr->getFirstPage(headerPageNo);
        if (status != OK){
            returnStatus = status;
            return;
        }
        // 2.1) pins the header page for the file in the buffer pool
        status = bufMgr->readPage(filePtr,headerPageNo,pagePtr);
        if (status != OK){
            returnStatus = status;
            return;
        }
          // 2.2) initializing the private data members headerPage, headerPageNo, and hdrDirtyFlag
        headerPage = (FileHdrPage *) pagePtr;
        hdrDirtyFlag = false;

        // 3) read first page into curPage and curPageNo
        curPageNo = headerPage->firstPage;
        // 3.1) pin the first page of the file into the buffer pool
        status = bufMgr->readPage(filePtr,curPageNo,curPage);
        if (status != OK){
            returnStatus = status;
            return;
        }
        curDirtyFlag = false;
        // 3.2) initializing the values of curPage, curPageNo, and curDirtyFlag appropriately
		
        // 4) curRec = NUllRID;
        curRec = NULLRID;
        returnStatus = status;
    }
    else
    {
    	cerr << "open of heap file failed\n";
		returnStatus = status;
		return;
    }
}

// the destructor closes the file
HeapFile::~HeapFile()
{
    Status status;

    // see if there is a pinned data page. If so, unpin it 
    if (curPage != NULL)
    {
    	status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
		curPage = NULL;
		curPageNo = 0;
		curDirtyFlag = false;
		if (status != OK) cerr << "error in unpin of date page\n";
    }
	
	 // unpin the header page
    status = bufMgr->unPinPage(filePtr, headerPageNo, hdrDirtyFlag);
    if (status != OK) cerr << "error in unpin of header page\n";
	
	// status = bufMgr->flushFile(filePtr);  // make sure all pages of the file are flushed to disk
	// if (status != OK) cerr << "error in flushFile call\n";
	// before close the file
	status = db.closeFile(filePtr);
    if (status != OK)
    {
		cerr << "error in closefile call\n";
		Error e;
		e.print (status);
    }
}

// Return number of records in heap file

const int HeapFile::getRecCnt() const
{
  return headerPage->recCnt;
}

// retrieve an arbitrary record from a file.
// if record is not on the currently pinned page, the current page
// is unpinned and the required page is read into the buffer pool
// and pinned.  returns a pointer to the record via the rec parameter
//
// Parameters:
//   fileName - the name of the File to be created
// Return:
//   rec - pointer to the record
//   status - status of the execution
const Status HeapFile::getRecord(const RID & rid, Record & rec)
{
    Status status;

    // 0) If curPage is NULL, read in the right page
    if (curPage == NULL) {
        curPageNo = rid.pageNo;
        status = bufMgr->readPage(filePtr,curPageNo,curPage);
        if (status != OK) {
                return status;
        }
        curDirtyFlag = false;
        curRec = rid;
    }
   
    // 1) check if on correct page
    if (curPageNo == rid.pageNo){
        // 1.1) call getRecord on currentPage
        status = curPage->getRecord(rid,rec);
        if (status != OK) {
            return status;
        }
        // 1.2) update HeapFile Object
        curRec = rid;
        // 1.3) return status
        return OK;
    }

    // 2) unpin current page
    status = bufMgr->unPinPage(filePtr,curPageNo,curDirtyFlag);
    if (status != OK && status != PAGENOTPINNED){
        return status;
    }

    // 3) read page using curPageNo
    curPageNo = rid.pageNo;
    status = bufMgr->readPage(filePtr,curPageNo,curPage);
    // 4) update heapfile: curPage, curDirtyFlag, curRec
    if (status != OK) {
            return status;
    }
    curDirtyFlag = false;

   // 4.1) call getRecord.
    status = curPage->getRecord(rid,rec);
    if (status != OK) {
        return status;
    }
    curRec = rid;
    return OK;
}

HeapFileScan::HeapFileScan(const string & name,
			   Status & status) : HeapFile(name, status)
{
    filter = NULL;
}

const Status HeapFileScan::startScan(const int offset_,
				     const int length_,
				     const Datatype type_, 
				     const char* filter_,
				     const Operator op_)
{
    if (!filter_) {                        // no filtering requested
        filter = NULL;
        return OK;
    }
    
    if ((offset_ < 0 || length_ < 1) ||
        (type_ != STRING && type_ != INTEGER && type_ != FLOAT) ||
        (type_ == INTEGER && length_ != sizeof(int)
         || type_ == FLOAT && length_ != sizeof(float)) ||
        (op_ != LT && op_ != LTE && op_ != EQ && op_ != GTE && op_ != GT && op_ != NE))
    {
        return BADSCANPARM;
    }

    offset = offset_;
    length = length_;
    type = type_;
    filter = filter_;
    op = op_;

    return OK;
}


const Status HeapFileScan::endScan()
{
    Status status;
    // generally must unpin last page of the scan
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        curPage = NULL;
        curPageNo = 0;
		curDirtyFlag = false;
        return status;
    }
    return OK;
}

HeapFileScan::~HeapFileScan()
{
    endScan();
}

const Status HeapFileScan::markScan()
{
    // make a snapshot of the state of the scan
    markedPageNo = curPageNo;
    markedRec = curRec;
    return OK;
}

const Status HeapFileScan::resetScan()
{
    Status status;
    if (markedPageNo != curPageNo) 
    {
		if (curPage != NULL)
		{
			status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
			if (status != OK) return status;
		}
		// restore curPageNo and curRec values
		curPageNo = markedPageNo;
		curRec = markedRec;
		// then read the page
		status = bufMgr->readPage(filePtr, curPageNo, curPage);
		if (status != OK) return status;
		curDirtyFlag = false; // it will be clean
    }
    else curRec = markedRec;
    return OK;
}

// Finds the RID of the next record that satisfies the scan predicate
// Parameters:
//   None
// Return:
//   outRid - the RID of the next record that satisfies the scan predicate
//   status - status of the execution
const Status HeapFileScan::scanNext(RID& outRid)
{
    Status 	status = OK;
    RID		nextRid;
    RID		tmpRid;
    int 	nextPageNo;
    Record      rec;

    //check if currentPage is invalid
    if (curPage == NULL){
        curPageNo = headerPage->firstPage;
        // currentPage is invalid
        // start from first page
        status = bufMgr->readPage(filePtr,curPageNo,curPage);
        if (status != OK) return status;
        // get first record with curPage->firstRecord
        status = curPage->firstRecord(curRec);
        if (status != OK) return status;
    }
    tmpRid = curRec;

    // 1) check if page is valid
    while (true) { 
        //1.1 iterate over all records on page

        while(true){  // REVISIT
            // get next record using nextRecord
            status = curPage->nextRecord(tmpRid,nextRid);
            if (status == ENDOFPAGE){
                break;
            }
            if (status != OK){
                return status;
            }
            tmpRid = nextRid;

            // 1.2) check if record matchs record we are searching for
            // Convert the rid to a pointer to the record data
            curRec = tmpRid;
            status = getRecord(rec);
            if (status != OK) {
                return status;
            }
            if (matchRec(rec)){
                outRid = curRec;
                return OK;
            }
        }
        // 2.1 Check to see if we've reached the end of the file.
        if (curPageNo == headerPage->lastPage){
            return FILEEOF;
        }
        // 2.2 unpin page
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        if (status != OK){
            return status;
        } 
        // use nextPage to get new page
        status = curPage->getNextPage(curPageNo);
        if (status != OK){
            return status;
        } 
        status = bufMgr->readPage(filePtr,curPageNo,curPage);
        if (status != OK) return status;

        tmpRid = NULLRID;
    }
}


// returns pointer to the current record.  page is left pinned
// and the scan logic is required to unpin the page 

const Status HeapFileScan::getRecord(Record & rec)
{
    return curPage->getRecord(curRec, rec);
}

// delete record from file. 
const Status HeapFileScan::deleteRecord()
{
    Status status;

    // delete the "current" record from the page
    status = curPage->deleteRecord(curRec);
    curDirtyFlag = true;

    // reduce count of number of records in the file
    headerPage->recCnt--;
    hdrDirtyFlag = true; 
    return status;
}


// mark current page of scan dirty
const Status HeapFileScan::markDirty()
{
    curDirtyFlag = true;
    return OK;
}

const bool HeapFileScan::matchRec(const Record & rec) const
{
    // no filtering requested
    if (!filter) return true;

    // see if offset + length is beyond end of record
    // maybe this should be an error???
    if ((offset + length -1 ) >= rec.length)
	return false;

    float diff = 0;                       // < 0 if attr < fltr
    switch(type) {

    case INTEGER:
        int iattr, ifltr;                 // word-alignment problem possible
        memcpy(&iattr,
               (char *)rec.data + offset,
               length);
        memcpy(&ifltr,
               filter,
               length);
        diff = iattr - ifltr;
        break;

    case FLOAT:
        float fattr, ffltr;               // word-alignment problem possible
        memcpy(&fattr,
               (char *)rec.data + offset,
               length);
        memcpy(&ffltr,
               filter,
               length);
        diff = fattr - ffltr;
        break;

    case STRING:
        diff = strncmp((char *)rec.data + offset,
                       filter,
                       length);
        break;
    }

    switch(op) {
    case LT:  if (diff < 0.0) return true; break;
    case LTE: if (diff <= 0.0) return true; break;
    case EQ:  if (diff == 0.0) return true; break;
    case GTE: if (diff >= 0.0) return true; break;
    case GT:  if (diff > 0.0) return true; break;
    case NE:  if (diff != 0.0) return true; break;
    }

    return false;
}

InsertFileScan::InsertFileScan(const string & name,
                               Status & status) : HeapFile(name, status)
{
  //Do nothing. Heapfile constructor will bread the header page and the first
  // data page of the file into the buffer pool
}

InsertFileScan::~InsertFileScan()
{
    Status status;
    // unpin last page of the scan
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, true);
        curPage = NULL;
        curPageNo = 0;
        if (status != OK) cerr << "error in unpin of data page\n";
    }
}

// Inserts the record described by rec into the file 
// returning the RID of the inserted record in outRid
// method finds the last page of the file, adds record if space
// otherwise allocates new page and adds records to new page, updating the
// header
// Parameters:
//   rec - the record to be inserted
// Return:
//   outRid - the RID of where the record was inserted into the file
//   status - status of the execution
const Status InsertFileScan::insertRecord(const Record & rec, RID& outRid)
{
    Page*	newPage;
    int		newPageNo;
    Status	status, unpinstatus;
    RID		rid;

    // check for very large records
    if ((unsigned int) rec.length > PAGESIZE-DPFIXED)
    {
        // will never fit on a page, so don't even bother looking
        return INVALIDRECLEN;
    }

    if (curPage == NULL){
        bufMgr->readPage(filePtr,headerPage->lastPage,curPage);
        curPageNo = headerPage->lastPage;
        curDirtyFlag = false;

    }
    if (curPage != NULL){
        status = curPage->insertRecord(rec, rid);
        if (status == OK){
            curRec = rid;
            outRid = rid;
            headerPage->recCnt++;
            hdrDirtyFlag = true;
            curDirtyFlag = true;
            return status;
        }
        else if (status == NOSPACE){
            status = bufMgr->unPinPage(filePtr,curPageNo,curDirtyFlag);
            if (status != OK){
                return status;
            }
            status = bufMgr->allocPage(filePtr,newPageNo,newPage);
            if (status != OK){
                return status;
            }
            newPage->init(newPageNo);
            curPage->setNextPage(newPageNo);
            curPage = newPage;
            curPageNo = newPageNo;
            status = curPage->insertRecord(rec,rid);
            if (status == OK){
                curRec = rid;
                outRid = rid;
                headerPage->recCnt++;
                hdrDirtyFlag = true;
                curDirtyFlag = true;
                headerPage->pageCnt++;
                headerPage->lastPage = curPageNo;
                return status;
            }
        }
        else {
            return status;
        }
    }
}


