///////////////////////////////////////////////////////////////////////////////
// Member:      Gabriel Selzer : 9076571836
// Member:      Ana Klabjan : 9081077043
// Member:      Salman Munaf : 9073929474
// ----------------------------------------------------------------------------
// Description: the backend for SQL select, insert and delete.
// Files: select.C, delete.C, insert.C
///////////////////////////////////////////////////////////////////////////////
#include "catalog.h"
#include "query.h"


// removes the rows in the relation that match the filter
//
// Parameters:
//   relation = name of relational table
//   attrName = attribute used for filtering
//   type= type of what you are filtering for
//   op = how you want to filter
//   attrValue = value threshold for filter
// Return:
//   status = status of the execution
const Status QU_Delete(const string & relation, 
		       const string & attrName, 
		       const Operator op,
		       const Datatype type, 
		       const char *attrValue)
{
	// part 6
	Status status;
	HeapFileScan *hfs = new HeapFileScan(relation, status);
	//cout << "created heapfile" << endl;
	if (status != OK) { 
		// cout << "failed to create heapfile" << endl;
		return status; }
	// no condition given
	if (attrName == "") { // no condition given
		Datatype type = STRING;
        hfs->startScan(0, 0, type, NULL, op);
    }
	else { // need to filter scan
		void *newFilter;
		if (type == 0) {
			newFilter = (void*)attrValue;
		}
		else if (type == 1) {
			int tempInt = atoi(attrValue);
			newFilter = &tempInt;
		}
		else {
			float tempFloat = atof(attrValue);
			newFilter = &tempFloat;
		}
		AttrDesc attrDesc;
		//get the attribute description
		status = attrCat->getInfo(relation, attrName, attrDesc);
		if (status != OK) { return status; }
		status = hfs->startScan(attrDesc.attrOffset, attrDesc.attrLen, type, (char*)newFilter, op);
		if (status != OK) { 
			 // cout << "failed to start scan"<< endl;
			return status; }
	}
	//cout << "scan started" << endl;
	RID tempRid;
    while ((status = hfs->scanNext(tempRid)) == OK) {
        // status = hfs->scanNext(tempRid);
		if (status == FILEEOF) { // reach the end and cannot do further delete
			break;
		}
		if (status != OK) return status;
		status = hfs->deleteRecord(); //deletes the current record
		if (status != OK) return status;
		//printf("deleted record");
    }
	//do we need to call endScan()?
    delete hfs;

	return OK;
}


