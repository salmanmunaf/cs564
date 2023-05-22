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

// inserts the rows in the relation
//
// Parameters:
//   relation = name of relational table
//   attrCnt = number of attributes
//   attrList = list of all the attributes to be inserted and its value
// Return:
//   status = status of the execution
const Status QU_Insert(const string & relation, 
	const int attrCnt, 
	const attrInfo attrList[])
{
	Status status;
	int cnt;
	AttrDesc *attrDescArray;
	if ((status = attrCat->getRelInfo(relation, cnt, attrDescArray)) != OK) {
        return status;
    }

	int reclen = 0;
    for (int i = 0; i < cnt; i++)
    {
        reclen += attrDescArray[i].attrLen;
    }

	char outputData[reclen];
    Record outputRec;
    outputRec.data = (void *) outputData;
    outputRec.length = reclen;

	int outputOffset = 0;
	for (int i = 0; i < cnt; ++i) {
		bool found = false;
        for (int j = 0; j < attrCnt; ++j) {
				// check if attibute belongs to projection
			if (strcmp(attrDescArray[i].attrName, attrList[j].attrName) == 0) { // at correct attribute
				void *ptr;
				if (attrList[j].attrType == 0) {
					ptr = attrList[j].attrValue;
				}
				else if (attrList[j].attrType  == 1) {
					int value = atoi((char*)attrList[j].attrValue);
					ptr = &value;
				}
				else {
					float value = atof((char*)attrList[j].attrValue);
					ptr = &value;
				}
				memcpy(outputData + outputOffset, (char *)ptr, attrDescArray[i].attrLen);
				outputOffset += attrDescArray[i].attrLen;
				found = true;
				break;
			}
		}
		if (found == false){
			return ATTRNOTFOUND;
		}
	}
	InsertFileScan resultRel(relation, status);
	ASSERT(status == OK);
	RID outRID;
	status = resultRel.insertRecord(outputRec, outRID);
	ASSERT(status == OK);
	// part 6
	return OK;

}

