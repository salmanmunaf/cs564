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


// forward declaration
const Status ScanSelect(const string & result, 
			const int projCnt, 
			const AttrDesc projNames[],
			const AttrDesc *attrDesc, 
			const Operator op, 
			const char *filter,
			const int reclen);

// returns the rows in the relationship that match the filter
//
// Parameters:
//   projCnt = number of attributes to project
//   projNames = array of attributes to project
//   attr = attribute used for filtering
//   op = how you want to filter
//   attrValue = value threshold for filter
// Return:
//   status = status of the execution
//   result =  results from select 
const Status QU_Select(const string & result, 
		       const int projCnt, 
		       const attrInfo projNames[],
		       const attrInfo *attr, 
		       const Operator op, 
		       const char *attrValue)
{
   // Qu_Select sets up things and then calls ScanSelect to do the actual work
    cout << "Doing QU_Select " << endl;
	Status status;

	AttrDesc projDescs[projCnt];
    for (int i = 0; i < projCnt; i++)
    {
        status = attrCat->getInfo(projNames[i].relName,
                                         projNames[i].attrName,
                                         projDescs[i]);
        if (status != OK)
        {
            return status;
        }
    }

	AttrDesc attrDesc;
	if (attr != NULL){
		status = attrCat->getInfo(attr->relName, attr->attrName, attrDesc);
		if (status != OK)
		{
			return status;
		}
	}

    // get output record length from attrdesc structures
    int reclen = 0;
    for (int i = 0; i < projCnt; i++)
    {
        reclen += projDescs[i].attrLen;
    }

	if (attr == NULL){
		return ScanSelect(result, projCnt, projDescs, &projDescs[0], EQ , NULL, reclen);
	}
	return ScanSelect( result, projCnt, projDescs, &attrDesc, op, attrValue, reclen);

}

// returns the rows in the relationship that match the filter
//
// Parameters:
//   projCnt = number of attributes to project
//   projNames = array of attribute descriptions to project
//   attrDesc = attribute used for filtering
//   op = how you want to filter
//   filter = value threshold for filter
//   reclen = length of a record to output
// Return:
//   status = status of the execution
//   result =  results from select 
const Status ScanSelect(const string & result, 
#include "stdio.h"
#include "stdlib.h"
			const int projCnt, 
			const AttrDesc projNames[],
			const AttrDesc *attrDesc, 
			const Operator op, 
			const char *filter,
			const int reclen)
{
    cout << "Doing HeapFileScan Selection using ScanSelect()" << endl;
	Status status;
    int resultTupCnt = 0;

    // open the result table
    InsertFileScan resultRel(result, status);
    if (status != OK) { return status; }

	// Start scan on table
    HeapFileScan scan(string(attrDesc->relName), status);
    if (status != OK) { return status; }
	if (filter == NULL){ // there is no filter
		Datatype type = STRING;
		status = scan.startScan(attrDesc->attrOffset, attrDesc->attrLen, type, NULL, op);
		
	}
	else { // need to filter scan
		Datatype type;
		void *newFilter;
		if (attrDesc->attrType == 0) {
			type = STRING;
			newFilter = (void*)filter;
		}
		else if (attrDesc->attrType == 1) {
			type = INTEGER;
			int tempInt = atoi(filter);
			newFilter = &tempInt;
		}
		else {
			type = FLOAT;
			float tempFloat = atof(filter);
			newFilter = &tempFloat;
		}
		status = scan.startScan(attrDesc->attrOffset, attrDesc->attrLen, type, (char*)newFilter, op);
	}
    if (status != OK) { return status; }

	char outputData[reclen];
    Record outputRec;
    outputRec.data = (void *) outputData;
    outputRec.length = reclen;

	RID rid;
	while (scan.scanNext(rid) == OK) {
		Record rec;
		status = scan.getRecord(rec);
		ASSERT(status == OK);

		// we have a match, copy data into the output record
		int outputOffset = 0;
		for (int i = 0; i < projCnt; i++)
		{
			memcpy(outputData + outputOffset, (char *)rec.data + projNames[i].attrOffset,
						projNames[i].attrLen);
			outputOffset += projNames[i].attrLen;
		} // end copy attrs

		// add the new record to the output relation
		RID outRID;
		status = resultRel.insertRecord(outputRec, outRID);
		ASSERT(status == OK);
		resultTupCnt++;
    } // end scan
    printf("tuple nested join produced %d result tuples \n", resultTupCnt);
    return OK;
}
