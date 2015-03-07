#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "../include/minirel.h"
#include "../include/heapfile.h"
#include "../include/scan.h"
#include "../include/join.h"
#include "../include/relation.h"
#include "../include/bufmgr.h"
#include <ctime>

using namespace std;

//---------------------------------------------------------------
// Each join method takes in at least two parameters :
// - specOfS
// - specOfR
//
// They specify which relations we are going to join, which 
// attributes we are going to join on, the offsets of the 
// attributes etc.  specOfS specifies the inner relation while
// specOfR specifies the outer one.
//
//You can use MakeNewRecord() to create the new result record.
//
// Remember to clean up before exiting by "delete"ing any pointers
// that you "new"ed.  This includes any Scan/BTreeFileScan that 
// you have opened.
//---------------------------------------------------------------


void TupleNestedLoopJoin(JoinSpec specOfR, JoinSpec specOfS, 
	long& pinRequests, long& pinMisses, double& duration)
{

	// Reset stat of buffer manager
	MINIBASE_BM->ResetStat();

	// Create a timer
	clock_t start = clock();

	Status status = OK;
	
	// Initialise scan on relation R.
	Scan *scanOnR = specOfR.file->OpenScan(status);
	if (status != OK) cerr << "ERROR : cannot open scan on the heapfile R.\n";
	
	// Create new relation for joined result.
	HeapFile *joinedRelation = new HeapFile(NULL, status); 
	if (status != OK) cerr << "Cannot create new file for joined relation\n";

	// Initialise record ids and record pointers.
	RecordID ridR, ridS, ridJoined;
	char *recPtrR = new char[specOfR.recLen];
	char *recPtrS = new char[specOfS.recLen];
	char *recPtrRandS = new char[specOfR.recLen + specOfS.recLen];
	// GetNext takes length of record (3rd parameter), returns next rid and record
	// in a relation (heapfile). See join.cpp L68-71.
	while ( scanOnR->GetNext( ridR, recPtrR, specOfR.recLen) == OK) 
	{
		// Initialise scan on relation S.
		Scan *scanOnS = specOfS.file->OpenScan(status);
		if (status != OK) cerr << "ERROR : cannot open scan on the heapfile S.\n";
		
		while( scanOnS->GetNext( ridS, recPtrS, specOfS.recLen) == OK)
		{
			// recPtr{S, R} are defined as char*, recPtrS[specOfS.offset] 
			// gets the first byte of the join attribute, then get the address, 
			// and cast to int pointer.
			int* attrToJoinOnS = (int*)&recPtrS[specOfS.offset];
			int* attrToJoinOnR = (int*)&recPtrR[specOfR.offset];
			if( *attrToJoinOnR == *attrToJoinOnS)
			{
				// join two records and jopined record is in recPtrRandS
				MakeNewRecord( recPtrRandS, recPtrR, recPtrS, specOfR.recLen, specOfS.recLen);	
				joinedRelation -> InsertRecord( recPtrRandS, specOfR.recLen + specOfS.recLen, ridJoined);
			}
		}

		delete scanOnS;
	}

	// clean up
	delete scanOnR;
	delete[] recPtrR, recPtrS, recPtrRandS;

	delete joinedRelation;

	// get stat;
	MINIBASE_BM->GetStat(pinRequests, pinMisses);
	duration = ( clock() - start) / (double) CLOCKS_PER_SEC;
}
