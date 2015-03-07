#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "../include/minirel.h"
#include "../include/heapfile.h"
#include "../include/scan.h"
#include "../include/join.h"
#include "../include/btfile.h"
#include "../include/btfilescan.h"
#include "../include/relation.h"
#include "../include/bufmgr.h"



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


void IndexNestedLoopJoin(JoinSpec specOfR, JoinSpec specOfS, 
	long& pinRequests, long& pinMisses, double& duration)
{
	// Reset stat of buffer manager
	MINIBASE_BM->ResetStat();

	// Create a timer
	clock_t start = clock();

	Status status = OK;

	// Create new relation for joined result.
	HeapFile *joinedRelation = new HeapFile(NULL, status); 
	if (status != OK) cerr << "Cannot create new file for joined relation\n";

	/////////////////////////////////////////////////
	//Build B+ Tree for inner relation //////////////
	/////////////////////////////////////////////////
	Scan *scanOnS = specOfS.file->OpenScan(status);
	if (status != OK) cerr << "ERROR : cannot open scan on the heapfile S.\n";
	BTreeFile *btree = new BTreeFile( status, "BTreeS", ATTR_INT, sizeof(int));

	char *recS = new char[specOfS.recLen];
	RecordID ridS;
	// Insert records in S to b+ tree
	while( scanOnS->GetNext(ridS, recS, specOfS.recLen))
	{
		btree->Insert(recS + specOfS.offset, ridS);
	}

	// clean up
	delete scanOnS;
	delete[] recS;

	/////////////////////////////////////////////////
	//Join R to S////////////////////////////////////
	/////////////////////////////////////////////////

	RecordID ridR, ridJoined;
	char *recPtrR = new char[specOfR.recLen];
	char *recPtrS = new char[specOfS.recLen];
	char *recPtrRandS = new char[specOfR.recLen + specOfS.recLen];

	// Initialise scan on relation R.
	Scan *scanOnR = specOfR.file->OpenScan(status);
	if (status != OK) cerr << "ERROR : cannot open scan on the heapfile R.\n";

	// for each record in R
	while( scanOnR->GetNext(ridR, recPtrR, specOfR.recLen))
	{
		BTreeFileScan  *btreeScan = (BTreeFileScan *)btree->OpenScan(NULL, NULL);
		
		// for each entry in b+ tree
		int key;
		while( btreeScan->GetNext(ridS, &key) == OK)
		{
			// get record from relation S
			specOfS.file->GetRecord(ridS, recPtrS, specOfS.recLen);

			// match
			int* attrToJoinOnS = (int*)&recPtrS[specOfS.offset];
			int* attrToJoinOnR = (int*)&recPtrR[specOfR.offset];
			if( *attrToJoinOnR == *attrToJoinOnS)
			{
				// join two records and jopined record is in recPtrRandS
				MakeNewRecord( recPtrRandS, recPtrR, recPtrS, specOfR.recLen, specOfS.recLen);	
				joinedRelation -> InsertRecord( recPtrRandS, specOfR.recLen + specOfS.recLen, ridJoined);
			}	
		}

		delete btreeScan;
	}


	// clean up
	delete btree;
	delete scanOnR;
	delete[] recPtrS, recPtrR, recPtrRandS;
	delete joinedRelation;

	// get stat;
	MINIBASE_BM->GetStat(pinRequests, pinMisses);
	duration = ( clock() - start) / (double) CLOCKS_PER_SEC;
}