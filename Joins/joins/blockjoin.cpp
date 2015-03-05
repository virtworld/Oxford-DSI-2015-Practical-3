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

void BlockNestedLoopJoin(JoinSpec specOfR, JoinSpec specOfS, 
	int blocksize, long& pinRequests, long& pinMisses, double& duration)
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
	char *recBlockPtrR = new char[blocksize];
	char *recPtrS = new char[specOfS.recLen];
	char *recPtrRandS = new char[specOfR.recLen + specOfS.recLen];

	bool done = false;

	// for each block b in R
	while(!done)
	{
		int numOfRecInBlockR = blocksize / specOfR.recLen;
		int numOfRecInRRead = 0;
		// Get the block
		for(int i = 0; i < numOfRecInBlockR; i++)
		{
			if( scanOnR->GetNext( ridR, recBlockPtrR + i*specOfR.recLen, specOfR.recLen) != OK)
			{
				// reach the end of relation R
				done = true;
				break;
			}
			numOfRecInRRead++;
		}

		// Initialise scan on relation R.
		Scan *scanOnS = specOfS.file->OpenScan(status);
		if (status != OK) cerr << "ERROR : cannot open scan on the heapfile S.\n";		

		// for each tuple in S
		while( scanOnS->GetNext( ridS, recPtrS, specOfS.recLen) == OK)
		{
			// for each tuple in r
			for(int i = 0; i < numOfRecInRRead; i++)
			{
				// extract record from record block
				memcpy(recPtrR, recBlockPtrR + i * specOfR.recLen, specOfR.recLen);

				// match r with s
				int* attrToJoinOnS = (int*)&recPtrS[specOfS.offset];
				int* attrToJoinOnR = (int*)&recPtrR[specOfR.offset];
				
				if( *attrToJoinOnR == *attrToJoinOnS)
				{
					// join two records and jopined record is in recPtrRandS
					MakeNewRecord( recPtrRandS, recPtrR, recPtrS, specOfR.recLen, specOfS.recLen);	
					joinedRelation -> InsertRecord( recPtrRandS, specOfR.recLen + specOfS.recLen, ridJoined);
				}
			}
		}

		delete scanOnS;
	}

	delete[] recPtrR, recPtrS, recBlockPtrR, recPtrS;
	delete joinedRelation;
	delete scanOnR;

	// get stat
	MINIBASE_BM->GetStat(pinRequests, pinMisses);
	duration = ( clock() - start) / (double) CLOCKS_PER_SEC;
}
