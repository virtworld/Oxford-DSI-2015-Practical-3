#include <stdlib.h>
#include <time.h>
#include <ctype.h>

#include "include/minirel.h"
#include "include/bufmgr.h"
#include "include/heapfile.h"
#include "include/join.h"
#include "include/relation.h"

#include <sys/select.h>
#include <sys/time.h>
#include <unistd.h>
#include <iomanip>
#include <stdio.h>

int MINIBASE_RESTART_FLAG = 0;// used in minibase part

#define NUM_OF_DB_PAGES  2000 // define # of DB pages
#define NUM_OF_BUF_PAGES 50 // define Buf manager size.You will need to change this for the analysis
#define RAW_OUTPUT_ON false

using namespace std;

static inline void loadbar(unsigned int x, unsigned int n, unsigned int w = 50);

int main()
{

	long count = 0;
	for(int numOfBuf = 16; numOfBuf <= 2048; numOfBuf *= 2)
	{
		for(int numOfRecR = 2; numOfRecR <= 1024; numOfRecR *= 2)
		{
			for(int numOfRecS = 2; numOfRecS <= 1024; numOfRecS *= 2)
			{
				if(!RAW_OUTPUT_ON)
				{	
					count++;
					int percentage = (int) ((count / (float) (10 * 10 * 8)) * 100);
					//cout<<percentage<<endl;
					loadbar(percentage, 100);
				}

				Status s;
				
				minibase_globals = new SystemDefs(s, 
					"MINIBASE.DB",
					"MINIBASE.LOG",
					NUM_OF_DB_PAGES,   // Number of pages allocated for database
					500,
					numOfBuf,  // Number of frames in buffer pool
					NULL);
		
				srand(1);

				//cerr << "Creating random records for relation R\n";
				CreateR(numOfRecR, numOfRecS);
				//cerr << "Creating random records for relation S\n";
				CreateS(numOfRecR, numOfRecS);

				JoinSpec specOfS, specOfR;

				CreateSpecForR(specOfR);
				CreateSpecForS(specOfS);

				// 
				// Do your joining here.
				//

				int blocksize = (MINIBASE_BM->GetNumOfUnpinnedBuffers()-3*3)*MINIBASE_PAGESIZE;
				long pinNo, pinMisses;
				double duration;

				if(RAW_OUTPUT_ON)
				{
					cout << ">> Number of buffer pages: " << numOfBuf << ", Number of record in R: ";
					cout << numOfRecR << ", Number of record in S: " << numOfRecS << endl;
				}
				
				TupleNestedLoopJoin(specOfR, specOfS, pinNo, pinMisses, duration);
				
				if(RAW_OUTPUT_ON)
				{
					cout << "TupleNestedLoopJoin: ";
					cout << "Duration " << duration;
					cout << ", pinNo " << pinNo;
					cout << ", pinMisses " << pinMisses << endl;
				}

				BlockNestedLoopJoin(specOfR, specOfS, blocksize, pinNo, pinMisses, duration);

				if(RAW_OUTPUT_ON)
				{
					cout << "BlockNestedLoopJoin: ";
					cout << "Duration " << duration;
					cout << ", pinNo " << pinNo;
					cout << ", pinMisses " << pinMisses << endl;
				}

				delete minibase_globals;
				remove( "MINIBASE.DB" ); 
			}
	 	}
	}

	return 1;
}

/**
  * Loadbar from: 
  * https://www.ross.click/2011/02/creating-a-progress-bar-in-c-or-any-other-console-app/
  */
static inline void loadbar(unsigned int x, unsigned int n, unsigned int w)
{
    if ( (x != n) && (x % (n/100+1) != 0) ) return;
 
    float ratio  =  x/(float)n;
    int   c      =  ratio * w;
 
    cout << setw(3) << (int)(ratio*100) << "% [";
    for (int x=0; x<c; x++) cout << "=";
    for (int x=c; x<w; x++) cout << " ";
    cout << "]\r" << flush;
}