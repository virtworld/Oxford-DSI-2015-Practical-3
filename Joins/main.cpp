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

int MINIBASE_RESTART_FLAG = 0;// used in minibase part

#define NUM_OF_DB_PAGES  2000 // define # of DB pages
#define NUM_OF_BUF_PAGES 50 // define Buf manager size.You will need to change this for the analysis

using namespace std;

int main()
{

	long count = 0;
	for(int numOfBuf = NUM_OF_BUF_PAGES; numOfBuf <= NUM_OF_DB_PAGES; numOfBuf += 50)
	{
		for(int numOfRecR = 2; numOfRecR < 8192; numOfRecR *= 2)
		{
			for(int numOfRecS = 2; numOfRecS < 2048; numOfRecS *= 2)
			{
				
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

				long pinNo, pinMisses;
				double duration;

				cout << ">> Number of buffer pages: " << numOfBuf << ", Number of record in R: ";
				cout << numOfRecR << ", Number of record in S: " << numOfRecS << endl;

				TupleNestedLoopJoin(specOfR, specOfS, pinNo, pinMisses, duration);
				// TODO: other two algoritrhms here
				
				cout << "TupleNestedLoopJoin: ";
				cout << "Duration " << duration;
				cout << ", pinNo " << pinNo;
				cout << ", pinMisses " << pinMisses << endl;

				delete minibase_globals;
			}
	 	}
	}

	return 1;
}
