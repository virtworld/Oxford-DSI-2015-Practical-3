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
#define NUM_OF_BUF_PAGES_ORIGINAL 50 // origional settings 
#define NUM_OF_REC_IN_R_ORIGINAL 10000 // origional settings 
#define NUM_OF_REC_IN_S_ORIGINAL 2500 // origional settings 

#define NUM_OF_BUF_MAX_PAGES 1024 // for the experiment on different buffer size
#define NUM_OF_MAX_REC_R 10000 // for the experiment on different R size 
#define NUM_OF_MAX_REC_S 2500 // for the experiment on different S size
#define NUM_OF_REPETITION_TASK1 10
#define NUM_OF_REPETITION_TASK2 10
#define NUM_OF_REPETITION_TASK3 10

using namespace std;

void perfCompTask1();
void perfCompTask2();
void perfCompTask3();
void callJoins( int numOfBuf, int numOfRecR, int numOfRecS, 
	long pinNo[3], long pinMisses[3], double duration[3] );
static inline void loadbar(unsigned int x, unsigned int n, unsigned int w = 50);

int main()
{
	perfCompTask1();
	perfCompTask2();
	perfCompTask3();
	return 1;
}

//--------------------------------------------------
// Performance comparison 1
// Using origional settings, repeat NUM_OF_REPETITION_TASK1 times, 
// calculate average pin number, pin misses and duration
//-------------------------------------------------- 
void perfCompTask1()
{
	int count = 0;
	double *avgPinNo = new double[3];
	double *avgPinMisses = new double[3];
	double *avgDuration = new double[3];

	cout<< ">>----------Test 1: Origional settings----------"<<endl;
	cout<< ">>Running " << NUM_OF_REPETITION_TASK1 << " joins..." << endl;
	cout << "Settings   | # Buffer pages: " << NUM_OF_BUF_PAGES_ORIGINAL << " | # Records in R: ";
	cout << NUM_OF_REC_IN_R_ORIGINAL << " | # Records in S: " << NUM_OF_REC_IN_S_ORIGINAL << endl;

	for(int numOfRepetition = 0; numOfRepetition < NUM_OF_REPETITION_TASK1; numOfRepetition++)
	{
		// display progress bar
		int percentage = (int) ((count / (float) NUM_OF_REPETITION_TASK1) * 100);
		loadbar(percentage, 100);
		if(percentage == 100) cout << endl;

		// do joins
		long *pinNo = new long[3];
		long *pinMisses = new long[3];
		double *duration = new double[3];

		callJoins( NUM_OF_BUF_PAGES_ORIGINAL, NUM_OF_REC_IN_R_ORIGINAL, NUM_OF_REC_IN_S_ORIGINAL, 
			pinNo, pinMisses, duration);

		for(int i = 0; i < 3; i++)
		{
			avgPinNo[i] = (avgPinNo[i] * numOfRepetition + pinNo[i]) / (numOfRepetition + 1);
			avgPinMisses[i] = (avgPinMisses[i] * numOfRepetition + pinMisses[i]) / (numOfRepetition + 1);
			avgDuration[i] = (avgDuration[i] * numOfRepetition + duration[i]) / (numOfRepetition + 1);
		}

		delete[] pinNo, pinMisses, duration;
		count++;
	}

	// print statistics
	cout << fixed << "Tuple Join | # Avg Pin: " << avgPinNo[0] << " | # Avg Misses: " << 
	        avgPinMisses[0] << " | Avg Duration: " << avgDuration[0] << endl;
	cout << fixed << "Block Join | # Avg Pin: " << avgPinNo[1] << " | # Avg Misses: " << 
	        avgPinMisses[1] << " | Avg Duration: " << avgDuration[1] << endl;
	cout << fixed << "Index Join | # Avg Pin: " << avgPinNo[2] << " | # Avg Misses: " << 
	        avgPinMisses[2] << " | Avg Duration: " << avgDuration[2] << endl;

	delete[] avgPinNo, avgPinMisses, avgDuration;
	 
	cout <<endl<< ">>----------End of Test 1----------" << endl << endl;
}

//--------------------------------------------------
// Performance comparison 2
// Using different size of buffer, repeat NUM_OF_REPETITION_TASK2 times, 
// calculate average pin number, pin misses and duration
//-------------------------------------------------- 
void perfCompTask2()
{
	int count = 0;
	
	cout<< ">>----------Test 2 : Variant buffer size----------"<<endl;
	cout<< ">>Running " << NUM_OF_REPETITION_TASK2 << " joins for each buffer size..." << endl;

	for(int numOfBuf = 16; numOfBuf <= NUM_OF_BUF_MAX_PAGES; numOfBuf *= 4)
	{
		double *avgPinNo = new double[3];
		double *avgPinMisses = new double[3];
		double *avgDuration = new double[3];
		cout << "Settings   | # Buffer pages: " << numOfBuf << " | # Records in R: ";
		cout << NUM_OF_REC_IN_R_ORIGINAL << " | # Records in S: " << NUM_OF_REC_IN_S_ORIGINAL << endl;

		for(int numOfRepetition = 0; numOfRepetition < NUM_OF_REPETITION_TASK2; numOfRepetition++)
		{
			// display progress bar
			
			int percentage = (int) ((count / (float) (NUM_OF_REPETITION_TASK2 * 4)) * 100);
			loadbar(percentage, 100);
			if(percentage == 100) cout << endl;

			// do joins
			long *pinNo = new long[3];
			long *pinMisses = new long[3];
			double *duration = new double[3];

			callJoins( numOfBuf, NUM_OF_REC_IN_R_ORIGINAL, NUM_OF_REC_IN_S_ORIGINAL, 
				pinNo, pinMisses, duration);

			for(int i = 0; i < 3; i++)
			{
				avgPinNo[i] = (avgPinNo[i] * numOfRepetition + pinNo[i]) / (numOfRepetition + 1);
				avgPinMisses[i] = (avgPinMisses[i] * numOfRepetition + pinMisses[i]) / (numOfRepetition + 1);
				avgDuration[i] = (avgDuration[i] * numOfRepetition + duration[i]) / (numOfRepetition + 1);
			}

			delete[] pinNo, pinMisses, duration;
			count++;
		}

		// print statistics
		cout << fixed << "Tuple Join | # Avg Pin: " << avgPinNo[0] << " | # Avg Misses: " << 
		        avgPinMisses[0] << " | Avg Duration: " << avgDuration[0] << endl;
		cout << fixed << "Block Join | # Avg Pin: " << avgPinNo[1] << " | # Avg Misses: " << 
		        avgPinMisses[1] << " | Avg Duration: " << avgDuration[1] << endl;
		cout << fixed << "Index Join | # Avg Pin: " << avgPinNo[2] << " | # Avg Misses: " << 
		        avgPinMisses[2] << " | Avg Duration: " << avgDuration[2] << endl;

		delete[] avgPinNo, avgPinMisses, avgDuration;
		cout<< endl;
	}
	cout << ">>----------End of Test 2----------" << endl << endl;
}

//--------------------------------------------------
// Performance comparison 3
// Using relation size, repeat NUM_OF_REPETITION_TASK1 times, 
// calculate average pin number, pin misses and duration
//-------------------------------------------------- 
void perfCompTask3()
{
	int count = 0;
	
	cout<< ">>----------Test 3 : Variant R & S size----------"<<endl;
	cout<< ">>Running " << NUM_OF_REPETITION_TASK3 << " joins for each size combination..." << endl;

	for(int numOfRecR = 100; numOfRecR <= NUM_OF_MAX_REC_R; numOfRecR *= 10)
	{
		for(int numOfRecS = 25; numOfRecS <= NUM_OF_MAX_REC_S; numOfRecS *= 10)
		{
			double *avgPinNo = new double[3];
			double *avgPinMisses = new double[3];
			double *avgDuration = new double[3];
			
			cout << "Settings   | # Buffer pages: " << NUM_OF_BUF_PAGES_ORIGINAL << " | # Records in R: ";
			cout << numOfRecR << " | # Records in S: " << numOfRecS << endl;

			for(int numOfRepetition = 0; numOfRepetition < NUM_OF_REPETITION_TASK3; numOfRepetition++)
			{
					// display progress bar

				int percentage = (int) ((count / (float) (NUM_OF_REPETITION_TASK3 * 3 * 3)) * 100);
				loadbar(percentage, 100);
				if(percentage == 100) cout << endl;

					// do joins
				long *pinNo = new long[3];
				long *pinMisses = new long[3];
				double *duration = new double[3];

				callJoins( NUM_OF_BUF_PAGES_ORIGINAL, numOfRecR, numOfRecS, 
					pinNo, pinMisses, duration);

				for(int i = 0; i < 3; i++)
				{
					avgPinNo[i] = (avgPinNo[i] * numOfRepetition + pinNo[i]) / (numOfRepetition + 1);
					avgPinMisses[i] = (avgPinMisses[i] * numOfRepetition + pinMisses[i]) / (numOfRepetition + 1);
					avgDuration[i] = (avgDuration[i] * numOfRepetition + duration[i]) / (numOfRepetition + 1);
				}

				delete[] pinNo, pinMisses, duration;
				count++;
			}

			// print statistics
			cout << fixed << "Tuple Join | # Avg Pin: " << avgPinNo[0] << " | # Avg Misses: " << 
			avgPinMisses[0] << " | Avg Duration: " << avgDuration[0] << endl;
			cout << fixed << "Block Join | # Avg Pin: " << avgPinNo[1] << " | # Avg Misses: " << 
			avgPinMisses[1] << " | Avg Duration: " << avgDuration[1] << endl;
			cout << fixed << "Index Join | # Avg Pin: " << avgPinNo[2] << " | # Avg Misses: " << 
			avgPinMisses[2] << " | Avg Duration: " << avgDuration[2] << endl;

			delete[] avgPinNo, avgPinMisses, avgDuration;
			cout<< endl;
		}
	}

	cout << ">>----------End of Test 3----------" << endl << endl;
}

void callJoins( int numOfBuf, int numOfRecR, int numOfRecS, 
	long pinNo[3], long pinMisses[3], double duration[3] )
{
	remove( "MINIBASE.DB" ); 		
	Status s;

	// Create a database manager
	minibase_globals = new SystemDefs(s, 
		"MINIBASE.DB",
		"MINIBASE.LOG",
		NUM_OF_DB_PAGES,   // Number of pages allocated for database
		500,
		numOfBuf,  // Number of frames in buffer pool
		NULL);

	srand(1);

	// Create relation R and S
	CreateR(numOfRecR, numOfRecS);
	CreateS(numOfRecR, numOfRecS);

	JoinSpec specOfS, specOfR;
	CreateSpecForR(specOfR);
	CreateSpecForS(specOfS);

	int blocksize = (MINIBASE_BM->GetNumOfUnpinnedBuffers()-3*3)*MINIBASE_PAGESIZE;

	TupleNestedLoopJoin(specOfR, specOfS, pinNo[0], pinMisses[0], duration[0]);
	BlockNestedLoopJoin(specOfR, specOfS, blocksize, pinNo[1], pinMisses[1], duration[1]);
	IndexNestedLoopJoin(specOfR, specOfS, pinNo[2], pinMisses[2], duration[2]);

	delete minibase_globals;
}

/**
  * Loadbar display function
  * Code from: 
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