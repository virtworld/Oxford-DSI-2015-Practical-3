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
#include <cstdio>

int MINIBASE_RESTART_FLAG = 0;// used in minibase part

#define NUM_OF_DB_PAGES  2000 // define # of DB pages
#define NUM_OF_BUF_PAGES_ORIGINAL 50 // origional settings 
#define NUM_OF_REC_IN_R_ORIGINAL 10000 // origional settings 
#define NUM_OF_REC_IN_S_ORIGINAL 2500 // origional settings 

#define NUM_OF_BUF_MAX_PAGES 1024 // for the experiment on different buffer size
#define NUM_OF_MAX_REC_R 10000 // for the experiment on different R size 
#define NUM_OF_MAX_REC_S 10000 // for the experiment on different S size
#define NUM_OF_REPETITION_TASK1 10
#define NUM_OF_REPETITION_TASK2 5
#define NUM_OF_REPETITION_TASK3 2

using namespace std;

void perfCompTask1();
void perfCompTask2();
void perfCompTask3(bool isVarR);

void callJoins( int numOfBuf, int numOfRecR, int numOfRecS, 
	long pinNo[3], long pinMisses[3], double duration[3] );

void printTestTitle(int testNo, bool isStart, const char* nameOfTest);
void printSettings(int buf, int recR, int recS);
void printResults(double avgPinNo[3], double avgPinMisses[3], double avgDuration[3]);

static inline void loadbar(unsigned int x, unsigned int n, unsigned int w = 40);

int main()
{
	remove( "data.txt" ); 
	perfCompTask1(); // orgional settings
	perfCompTask2(); // variable buffer size
	perfCompTask3(true); // variable R
	perfCompTask3(false); // variable S
	printf("%s\n\n", ">> All done! See data.txt for output.");
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

	//printf("%s\n", ">>----------Test 1: Origional settings----------");
	printTestTitle(1, true, "Origional settings");
	printSettings(NUM_OF_BUF_PAGES_ORIGINAL, NUM_OF_REC_IN_R_ORIGINAL, NUM_OF_REC_IN_S_ORIGINAL);

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

	printResults( avgPinNo, avgPinMisses, avgDuration);
	delete[] avgPinNo, avgPinMisses, avgDuration;
	
	printTestTitle(1, false, "Origional settings");
}

//--------------------------------------------------
// Performance comparison 2
// Using different size of buffer, repeat NUM_OF_REPETITION_TASK2 times, 
// calculate average pin number, pin misses and duration
//-------------------------------------------------- 
void perfCompTask2()
{
	int count = 0;
	printTestTitle(2, true, "Variant buffer size");

	for(int numOfBuf = 16; numOfBuf <= NUM_OF_BUF_MAX_PAGES; numOfBuf *= 4)
	{
		printSettings(numOfBuf, NUM_OF_REC_IN_R_ORIGINAL, NUM_OF_REC_IN_S_ORIGINAL);

		double *avgPinNo = new double[3];
		double *avgPinMisses = new double[3];
		double *avgDuration = new double[3];

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

		printResults( avgPinNo, avgPinMisses, avgDuration);
		delete[] avgPinNo, avgPinMisses, avgDuration;
	}

	printTestTitle(2, false, "Variant buffer size");
}

//--------------------------------------------------
// Performance comparison 3
// Using relation size, repeat NUM_OF_REPETITION_TASK1 times, 
// calculate average pin number, pin misses and duration
//-------------------------------------------------- 
void perfCompTask3(bool varyR)
{
	int count = 0;
	
	printTestTitle(3, true, (varyR) ?"Variant R size" : "Variant S size");

	for(int numOfRec = 2; numOfRec <= ((varyR) ? NUM_OF_MAX_REC_R : NUM_OF_MAX_REC_S); numOfRec *= 4)
	{
		printSettings(NUM_OF_BUF_PAGES_ORIGINAL, (varyR) ? numOfRec : NUM_OF_REC_IN_R_ORIGINAL, 
			(varyR) ? NUM_OF_REC_IN_S_ORIGINAL : numOfRec);

		double *avgPinNo = new double[3];
		double *avgPinMisses = new double[3];
		double *avgDuration = new double[3];

		for(int numOfRepetition = 0; numOfRepetition < NUM_OF_REPETITION_TASK3; numOfRepetition++)
		{
				// display progress bar
			int percentage = (int) ((count / (float) (NUM_OF_REPETITION_TASK3 * 8)) * 100);
			loadbar(percentage, 100);
			if(percentage == 100) cout << endl;

				// do joins
			long *pinNo = new long[3];
			long *pinMisses = new long[3];
			double *duration = new double[3];

			callJoins( NUM_OF_BUF_PAGES_ORIGINAL, (varyR) ? numOfRec : NUM_OF_REC_IN_R_ORIGINAL, 
				(varyR) ? NUM_OF_REC_IN_S_ORIGINAL : numOfRec, pinNo, pinMisses, duration);

			for(int i = 0; i < 3; i++)
			{
				avgPinNo[i] = (avgPinNo[i] * numOfRepetition + pinNo[i]) / (numOfRepetition + 1);
				avgPinMisses[i] = (avgPinMisses[i] * numOfRepetition + pinMisses[i]) / (numOfRepetition + 1);
				avgDuration[i] = (avgDuration[i] * numOfRepetition + duration[i]) / (numOfRepetition + 1);
			}

			delete[] pinNo, pinMisses, duration;
			count++;
		}

		printResults( avgPinNo, avgPinMisses, avgDuration);
		delete[] avgPinNo, avgPinMisses, avgDuration;
		cout<< endl;
	}

	printTestTitle(3, false, (varyR) ? "Variant R size" : "Variant S size");
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

void printTestTitle(int testNo, bool isStart, const char* nameOfTest)
{
	FILE *pFile = fopen ("data.txt","a");
	if(isStart)
	{
		printf("%s%d%s%s%s\n", ">>----------Test ", testNo, ": ", nameOfTest, "----------");
		fprintf(pFile, "%s%d%s%s%s\n", ">>----------Test ", testNo, ": ", nameOfTest, "----------");
	}
	else
	{
		printf("%s%d%s%s%s\n\n", ">>----------End of Test ", testNo, ": ", nameOfTest, "----------");
		fprintf(pFile, "%s%d%s%s%s\n\n", ">>----------End of Test ", testNo, ": ", nameOfTest, "----------");
	}

	fclose(pFile);
}

void printSettings(int buf, int recR, int recS)
{
	FILE *pFile = fopen ("data.txt","a");
	printf("%-10s%15s%15s%15s\n", "Settings", "# Buf Pages", "# Rec in R", "# Rec in S"); 
	printf("%25d%15d%15d\n", buf, recR, recS); 
	fprintf(pFile, "%-10s%15s%15s%15s\n", "Settings", "# Buf Pages", "# Rec in R", "# Rec in S");
	fprintf(pFile, "%25d%15d%15d\n", buf, recR, recS); 
	fclose (pFile);
}

void printResults(double avgPinNo[3], double avgPinMisses[3], double avgDuration[3])
{
	FILE *pFile = fopen ("data.txt","a");
	printf("%-10s%15s%15s%15s\n", "Results", "Avg # Pin", "Avg # Misses", "Avg Duration"); 
	printf("%-10s%15.0f%15.0f%15f\n", "Tuple Join", avgPinNo[0], avgPinMisses[0], avgDuration[0]); 
	printf("%-10s%15.0f%15.0f%15f\n", "Block Join", avgPinNo[1], avgPinMisses[1], avgDuration[1]); 
	printf("%-10s%15.0f%15.0f%15f\n\n", "Index Join", avgPinNo[2], avgPinMisses[2], avgDuration[2]); 
	fprintf(pFile, "%-10s%15s%15s%15s\n", "Results", "Avg # Pin", "Avg # Misses", "Avg Duration"); 
	fprintf(pFile, "%-10s%15.0f%15.0f%15f\n", "Tuple Join", avgPinNo[0], avgPinMisses[0], avgDuration[0]); 
	fprintf(pFile, "%-10s%15.0f%15.0f%15f\n", "Block Join", avgPinNo[1], avgPinMisses[1], avgDuration[1]); 
	fprintf(pFile, "%-10s%15.0f%15.0f%15f\n\n", "Index Join", avgPinNo[2], avgPinMisses[2], avgDuration[2]); 
	fclose(pFile);
}

/**
  * Loadbar display function
  * Code from: 
  * https://www.ross.click/2011/02/creating-a-progress-bar-in-c-or-any-other-console-app/
  */
  static inline void loadbar(unsigned int x, unsigned int n, unsigned int w)
  {
  	//if ( (x != n) && (x % (n/100+1) != 0) ) return;
  	float ratio  =  x/(float)n;
  	int   c      =  ratio * w;

  	cout << setw(3) << (int)(ratio*100) << "% [";
  	for (int x=0; x<c; x++) cout << "=";
  	for (int x=c; x<w; x++) cout << " ";
  	cout << "]\r" << flush;
}