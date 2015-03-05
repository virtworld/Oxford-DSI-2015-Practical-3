#include <stdlib.h>
#include <time.h>
#include <ctype.h>

#include "include/minirel.h"
#include "include/bufmgr.h"
#include "include/heapfile.h"
#include "include/join.h"
#include "include/relation.h"

int MINIBASE_RESTART_FLAG = 0;// used in minibase part

#define NUM_OF_DB_PAGES  2000 // define # of DB pages
#define NUM_OF_BUF_PAGES 50 // define Buf manager size.You will need to change this for the analysis

int main()
{
	Status s;


	//
	// Initialize Minibase's Global Variables
	//

	minibase_globals = new SystemDefs(s, 
		"MINIBASE.DB",
		"MINIBASE.LOG",
		NUM_OF_DB_PAGES,   // Number of pages allocated for database
		500,
		NUM_OF_BUF_PAGES,  // Number of frames in buffer pool
		NULL);
	
	//
	// Initialize random seed
	//

	srand(1);

	//
	// Create Random Relations R(outer relation) and S for joining. The definition is in relation.h, 
	// # of tuples: NUM_OF_REC_IN_R, NUM_OF_REC_IN_S in join.h
	//  
	//

	cerr << "Creating random records for relation R\n";
	CreateR();
	cerr << "Creating random records for relation S\n";
	CreateS();

	//
	// Initialize the specification for joins
	//

	JoinSpec specOfS, specOfR;

	CreateSpecForR(specOfR);
	CreateSpecForS(specOfS);

	// 
	// Do your joining here.
	//
	
	//
	// The end.
	//

	return 1;
}
