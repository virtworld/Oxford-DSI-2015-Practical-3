
#ifndef _BUF_H
#define _BUF_H

#include "db.h"
#include "page.h"
#include "frame.h"
#include "replacer.h"
#include "hash.h"

class BufMgr 
{
	private:

		HashTable *hashTable;
		Frame **frames;
		Replacer *replacer;
		int   numOfBuf;

		int FindFrame( PageID pid );
		long totalCall;
		long totalHit;
		long numDirtyPageWrites;

	public:

		BufMgr( int bufsize );
		~BufMgr();      
		Status PinPage( PageID pid, Page*& page, Bool emptyPage=FALSE );
		Status UnpinPage( PageID pid, Bool dirty=FALSE );
		Status NewPage( PageID& pid, Page*& firstpage,int howmany=1 ); 
		Status FreePage( PageID pid ); 
		Status FlushPage( PageID pid );
		Status FlushAllPages();
		Status  GetStat(long& pinNo, long& missNo) { pinNo = totalCall; missNo = totalCall-totalHit; return OK;}

		unsigned int GetNumOfUnpinnedFrames();

		unsigned int GetNumOfBuffers();
		unsigned int GetNumOfUnpinnedBuffers();

		void   PrintStat();
		void   ResetStat() { totalHit = 0; totalCall = 0; numDirtyPageWrites = 0;}
};


#endif // _BUF_H
