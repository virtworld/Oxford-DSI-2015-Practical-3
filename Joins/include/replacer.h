#include "frame.h"
#include "hash.h"

class Replacer 
{
	public :

		Replacer();
		~Replacer();

		virtual int PickVictim() = 0;
};

class Clock : public Replacer
{
	private :
		
		int current;
		int numOfBuf;
		Frame **frames;
		HashTable *hashTable;

	public :
		
		Clock( int bufSize, Frame **frames, HashTable *hashTable );
		~Clock();
		int PickVictim();
};
