#include <memory.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <fcntl.h>
#include <iostream>
#include <stdio.h>
#include "page.h"
#include "buf.h"

#define ASSERT(c)  { if (!(c)) { \
		       cerr << "At line " << __LINE__ << ":" << endl << "  "; \
                       cerr << "This condition should hold: " #c << endl; \
                       exit(1); \
		     } \
                   }

//  Brock Naidl - bnaidl -- bnaidl@wisc.edu
//	Matej Popovski - popovski -- popovski@wisc.edu


//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(const int bufs)
{
    numBufs = bufs;

    bufTable = new BufDesc[bufs];
    memset(bufTable, 0, bufs * sizeof(BufDesc));
    for (int i = 0; i < bufs; i++) 
    {
        bufTable[i].frameNo = i;
        bufTable[i].valid = false;
    }

    bufPool = new Page[bufs];
    memset(bufPool, 0, bufs * sizeof(Page));

    int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
    hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

    clockHand = bufs - 1;
}


BufMgr::~BufMgr() {

    // flush out all unwritten pages
    for (int i = 0; i < numBufs; i++) 
    {
        BufDesc* tmpbuf = &bufTable[i];
        if (tmpbuf->valid == true && tmpbuf->dirty == true) {

#ifdef DEBUGBUF
            cout << "flushing page " << tmpbuf->pageNo
                 << " from frame " << i << endl;
#endif

            tmpbuf->file->writePage(tmpbuf->pageNo, &(bufPool[i]));
        }
    }

    delete [] bufTable;
    delete [] bufPool;
}

const Status BufMgr::allocBuf(int &frame)
{	
    // tracks total number of frames clock algo has gone through
    int count_of_checked_frames = 0; 

	// max number of iterations of clock algo we can have 2 time around.
	int max_possible_steps = 2 * (int)numBufs;

    while (count_of_checked_frames < max_possible_steps){
        // https://piazza.com/class/lrr3zuxe3cw1kw/post/320 -- Can call this before every iteration
        advanceClock();
		// Ran into some errors this code below was added to debug, help from TA 
		//cout << "Warning Test Buf" << bufTable[clockHand].valid << " | "<< bufTable[clockHand].pinCnt  << "||" << tracker << "|" << max << endl;
        // Checking if there's a valid page
        if (bufTable[clockHand].valid == true){

            // Checks if reference bit is set
            if (bufTable[clockHand].refbit == true){

                // Resets the reference bit and proceeds with the search.
                bufTable[clockHand].refbit = false;
                count_of_checked_frames = count_of_checked_frames + 1;
                continue;
            }
            else{

                if (bufTable[clockHand].pinCnt == 0){

                    if (bufTable[clockHand].dirty){

                        Status status_of_disc_operation;
                        // Verifies the status of the disk write operation. Not flushfile function but similar code
                        status_of_disc_operation = bufTable[clockHand].file->writePage(bufTable[clockHand].pageNo, &bufPool[clockHand]);
                        // Indicates failure in writing to disk.
                        if (status_of_disc_operation != OK){

                            return UNIXERR;
                        }

                    }
					// If not dirty just use and remove from hashtable/buftable
                    frame = clockHand;

                    hashTable->remove(bufTable[clockHand].file, bufTable[clockHand].pageNo);
                    
					bufTable[clockHand].Clear();
                    
					return OK;
                }
            }
        }
        // If frame isn't valid yet just use the frame and return OK
        else{

            frame = clockHand;

            return OK;
        }
        count_of_checked_frames = count_of_checked_frames + 1;
    }

    return BUFFEREXCEEDED;
}
	
const Status BufMgr::readPage(File* file, const int PageNo, Page*& page)
{
	int frame;
	int newFrame;
	Status fileStat;
	Status hashStat;
	Status allocStat;
	Status insertStat;
	// Runs lookup function on hashTable
	hashStat = hashTable->lookup(file, PageNo, frame);
	
	//Case 1
	if(hashStat != OK){
	  // call allocBuf to allocate a buffer frame 
	  allocStat = allocBuf(newFrame);
		
	  if (allocStat == UNIXERR){
	    return UNIXERR;
	  }
	  
	  else if (allocStat == BUFFEREXCEEDED){
	    return BUFFEREXCEEDED;
	  }
	  
	  else{
	    //to read the page from disk to buffer pool frame	
	    fileStat = file->readPage(PageNo, &bufPool[newFrame]);
	    
	    if(fileStat != OK){
	      return fileStat;
	    }
			
	    // next, insert the page into the hashtable			
	    insertStat = hashTable->insert(file, PageNo, newFrame);
	    
	    if (insertStat == HASHTBLERROR){
	      return HASHTBLERROR;
	    }
	    else{
	      // set the details for buffer pool frame
	      page = &bufPool[newFrame];
	      bufTable[newFrame].Set(file, PageNo);
	    }
	  }
	}
	//Case 2
	else if (hashStat == OK)
	{
		//update bits and pins
		bufTable[frame].pinCnt++;
		bufTable[frame].refbit = true;
		page = &bufPool[frame];
	}
	return OK;
}


const Status BufMgr::unPinPage(File* file, const int PageNo, 
			       const bool dirty) 
{
	int frame;
	Status status;

	// Runs lookup function on hashTable
	status = hashTable->lookup(file, PageNo, frame);

	// HASHNOTFOUND if the page is not in the buffer pool hash table
	if (status == HASHNOTFOUND){
		return HASHNOTFOUND;
	};
	
	// PAGENOTPINNED if the pin count_of_checked_frames is already 0.
	if (bufTable[frame].pinCnt == 0) {
		return PAGENOTPINNED;
	};

	// Decrements the pinCnt of the frame containing (file, PageNo)
	bufTable[frame].pinCnt = bufTable[frame].pinCnt - 1;

	// if dirty == true, sets the dirty bit to true
	if (dirty == true) {
		bufTable[frame].dirty = true;	
	}

	// returns OK if no errors occurred
	return OK;
}

const Status BufMgr::allocPage(File* file, int& pageNo, Page*& page) 
{
	// Allocate an empty page in the specified file by invoking file->allocatePage()
	file->allocatePage(pageNo);
	Status allocStat;
	int frame;

	// Obtain a buffer pool frame
	allocStat = allocBuf(frame);
	
	// UNIXERR if a Unix error occurred
	if(allocStat == UNIXERR){
		return UNIXERR;
	}
	// BUFFEREXCEEDED if all buffer frames are pinned	
	if(allocStat == BUFFEREXCEEDED){
		return BUFFEREXCEEDED;
	}
	
	// Entry inserted into the hash table
	Status insertStat;
	insertStat = hashTable->insert(file, pageNo, frame);

	// HASHTBLERROR if a hash table error occurred
	if(insertStat == HASHTBLERROR){
		return HASHTBLERROR;
	}
	
	// Set() invoked on the frame to set it up properly
	page = &bufPool[frame];
	bufTable[frame].Set(file, pageNo);

	// Return OK if no errors occurred
	return OK;
}

const Status BufMgr::disposePage(File* file, const int pageNo) 
{
    // see if it is in the buffer pool
    Status status = OK;
    int frameNo = 0;
    status = hashTable->lookup(file, pageNo, frameNo);
    if (status == OK)
    {
        // clear the page
        bufTable[frameNo].Clear();
    }
    status = hashTable->remove(file, pageNo);

    // deallocate it in the file
    return file->disposePage(pageNo);
}

const Status BufMgr::flushFile(const File* file) 
{
  Status status;

  for (int i = 0; i < numBufs; i++) {
    BufDesc* tmpbuf = &(bufTable[i]);
    if (tmpbuf->valid == true && tmpbuf->file == file) {

      if (tmpbuf->pinCnt > 0)
	  return PAGEPINNED;

      if (tmpbuf->dirty == true) {
#ifdef DEBUGBUF
	cout << "flushing page " << tmpbuf->pageNo
             << " from frame " << i << endl;
#endif
	if ((status = tmpbuf->file->writePage(tmpbuf->pageNo,
					      &(bufPool[i]))) != OK)
	  return status;

	tmpbuf->dirty = false;
      }

      hashTable->remove(file,tmpbuf->pageNo);

      tmpbuf->file = NULL;
      tmpbuf->pageNo = -1;
      tmpbuf->valid = false;
    }

    else if (tmpbuf->valid == false && tmpbuf->file == file)
      return BADBUFFER;
  }
  
  return OK;
}


void BufMgr::printSelf(void) 
{
    BufDesc* tmpbuf;
  
    cout << endl << "Print buffer...\n";
    for (int i=0; i<numBufs; i++) {
        tmpbuf = &(bufTable[i]);
        cout << i << "\t" << (char*)(&bufPool[i]) 
             << "\tpinCnt: " << tmpbuf->pinCnt;
    
        if (tmpbuf->valid == true)
            cout << "\tvalid\n";
        cout << endl;
    };
}


