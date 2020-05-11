/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

/**
 * Authors: Anthony Quintero-Quiroga & Joseph Leovao
 * PID: A10845368 & A12478957
 * Date: 04/26/2017
 * Filename: buffer.cpp
 * Purpose of File: Defines functions described in buffer.h
 * This is where the majority of our buffer manager is defined.
 * This file contains functions for constructing a buffMgr, destructing a buffMgr,
 * allocating a buffer frame using the clock algorithm, flushing dirty files and
 * other page operations
 */

#include <memory>
#include <iostream>
#include "buffer.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"

namespace badgerdb {

/*
 * Function Name: BufMgr
 * Input: uint32
 * Output: BufMgr Object
 * Purpose: Constructor for BufMgr class
 * Creates an array of BufDesc, an array of pages, a BufHashTable
 * and initializes a clockHand.
 */
BufMgr::BufMgr(std::uint32_t bufs)
	: numBufs(bufs) {
	bufDescTable = new BufDesc[bufs];

  for (FrameId i = 0; i < bufs; i++)
  {
  	bufDescTable[i].frameNo = i;
  	bufDescTable[i].valid = false;
  }

  bufPool = new Page[bufs];

	int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
  hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

  clockHand = bufs - 1;
}

/*
 * Function Name: ~BufMgr
 * Input: None
 * Output: None
 * Purpose: Destructor for BufMgr
 * Flushes out all dirty pages from the bufPool
 * then deallocates the buffer pool and the BufDesc Table
 */
BufMgr::~BufMgr() {
    for(unsigned int i = 0; i < numBufs; i++){
        if(bufDescTable[i].dirty == true){
            flushFile(bufDescTable[i].file);
        }
    }
  //Deallocate bufDescTable, bufPool and hashTable
    delete [] bufDescTable;
    delete [] bufPool;
    delete hashTable;
}

/*
 * Function Name: advanceClock
 * Input: None
 * Output: None
 * Purpose: Advance clock to next frame in the buffer pool
 * Uses modular math to logically make it like a clockHand
 */
void BufMgr::advanceClock()
{
    clockHand = (clockHand+1) % numBufs;
}

/*
 * Function Name: allocBuf
 * Input: FrameId reference
 * Output: None
 * Purpose: Allocates a freem frame using the clock algorithm
 */
void BufMgr::allocBuf(FrameId & frame)
{
 bool findNext = false;
    unsigned int pinned = 0;
    //
    while(!findNext){
        advanceClock();
        if(pinned == numBufs){
            throw BufferExceededException();
        }
        if (bufDescTable[clockHand].valid == false){
            bufDescTable[clockHand].Clear();
            frame = clockHand;
            findNext = true;
        }
        else if(bufDescTable[clockHand].refbit == true){
            bufDescTable[clockHand].refbit = false;
        }
        else if(bufDescTable[clockHand].pinCnt > 0){
            pinned++;
        }

        else if(bufDescTable[clockHand].refbit == false){
            if(bufDescTable[clockHand].dirty == true){
                bufDescTable[clockHand].file->writePage(bufPool[clockHand]);
            }
            hashTable->remove(bufDescTable[clockHand].file, bufDescTable[clockHand].pageNo);
            bufDescTable[clockHand].Clear();
            frame = clockHand;
            findNext = true;
        }
    }
}

/*
 * Function Name: readPage
 * Input: File pointer, constant PageID and reference to a Page
 * Output: None
 * Purpose: Read a page from disk into the buffer pool
 * or set appropriate ref bit and increment pinCnt
 */
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{
  FrameId frame;
    try{
        hashTable->lookup(file, pageNo, frame);
        bufDescTable[frame].refbit = true;
        bufDescTable[frame].pinCnt++;
        page = &bufPool[frame];

    }catch(HashNotFoundException e){
        FrameId frame;
        allocBuf(frame);
        //printf("This is frame#: %id\n", frame);
        bufPool[frame] = file->readPage(pageNo);
        hashTable->insert(file, pageNo, frame);
        bufDescTable[frame].Set(file, pageNo);
        page = &bufPool[frame];
    }
}

/*
 * Function Name: unPinPage
 * Input: File pointer, constant PageID and constant bool
 i* Output: None
 * Purpose: Decrement pinCnt of of the input and set the dirty bit
 */
void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty)
{
  FrameId frame;
    try{
        hashTable->lookup(file, pageNo, frame);
        if(bufDescTable[frame].pinCnt > 0){
            bufDescTable[frame].pinCnt--;
            if (dirty == true){
                bufDescTable[frame].dirty = dirty;
            }
        }
        else{
            throw PageNotPinnedException("Ping 本來就是 0", pageNo, frame);
        }
    }catch(HashNotFoundException e){

    }
}

/*
 * Function Name: flushFile
 * Input: File pointer
 * Output: None
 * Purpose:Flushes all pages belonging to the file, remove the pages from the
 * hashTable and clear the corresponding bufDescs
 */
void BufMgr::flushFile(const File* file)
{
    for(unsigned int i = 0; i < numBufs; i++){
        if(bufDescTable[i].file == file){//是他文件中的PAGE
            if(bufDescTable[i].pinCnt != 0){
                throw PagePinnedException("This removing page is already being used", bufDescTable[i].pageNo, bufDescTable[i].frameNo);
            }
            if(bufDescTable[i].valid == false){
                throw BadBufferException(bufDescTable[i].frameNo, bufDescTable[i].dirty, false, bufDescTable[i].refbit);
            }
            if(bufDescTable[i].dirty == true){
                bufDescTable[i].file->writePage(bufPool[i]);
                bufDescTable[i].dirty = false;
            }
            hashTable->remove(file, bufPool[i].page_number());
            bufDescTable[i].Clear();
        }
    }
}

/*
 * Function Name: allocPage
 * Input: File pointer, page number and reference to a page
 * Output: None
 * Purpose: Allocates an empty page and returns both the page number of
 *          the newly allocated page to the caller via the pageNo param
 *          and a pointer to the buffer frame allocated for the page via
 *          page param
 */

// InvalidRecordException thrown during main
void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page)
{
    FrameId frame;
    Page new_page = file->allocatePage();
    allocBuf(frame);
    hashTable->insert(file,new_page.page_number(), frame);
    bufDescTable[frame].Set(file, new_page.page_number());
    pageNo = new_page.page_number();
    bufPool[frame] = new_page; //?????????????????????
    page = &bufPool[frame];
}

/*
 * Function Name: disposePage
 * Input: File pointer and page number
 * Output: None
 * Purpose: Deletes a page from file.
 * If the page is in the buffer, clear page from buffer and remove
 * from hashTable
 */
void BufMgr::disposePage(File* file, const PageId PageNo)
{
    FrameId frame;
    try{
        hashTable->lookup(file, PageNo, frame);
        bufDescTable[frame].Clear();
        file->deletePage(PageNo);
        hashTable->remove(file, PageNo);
    }catch(HashNotFoundException e){
        file->deletePage(PageNo);
    }
}

/*
 * Function Name: printSelf
 * Input: void
 * Output: void
 * Purpose: Prints out the total number of valid frames in bufDescTable
 * Iterates through bufDescTable and counts the number of frames whose valid
 * bit is set to true. Then it prints that counted value.
 */
void BufMgr::printSelf(void)
{
  BufDesc* tmpbuf;
	int validFrames = 0;

  for (std::uint32_t i = 0; i < numBufs; i++)
	{
  	tmpbuf = &(bufDescTable[i]);
		std::cout << "FrameNo:" << i << " ";
		tmpbuf->Print();

  	if (tmpbuf->valid == true)
    	validFrames++;
  }

	std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

}
