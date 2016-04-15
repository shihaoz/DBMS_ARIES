#include "StorageEngine.h"
#include "../StudentComponent/LogMgr.h"
#include <cstring>
#include <string>
#include <fstream>

using namespace std;

StorageEngine::StorageEngine() : MEMORY_SIZE(10) {
    page_writes_permitted = 0;
}

/* 
 * 
 * Starts the storage engine with a database by reading the database from a file
 * Also sets the associated LogMgr and the logfile name.
 */
void StorageEngine::start(string db_filename, LogMgr* log_mgr_ptr, string testcase_num) {

  lm_ptr = log_mgr_ptr;
  log_filename = "output/log/log";
  log_filename.append(testcase_num);
  log_filename.append(".log");

  output_filename = "output/dbs/db";
  output_filename.append(testcase_num);
  output_filename.append(".db");

  ifstream dbf(db_filename);
  int page_id = 1;
  int pageLSN = 0;
  string data = "";
  while(true) {
    if(!(dbf >> pageLSN))
      break;
    dbf.get();
    if(!getline(dbf, data))
      break;

    Page p = Page(page_id, pageLSN, false, data);
    onDisk.push_back(p);

    ++page_id;
  }

  dbf.close();
}

void StorageEngine::end(string db_filename) {
  //For each page in onDisk, 
    //write the page to db_filename 
  ofstream dbf(db_filename);
  for(unsigned i = 0; i < onDisk.size(); ++i) {
    dbf << onDisk[i].pageLSN << ' ' << onDisk[i].data << endl;
  }
  dbf.close();
}

/* 
 * crash(int safe_writes, LogMgr* log_mgr_ptr)
 * Sets page_writes_permitted to safe_writes. This is how many writes will
 * be allowed before the next crash occurs.
 * Replaces the old lm_ptr with log_mgr_ptr.
 * Empties the records vector (our page buffer).
 * Reads the log from log_entries
 * Calls lm_ptr ->recover()
 * 
 */
void StorageEngine::crash(int safe_writes, LogMgr* log_mgr_ptr) {
  page_writes_permitted = safe_writes;
  lm_ptr = log_mgr_ptr;
  records.clear();
  string log = getLog();
  lm_ptr->recover(log);
}

void StorageEngine::end_crash(LogMgr* log_mgr_ptr) {
  lm_ptr = log_mgr_ptr;
  page_writes_permitted = 0;
}


/* 
 * update_log(log_entries)
 *
 * We will append the log entries to the end of our log file.
 *
 */
void StorageEngine::updateLog(string log_entries) {
//find the file called [log_filename]. If it doesn't exist, create it.
//Append the string log_entries to the end of it.
//Close it.
    ofstream myfile;
    myfile.open(log_filename, std::ios_base::app);
    if (!myfile.is_open()){
        std::ofstream outfile (log_filename);
        outfile << log_entries;
        outfile.close();
    }
    else{
        myfile << log_entries;
    }
    
    myfile.close();
    
}

/* 
 * write (txid, page_id, offset, input)
 *
 * Write to a page starting from the offset byte with the particular
 * transaction specified by txid.
 * 
 */
void StorageEngine::write(int txid, int page_id, int offset, string input) {
    //Use findPage() to get the page's location in records vector
    int getindex = findPage(page_id);
    //old = whatever's on the page at the offset; length of old should be same as length of input
    string old;
    for (unsigned i = 0; i<input.length(); ++i) {
      old += records[getindex].data[offset+i];
    }
    int pageLSN = lm_ptr->write(txid, page_id, offset, input, old);
    //write the updated page
    updatePage(page_id, offset, input);
    //and update the pageLSN for the page
    updateLSN(page_id, pageLSN);
}

void StorageEngine::abort(int txid, int pages_allowed){
  page_writes_permitted = pages_allowed;
  lm_ptr->abort(txid);
}

/*
 * nextLSN()
 *
 * Increments the log_sequence_number by 1 and returns it.
 */
int StorageEngine::nextLSN() {
  ++log_sequence_number;
  return log_sequence_number;
}

/*
 * store_master(int lsn)
 *
 * Writes lsn to a particular location on the disk, returns true on success.
 */
bool StorageEngine::store_master(int lsn) {
    master_lsn = lsn;
    return true;
}

/*
 * get_master()
 *
 * Gets lsn from a particular location on the disk (same as above)
 */
int StorageEngine::get_master() {
    return master_lsn;
}


/* 
* Returns the LSN of a page.
*/
int StorageEngine::getLSN(int page_id) {
  int i = findPage(page_id);
  return records[i].pageLSN;
}

/*
 * Return the filename of output file
 */
string StorageEngine::getOutputFileName() {
  return output_filename;
}

/* 
* Returns as much of the log as is on disk
*/
string StorageEngine::getLog() {
//read the file [log_filename] in as a string, and return that.
    string wholefile, tmp;
    
    ifstream input(log_filename);
    
    while(!input.eof()) {
        getline(input, tmp);
	if (tmp != "") {
	  wholefile += tmp;
	  wholefile += "\n";
	}
    }
    input.close();
    return wholefile;
    
}


/* 
* void pageWrite(int page_id, int offset, string text)
* Writes to a page, if allowed.  If page_writes_permitted <= 0, this just 
* returns false and doesn't write the page. 
*/
bool StorageEngine::pageWrite(int page_id, int offset, string text, int lsn) {
  if (page_writes_permitted <= 0) 
    return false;
  --page_writes_permitted;
  updatePage(page_id, offset, text);
  updateLSN(page_id, lsn);
  return true;
}


//private

/* 
 * Returns the index of the specified page in the records vector.
 * If the desired page is not in the records vector, flushes some
 * other page from records to disk and reads the desired page
 * into records, then returns the index.
 *
 * return -1 if page not found in either records or onDisk
 */
int StorageEngine::findPage(int page_id) {
  if (page_id > (int)onDisk.size()) //page does not exist
    return -1;

  for (unsigned i = 0; i < records.size(); ++i)
      if (records[i].page_id == page_id)
          return i;

  // If did not return, that means page not found inside records.
  if (records.size() >= MEMORY_SIZE){
    Page p = records.back();
    int page_id = p.page_id;
    flushPage(page_id); 
  }

  records.push_back(onDisk[page_id-1]);
  return (int)records.size()-1;
  
}

/* 
 * updatePage(int page_id, int offset, string text)
 *
 */
void StorageEngine::updatePage(int page_id, int offset, string text) {
  int i = findPage(page_id);
  records[i].dirty = true;
  //update records[i].data to have the specified text at the specified offset. 
  records[i].data.replace(offset, text.length(), text);
}

void StorageEngine::flushPage(int page_id) {
  //If the page's dirty bit is true, set it false and update this page in onDisk, 
  //Remove it from the records vector
  for (unsigned i = 0; i < records.size(); ++i){
    if (records[i].page_id == page_id) {
      if (records[i].dirty){
	records[i].dirty = false;
	lm_ptr->pageFlushed(page_id);
	onDisk[page_id-1] = records[i];
      }
      records.erase(records.begin() + i);
    }
    }
}

void StorageEngine::updateLSN(int page_id, int newLSN) {
  int i = findPage(page_id);
  records[i].pageLSN = newLSN;
}
