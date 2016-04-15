#ifndef LOGMGR_H_
#define LOGMGR_H_

#include "LogRecord.h"
#include <vector>
#include "../StorageEngine/StorageEngine.h"

using namespace std;


const int NULL_LSN = -1;
const int NULL_TX = -1;



///////////////////  LogMgr  ///////////////////

class LogMgr {
 private:
    /* tx id -> entryn */
  map <int, txTableEntry> tx_table;
    /* page id -> earliest redo lsn */
  map <int, int> dirty_page_table;
  vector <LogRecord*> logtail; 

  /*
   * Find the LSN of the most recent log record for this TX.
   * If there is no previous log record for this TX, return 
   * the null LSN.
   */
  int getLastLSN(int txnum);

  /*
   * Update the TX table to reflect the LSN of the most recent
   * log entry for this transaction.
   */
  void setLastLSN(int txnum, int lsn);

  /*
   * Force log records up to and including the one with the
   * maxLSN to disk. Don't forget to remove them from the
   * logtail once they're written!
   */
  void flushLogTail(int maxLSN);

  StorageEngine* se;

  /* 
   * Run the analysis phase of ARIES.
   */
  void analyze(vector <LogRecord*> log);

  /*
   * Run the redo phase of ARIES.
   * If the StorageEngine stops responding, return false.
   * Else when redo phase is complete, return true. 
   */
  bool redo(vector <LogRecord*> log);

  /*
   * If no txnum is specified, run the undo phase of ARIES.
   * If a txnum is provided, abort that transaction.
   * Hint: the logic is very similar for these two tasks!
   */
  void undo(vector <LogRecord*> log, int txnum = NULL_TX);
  vector<LogRecord*> stringToLRVector(string logstring);
  
 public:
  /*
   * Abort the specified transaction.
   * Hint: you can use your undo function
   */
  void abort(int txid);

  /*
   * Write the begin checkpoint and end checkpoint
   */
  void checkpoint();

  /*
   * Commit the specified transaction.
   */
  void commit(int txid);

  /*
   * A function that StorageEngine will call when it's about to 
   * write a page to disk. 
   * Remember, you need to implement write-ahead logging
   */
  void pageFlushed(int page_id);

  /*
   * Recover from a crash, given the log from the disk.
   */
  void recover(string log);

  /*
   * Logs an update to the database and updates tables if needed.
   */
  int write(int txid, int page_id, int offset, string input, string oldtext);

  /*
   * Sets this.se to engine. 
   */
  void setStorageEngine(StorageEngine* engine);

  //destructor
  ~LogMgr() {
    while (!logtail.empty()) {
      delete logtail[0];
      logtail.erase(logtail.begin());
    }
  }
  //copy constructor omitted
  //Overloaded assignment operator
  LogMgr &operator= (const LogMgr &rhs) {
    if (this == &rhs) return *this;
    //delete anything in the logtail vector
    while (!logtail.empty()) {
      delete logtail[0];
      logtail.erase(logtail.begin());
    }
    for (vector<LogRecord*>::const_iterator it = rhs.logtail.begin(); it !=rhs.logtail.end(); ++it) {
      LogRecord * lr = *it;
      int lsn = lr->getLSN();
      int prevLSN = lr->getprevLSN();
      int txid = lr->getTxID();
      TxType type = lr->getType();
      if (type == UPDATE) {
	UpdateLogRecord* ulr = dynamic_cast<UpdateLogRecord *>(lr);
	int page_id = ulr->getPageID();
	int offset  = ulr->getOffset();
	string before = ulr->getBeforeImage();
	string after = ulr->getAfterImage();
	UpdateLogRecord* cpy_lr = new UpdateLogRecord(lsn, prevLSN, txid, page_id, offset, 
						      before, after);
	logtail.push_back(cpy_lr);
      } else if (type == CLR) {
	CompensationLogRecord* clr = dynamic_cast<CompensationLogRecord *>(lr);
	int page_id = clr->getPageID();
	int offset  = clr->getOffset();
	string after = clr->getAfterImage();
	int nextLSN = clr->getUndoNextLSN();
	CompensationLogRecord* cpy_lr = new CompensationLogRecord(lsn, prevLSN, txid, page_id, offset, 
								  after, nextLSN);
	logtail.push_back(cpy_lr);
      } else if (type == END_CKPT) {
	ChkptLogRecord * chk_ptr = dynamic_cast<ChkptLogRecord *>(lr);
	map <int, txTableEntry> tx_table = chk_ptr->getTxTable();
	map <int, int> dp_table = chk_ptr->getDirtyPageTable();
	ChkptLogRecord * cpy_lr = new ChkptLogRecord(lsn, prevLSN, txid, tx_table, dp_table);
	logtail.push_back(cpy_lr);
      } else { //type is ordinary log record
	LogRecord * cpy_lr = new LogRecord(lsn, prevLSN, txid, type);
	logtail.push_back(cpy_lr);
      }
    }
    se = rhs.se;
    tx_table = rhs.tx_table;
    dirty_page_table = rhs.dirty_page_table;
    return *this;
    
  }

  
};

/////////////////// End LogMgr  ///////////////////

#endif
