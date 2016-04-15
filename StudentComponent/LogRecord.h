#include <string>
#include <map>

using namespace std;

enum TxStatus {U, C};
enum TxType {UPDATE, COMMIT, ABORT, END, CLR, BEGIN_CKPT, END_CKPT};

struct txTableEntry {
  int lastLSN;
  TxStatus status;
  txTableEntry(){};
  txTableEntry(int lsn, TxStatus stat) {lastLSN=lsn; status=stat; };
};

///////////////////  LogRecord  ///////////////////

class LogRecord {
 public:
 LogRecord(int lsn_in, int prev_lsn, int tx_id, TxType txtype) :
  lsn(lsn_in), prevLSN(prev_lsn), txID(tx_id), type(txtype) {}

  static LogRecord* stringToRecordPtr(string rec_string);

  virtual string toString();

  virtual ~LogRecord() {}

  int getLSN() {return lsn;}
  int getprevLSN() {return prevLSN;}
  int getTxID() {return txID;}
  TxType getType(){return type;}
  

 protected:
  int lsn;
  int prevLSN;
  int txID;
  TxType type;

  //Make a string with the lsn, prevLSN, txID, and type
  //for use in this and the subclass toString functions
  string basicToString();  
};
///////////////////  End LogRecord  ///////////////////

///////////////////  UpdateLogRecord  ///////////////////
class UpdateLogRecord : public LogRecord{
 public:
  UpdateLogRecord(int lsn_in, int prev_lsn, int tx_id, 
		 int page_id, int page_offset, 
		 string before_img, string after_img) :
  LogRecord(lsn_in, prev_lsn, tx_id, UPDATE)
    {
        pid = page_id; 
        offset = page_offset;
        beforeImage = before_img;
        afterImage = after_img;
    }



  int getPageID() {return pid;}
  int getOffset() {return offset;}
  string getBeforeImage() {return beforeImage;}
  string getAfterImage() {return afterImage;}

  virtual string toString();

 private:
  int pid;
  int offset;
  string beforeImage;
  string afterImage;
};
///////////////////  End UpdateLogRecord  ///////////////////

///////////////////  CompensationLogRecord  ///////////////////
class CompensationLogRecord : public LogRecord{
 public:
 CompensationLogRecord(int lsn_in, int prev_lsn, int tx_id, 
		       int page_id, int page_offset,
		       string after_img, int undo_next_lsn) :
  LogRecord(lsn_in, prev_lsn, tx_id, CLR), pageID(page_id),
    offset(page_offset), afterImage(after_img),
    undoNextLSN(undo_next_lsn) {}

  virtual string toString();

  int getPageID() {return pageID;}
  int getOffset() {return offset;}
  string getAfterImage() {return afterImage;}
  int getUndoNextLSN() {return undoNextLSN;}
 private: 
  int pageID;
  int offset;
  string afterImage; 
  //Unlike an update record, only need redo info, not undo info!
  int undoNextLSN;
};

///////////////////  End CompenstationLogRecord  ///////////////////

/////////////////// ChkptLogRecord  ///////////////////
class ChkptLogRecord : public LogRecord{
 public:
  ChkptLogRecord(int lsn_in, int prev_lsn, int tx_id, 
		      map <int,txTableEntry> tx_table, 
		      map <int,int> dirty_page_table) :
  LogRecord(lsn_in, prev_lsn, tx_id, END_CKPT), txTable(tx_table),
    dirtyPageTable(dirty_page_table)
    {}

  map <int,txTableEntry> getTxTable() {return txTable;}
  map <int,int> getDirtyPageTable() {return dirtyPageTable;}
  virtual string toString();
 private:
  map <int,txTableEntry> txTable;
  map <int,int> dirtyPageTable;  

  string intMapToString(map <int, int> myMap);
  string txMapToString(map <int, txTableEntry> myMap);
};


///////////////////  End ChkptLogRecord  ///////////////////
