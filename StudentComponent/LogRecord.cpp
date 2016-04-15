#include "LogRecord.h"
#include <sstream>

using namespace std;

LogRecord* LogRecord::stringToRecordPtr(string rec_string){
  stringstream ss(rec_string);
  int lsn, prevLSN, txID;
  string str_type;
  TxType type = UPDATE; //initializing arbitrarily to get rid of compiler warning.
  ss >> lsn >> prevLSN >> txID >> str_type;
  if (str_type == "update") {
    type = UPDATE;
    int pageID, offset;
    string before_image, after_image;
    ss >> pageID >> offset >> before_image >> after_image;
    UpdateLogRecord* ulr = new UpdateLogRecord(lsn, prevLSN, txID, pageID, offset, before_image, after_image); 
    return ulr;
  } else if (str_type == "CLR") {
    type = CLR;
    int pageID, offset, undoNextLSN;
    string after_image;
    ss >> pageID >> offset >> after_image >> undoNextLSN;
    CompensationLogRecord* clr = new CompensationLogRecord(lsn,prevLSN, txID,
							  pageID, offset, after_image,
							  undoNextLSN);

    return clr;
  } else if (str_type == "end_checkpoint") {
    type = END_CKPT;
    map<int, txTableEntry> txmap;
    map<int, int> dirtypagemap;
    string curly;
    ss >> curly;
    //parse the tx table map
    string txmapstr;
    getline(ss, txmapstr, '}');
    stringstream ss2(txmapstr);
    string item;
    while (getline(ss2, item, ']')) {
      stringstream ss3(item);
      string square, status_str;
      int tx_int, lastLSN;
      ss3 >> square >> tx_int >> lastLSN >> status_str;
      TxStatus status;
      if (status_str == "U")
	status = U;
      else
	status = C;
      txTableEntry entry = txTableEntry(lastLSN, status);
      txmap.insert(pair<int, txTableEntry>(tx_int, entry));
    }
    ss >> curly;
    //parse the dirty page table map
    string dpmapstr;
    getline(ss, dpmapstr, '}');
    stringstream ss4(dpmapstr);
    string item2;
    while (getline(ss4, item2, ']')) {
      stringstream ss3(item2);
      string square;
      int i, j;
      ss3 >> square >> i >> j;
      dirtypagemap.insert(pair<int, int>(i,j));
    }
    ChkptLogRecord* chlr = new ChkptLogRecord(lsn, prevLSN, txID, 
					      txmap, dirtypagemap);
    return chlr;

  } else {
    if (str_type == "commit") {
      type = COMMIT;
    } else if (str_type == "abort") {
      type = ABORT;
    } else if (str_type == "end") {
      type = END;
    } else if (str_type == "begin_checkpoint") {
      type = BEGIN_CKPT;
    }
    LogRecord* lr = new LogRecord(lsn, prevLSN, txID, type);
    return lr;
  }
  
}

string LogRecord::toString() {
  string result = basicToString();
  result.append("\n");
  return result;
}

string LogRecord::basicToString() {
    string result = "";
    result.append(to_string(lsn));
    result.append("\t");
    result.append(to_string(prevLSN));
    result.append("\t");
    result.append(to_string(txID));
    result.append("\t");

    switch (type) {
    case UPDATE:
      result.append("update");
      break;
    case COMMIT:
      result.append("commit");
      break;
    case ABORT:
      result.append("abort");
      break;
    case END:
      result.append("end");
      break;
    case CLR:
      result.append("CLR");
      break;
    case BEGIN_CKPT:
      result.append("begin_checkpoint");
      break;
    case END_CKPT:
      result.append("end_checkpoint");
      break;    
    }
    
    return result;
  }



string UpdateLogRecord::toString() {
  string result = basicToString();
  result.append("\t");
  result.append(to_string(pid));
  result.append("\t");
  result.append(to_string(offset));
  result.append("\t");
  result.append(beforeImage);
  result.append("\t");
  result.append(afterImage);
  result.append("\n");
  return result;
}



string CompensationLogRecord::toString() {
  string result = basicToString();
  result.append("\t");
  result.append(to_string(pageID));
  result.append("\t");
  result.append(to_string(offset));
  result.append("\t");
  result.append(afterImage);
  result.append("\t");
  result.append(to_string(undoNextLSN));
  result.append("\n");
  return result;
}

string ChkptLogRecord::toString() {
  string result = basicToString();
  result.append("\t");
  result.append(txMapToString(txTable));
  result.append("\t");
  result.append(intMapToString(dirtyPageTable));
  result.append("\n");
  return result;
}

string ChkptLogRecord::intMapToString(map <int, int> myMap) {
  string result = "{";
  for (map<int,int>::iterator it = myMap.begin(); 
	 it != myMap.end(); ++it) {
    result.append(" [ ");
    result.append(to_string(it->first));
    result.append(" ");
    result.append(to_string(it->second));
    result.append(" ]");
  } 
  result.append("}");
  return result;
}

string ChkptLogRecord::txMapToString(map <int, txTableEntry> myMap) {
  string result = "{";
  for (map<int,txTableEntry>::iterator it = myMap.begin(); 
	 it != myMap.end(); ++it) {
    result.append(" [ ");
    result.append(to_string(it->first));
    result.append(" ");
    result.append(to_string((it->second).lastLSN));
    result.append(" ");
    if ((it->second).status == U)
      result.append("U");
    else
      result.append("C");
    result.append(" ]");
  } 
  result.append("}");
  return result;
}
