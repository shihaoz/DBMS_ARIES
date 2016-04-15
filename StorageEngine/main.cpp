#include "StorageEngine.h"
#include "../StudentComponent/LogMgr.h"
#include <vector>
#include <iostream>
#include <fstream>
#include <sstream>
#include <string>

using namespace std;

/*
 * crash(vector<int> safe_writes, StorageEngine* se)
 * For each num in safe_writes:
 * Destroys the running LogMgr instance
 * and replaces it with another LogMgr.
 * Calls se->crash(num, LogMgr).
 */
LogMgr* crash(vector<int> safe_writes, StorageEngine* se) {
  LogMgr* newLm = NULL;
  for (unsigned i = 0; i < safe_writes.size(); ++i)
    {
      if (newLm)
	delete newLm;
      newLm = new LogMgr();
      newLm->setStorageEngine(se);
      se->crash(safe_writes[i], newLm);
    }
    return newLm;
}

// Assumption: 'correct' folder and student submission's folder has already be created.
// Assumption: code will run in root eecs484 folder
void runTestcase(string filename) {
  //Create an instance of StorageEngine called se.
  StorageEngine se;
  //Create an instance of LogMgr called lm.
  LogMgr* lm = new LogMgr();
  lm->setStorageEngine(&se);
  //open testcase file filename
  ifstream myfile;
  myfile.open(filename);
  //The first line of the testcase tells you the filename for the database.
  string db_filename;
  getline(myfile, db_filename);
  //Call se.start(db_filename)
  se.start(db_filename, lm, filename.substr( filename.length() - 2 ));
  //for the remaining lines in testcase:
  string contents;
  getline(myfile, contents);
  
  while (contents != ""){  
    stringstream ss(contents);
    string ifcrash;
    ss >> ifcrash;
    // if it looks like <crash {5 2}>, call crash({5,2}), where {5, 2} is a vector of ints.
    if (ifcrash == "crash") {
      string intvector;
      vector<int> crashint;
      while(ss >> intvector) {
	if (intvector[0] == '{') 
	  intvector.erase(0,1);
	unsigned int len = intvector.length();
	if (intvector[len-1] == '}')
	  intvector.erase(len-1,1);
	stringstream ss2(intvector);
	int i;
	while (ss2 >> i) {
	  crashint.push_back(i);
	}
      }
      lm=crash(crashint, &se);//return pointer?
      se.end_crash(lm);
    }
    else if (ifcrash == "end") {
      se.end(se.getOutputFileName());
      break;
    } 
    else if (ifcrash == "checkpoint"){
	lm->checkpoint();
    }
    else{
      stringstream ss(contents);
      int firstnum;
      ss >> firstnum;
      string typechoose;
      ss >>typechoose;
      //if it looks like <1 commit>, call lm.commit(1)
      if (typechoose == "commit") {
	lm->commit(firstnum);
      }
      //if it looks like <1 abort 5>, call se.abort(1, 5)
      else if (typechoose == "abort"){
	int pages_allowed;
	ss >> pages_allowed;
	se.abort(firstnum, pages_allowed);
      }
      //if it looks like <1 write 34 27 "ABC">,
      //Call se.write(1, 34, 27, "ABC")
      else if (typechoose == "write"){
	int a,b;
	string c;
	ss >> a >> b >> c;
	se.write(firstnum,a,b,c);
      }
    }
    getline(myfile, contents);
  }
  delete lm; lm = NULL;
  myfile.close();
}

/*
 * Main function for running the database recovery simulator.
 * 
 */
int main (int argc, char *argv[]) {
    runTestcase(argv[1]);

    return 0;
}
