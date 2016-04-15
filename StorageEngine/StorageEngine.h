#ifndef STORAGEENGINE_H_
#define STORAGEENGINE_H_

#include <string>
#include <vector>

class LogMgr; 

struct Page {
    int page_id; //equal to the line number where it's stored in the file. 
    int pageLSN;
    bool dirty;
    std::string data;

    Page() {
        dirty = false;
    }

    Page(int new_page_id, int new_pageLSN, bool new_dirty, std::string new_data) {
        page_id = new_page_id;
        pageLSN = new_pageLSN;
        dirty = new_dirty;
        data = new_data;
    }
};

class StorageEngine {

    private:
        // Memory for records, when crash clear records.
        std::vector<Page> records;
	std::vector<Page> onDisk; 
	int log_sequence_number = 1;
        int master_lsn = -1;
	//Number of pageWrite calls permitted.
	//Must be 0 until a crash.
	int page_writes_permitted = 0;
	LogMgr* lm_ptr;
	std::string log_filename;
        std::string output_filename;
	const unsigned MEMORY_SIZE; //number of pages buffer can hold at once
	int findPage(int page_id); 
	void updatePage(int page_id, int offset, std::string text);
	void flushPage(int page_id);
	void updateLSN(int page_id, int newLSN);

    public:
        // Constructor
        StorageEngine();

	/* 
	 * Starts the storage engine with a database by reading the database
	 * from a file.
	 * Also sets the associated LogMgr and the logfile name.
	 */
	void start(std::string db_filename, LogMgr* log_mgr_ptr, std::string testcase_num);

	/*
	 * Ends the test case, writing onDisk to actual disk.
	 */
	void end(std::string db_filename);

	/* 
	 * Simulates a crash. 
	 * Sets page_writes_permitted to safe_writes.
	 * Replaces the old lm_ptr with log_mgr_ptr.
	 * Empties the records vector (our page buffer).
	 * Reads the log from log_entries
	 * Calls lm_ptr ->recover()
	 */
        void crash(int safe_writes, LogMgr* log_mgr_ptr);
	void end_crash(LogMgr* log_mgr_ptr);

	/*
	 * Appends the given string to the log file on disk.
	 */
        void updateLog(std::string log_entries);

	/*
	 * Write to a page starting from the offset byte with the particular
	 * transaction specified by txid.
	 */
        void write(int txid, int page_id, int offset, std::string input);

	/*
	 * Sets the number of page writes allowed for this abort,
	 * then calls LogMgr's abort function. 
	 */
	void abort(int txid, int pages_allowed);

	/*
	 * Increments the log_sequence_number by 1 and returns it.
	 */
        int nextLSN();

	/*
	 * Writes lsn to a particular location on the disk.
	 * Returns true on success.
	 */
	bool store_master(int lsn);

	/*
	 * Gets lsn from a particular location on the disk
	 * (where store_master wrote it)
	 */
        int get_master();
        

	/* 
	 * Returns the LSN of a page.
	 */
        int getLSN(int page_id);

	/*
	 * Return the filename of output file
	 */
        std::string getOutputFileName();

	/* 
	 * Returns as much of the log as is on disk
	 */
        std::string getLog();

	/*
	* Writes to a page in memory, if allowed.  
	* If page_writes_permitted <= 0, this just 
	* returns false and doesn't write the page. 
	*/
        bool pageWrite(int page_id, int offset, std::string text, int lsn);
};

#endif
