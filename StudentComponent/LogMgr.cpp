//
//  LogMgr.cpp
//  ARIES_recovery_DBMS
//
//  Created by Shihao Zhang on 4/13/16.
//  Copyright © 2016 David Zhang. All rights reserved.
//

#include "LogMgr.h"
#include <string>
#include <vector>
#include <algorithm>
#include <functional>
#include <sstream>
#include <queue>
/**
 LogMgr can call a LogRecord's toString method to transform the LogRecord into a string, and then pass a string to StorageEngine::updateLog to append a string to the log on disk. 
 The log on disk will have one record per line; you can append multi-line strings to it if you want to add more than one record at once. 
 If LogMgr wants to read old log entries, it can call StorageEngine::getLog(), which will return a (multi-line) string. LogRecord::stringToRecordPtr can parse a line of that string and give you a pointer to a LogRecord of that line.
 */

vector<LogRecord*> LogMgr::stringToLRVector(string logstring){
    stringstream ss;
    ss << logstring;
    vector<LogRecord*> ret;
    string holder;
    
    while (getline(ss, holder)) {
        ret.push_back(LogRecord::stringToRecordPtr(holder));
    }
    
    return ret;
}


int LogMgr::getLastLSN(int txnum){
    /*
     * Find the LSN of the most recent log record for this TX.
     * If there is no previous log record for this TX, return
     * the null LSN.
     */
    if (tx_table.find(txnum) != tx_table.end()) {
        return tx_table[txnum].lastLSN;
    }
    return NULL_LSN;
}


void LogMgr::setLastLSN(int txnum, int lsn){
    /*
     * Update the TX table to reflect the LSN of the most recent
     * log entry for this transaction.
     */
    tx_table[txnum].lastLSN = lsn;
}

/*
 * Force log records up to and including the one with the
 * maxLSN to disk. Don't forget to remove them from the
 * logtail once they're written!
 */
void LogMgr::flushLogTail(int maxLSN){
    string logs_to_flush;
    auto it = logtail.begin();
    
    /* get the records up to maxLSN */
    while (it != logtail.end() && (*it)->getLSN() <= maxLSN) {
        logs_to_flush.append((*it)->toString());
        ++it;
    }
    se->updateLog(logs_to_flush);
    logtail.erase(logtail.begin(), it);
}

/*
 * Run the analysis phase of ARIES.
 */
void LogMgr::analyze(vector <LogRecord*> log){
    /* 1. get most recent checkpoint */
    int lsn_checkpoint = se->get_master();

    /* 2. recover TxTable, DPT from most recent checkpoint (if exists)
        */
    int log_index = 0;
    if (lsn_checkpoint == -1) {
        // TxTable and DPT should be empty
        tx_table.clear();
        dirty_page_table.clear();
    }
    else{
        while (log[log_index]->getLSN() < lsn_checkpoint) {
            log_index++;
        }
        ChkptLogRecord* checkpoint = dynamic_cast<ChkptLogRecord*>(log[log_index]);
        tx_table = checkpoint->getTxTable();
        dirty_page_table = checkpoint->getDirtyPageTable();
        log_index++;
    }
    
    /* 3. scan forward */
    for (; log_index < log.size(); log_index++) {
        LogRecord* this_record = log[log_index];

        if (this_record->getType() == TxType::END) {
            /* REMOVE from TxTable */
            if (tx_table.find(this_record->getTxID()) != tx_table.end()) {
                tx_table.erase(this_record->getTxID());
            }
        }
        else{
            /* 3.1 update Tx Table */
            setLastLSN(this_record->getTxID(), this_record->getLSN());
            if (this_record->getType() == TxType::COMMIT) {
                tx_table[this_record->getTxID()].status = TxStatus::C;
            }
            else{
                tx_table[this_record->getTxID()].status = TxStatus::U;
            }
            
            /* 3.2 update dirty page table
              only if type= Update/CLR */
            int page_id = -1;
            if (this_record->getType() == TxType::UPDATE) {
                UpdateLogRecord* update = dynamic_cast<UpdateLogRecord*>(this_record);
                page_id = update->getPageID();
            }
            else if (this_record->getType() == TxType::CLR){
                CompensationLogRecord* clr = dynamic_cast<CompensationLogRecord*>(this_record);
                page_id = clr->getPageID();
            }
            
            if (page_id != -1 && dirty_page_table.find(page_id) == dirty_page_table.end()) {
                /* if this is an update/clr, and DPT has no entry */
                dirty_page_table[page_id] = this_record->getLSN();
            }
        }
    }
}

/*
 * Run the redo phase of ARIES.
 * If the StorageEngine stops responding aka pageWrite = false, return false.
 * Else when redo phase is complete, return true.
 */
bool LogMgr::redo(vector <LogRecord*> log){

    if (dirty_page_table.empty()) {
        /* nothing to do */

    }
    else{
        /* find the smallest lsn in DPT */
        auto it = dirty_page_table.begin();
        int lsn_start = it->second;
        ++it;
        
        while (it != dirty_page_table.end()) {
            lsn_start = min(it->second, lsn_start);
            ++it;
        }
        
        int idx = 0;
        while (idx < log.size() && log[idx]->getLSN() < lsn_start) {++idx;}
        
        for (; idx < log.size(); idx++) {
            int page_id, lsn_now, offset;
            string to_write;

            /* 1. check if in dirty_page_table */
            /* 2. check if needs to write */
            /* 3. read the actual lsn from disk, check if need to update */
            /* 3.1 if yes, apply update, and update the page's lsn in disk */
            

            if(log[idx]->getType() == TxType::UPDATE){
                UpdateLogRecord* holder = dynamic_cast<UpdateLogRecord*>(log[idx]);
                page_id = holder->getPageID();
                lsn_now = holder->getLSN();
                offset = holder->getOffset();
                to_write = holder->getAfterImage();
            }
            else if(log[idx]->getType() == TxType::CLR){
                CompensationLogRecord* holder = dynamic_cast<CompensationLogRecord*>(log[idx]);
                page_id = holder->getPageID();
                lsn_now = holder->getLSN();
                offset = holder->getOffset();
                to_write = holder->getAfterImage();
            }
            else{
                continue;
            }
            
            if (dirty_page_table.find(page_id) != dirty_page_table.end() &&
                dirty_page_table[page_id] <= lsn_now &&
                se->getLSN(page_id) < lsn_now
                ) {
                /* if pageWrite fail, return false */
                if(se->pageWrite(page_id, offset, to_write, lsn_now) == false){
                    return false;
                }
            }
        }// end:for
    }

    /* write an end for every commited Tx */
    for (auto it : tx_table) {
        if (it.second.status == TxStatus::C) {
            logtail.push_back(new LogRecord(se->nextLSN(), it.second.lastLSN, it.first, TxType::END));
            tx_table.erase(it.first);
        }
    }
    return true;
}

/*
 * If no txnum is specified, run the undo phase of ARIES.
 * If a txnum is provided, abort that transaction.
 * Hint: the logic is very similar for these two tasks!
 */
void LogMgr::undo(vector <LogRecord*> log, int txnum){
    priority_queue<int> toUndo; // lsns to undo
    
    if (txnum != NULL_TX) {
        if (tx_table.find(txnum) == tx_table.end()) {
            return;
        }
        toUndo.push(getLastLSN(txnum));
    }
    else{
        auto it = tx_table.begin();
        while (it != tx_table.end()) {
            if (it->second.lastLSN != NULL_LSN && it->second.status == TxStatus::U) {
                toUndo.push(it->second.lastLSN);
            }

            it++;
        }
    }
    int idx = log.size() - 1;
    while (toUndo.size() > 0 && idx >= 0) {
        int lsn_now = toUndo.top();  toUndo.pop();
        
        while ( idx >= 0 && log[idx]->getLSN() != lsn_now) {idx--;}
        if (idx < 0) {
            return; // should not reach here
        }
        
        if (log[idx]->getType() == TxType::CLR) {
            /* if this is a CLR */
            CompensationLogRecord* holder = dynamic_cast<CompensationLogRecord*>(log[idx]);
            if (holder->getUndoNextLSN() == NULL_LSN) {
                /* write an end for this Tx */
                logtail.push_back(new LogRecord(se->nextLSN(), getLastLSN(holder->getTxID()), holder->getTxID(), TxType::END));
                tx_table.erase(holder->getTxID());
                continue;
            }
            toUndo.push(holder->getUndoNextLSN());
        }
        
        else if (log[idx]->getType() == TxType::UPDATE){
            /* if update, undo */
            UpdateLogRecord* update_log = dynamic_cast<UpdateLogRecord*>(log[idx]);
            int lsn = se->nextLSN();
            
            /* 1. write an CLR to log
              update Tx Table */
            CompensationLogRecord* new_log = new CompensationLogRecord(lsn,
                                                                       getLastLSN(update_log->getTxID()),
                                                                       update_log->getTxID(),
                                                                       update_log->getPageID(),
                                                                       update_log->getOffset(),
                                                                       update_log->getBeforeImage(),
                                                                       update_log->getprevLSN());
            
            logtail.push_back(new_log);
            setLastLSN(update_log->getTxID(), lsn);
            
            /* 2. undo */
            if (se->pageWrite(update_log->getPageID(), update_log->getOffset(), update_log->getBeforeImage(), lsn) == false) {
                return;
            }
            
            /* 3. if end record for this Tx */
            if (update_log->getprevLSN() == NULL_LSN) {
                /* write an end record for this transaction, take it off TxTable */
                logtail.push_back(new LogRecord(se->nextLSN(), lsn, update_log->getTxID(), TxType::END));
                tx_table.erase(update_log->getTxID());
            }
            else{
                toUndo.push(update_log->getprevLSN());
            }
        }
        else if(log[idx]->getType() == TxType::ABORT){
            if (log[idx]->getprevLSN() == NULL_LSN) {
                /* write an end to the abort Tx */
                logtail.push_back(new LogRecord(se->nextLSN(), log[idx]->getLSN(), log[idx]->getTxID(), TxType::END));
                tx_table.erase(log[idx]->getTxID());
            }
            else{
                toUndo.push(log[idx]->getprevLSN());
            }
        }
        else{
            /* no need to do anything */
        }
    }// end: while
}


/*
 * Abort the specified transaction.
 * Hint: you can use your undo function
 */
void LogMgr::abort(int txid){
    /* write an abort */
    int lsn = se->nextLSN();
    logtail.push_back(new LogRecord(lsn, getLastLSN(txid), txid, TxType::ABORT));
    setLastLSN(txid, lsn);
    
    vector<LogRecord*> logs = LogMgr::stringToLRVector(se->getLog());
    for (int i = 0; i < logtail.size(); i++) {
        logs.push_back(logtail[i]);
    }
    /* call undo  */
    undo(logs, txid);
}

/*
 * Write the begin checkpoint and end checkpoint
 */
void LogMgr::checkpoint(){
    /* write a begin checkpoint message */
    int lsn_now = se->nextLSN();
    int lsn_prev = NULL_LSN;
    logtail.push_back(new LogRecord(lsn_now, lsn_prev, NULL_TX, TxType::BEGIN_CKPT));
    lsn_prev = lsn_now;
    lsn_now = se->nextLSN();
    /* write a end checkpoint */
    logtail.push_back(new ChkptLogRecord(lsn_now, lsn_prev, NULL_TX, tx_table, dirty_page_table));

    /* write end checkpoint to stable storage */
    se->store_master(lsn_now);
    flushLogTail(lsn_now);
}

/* force-write a commit
 * Commit the specified transaction.
 */
void LogMgr::commit(int txid){
    /* write a commit log */
    int lsn_now = se->nextLSN();
    logtail.push_back(new LogRecord(lsn_now, getLastLSN(txid), txid, TxType::COMMIT));
    
    flushLogTail(lsn_now);
    tx_table.erase(txid);
    
    /* write an end record after flush */
    logtail.push_back(new LogRecord(se->nextLSN(), lsn_now, txid, TxType::END));

}

/*
 * A function that StorageEngine will call when it's about to
 * write a page to disk.
 * Remember, you need to implement write-ahead logging
 */
void LogMgr::pageFlushed(int page_id){
    
    /* log first */
    flushLogTail(se->getLSN(page_id));
    dirty_page_table.erase(page_id);
    return;
}

/*
 * Recover from a crash, given the log from the disk.
 */
void LogMgr::recover(string log){
    /* 1. get log */
    vector<LogRecord*> logs = stringToLRVector(log);
    analyze(logs);
    if (redo(logs) == false) {
        return;
    }
    undo(logs);
}

/*
 * Logs an update to the database and updates tables if needed.
 * return the pageLSN that that page should update it's pageLSN to
 */
int LogMgr::write(int txid, int page_id, int offset, string input, string oldtext){
    int lsn_now = se->nextLSN();
    int lsn_prev = getLastLSN(txid);

        /* update log tail */
    UpdateLogRecord* log_now = new UpdateLogRecord(lsn_now, lsn_prev, txid, page_id, offset, oldtext, input);
    logtail.push_back(log_now);
    setLastLSN(txid, lsn_now);
    
    /* update tx table */
    tx_table[txid].lastLSN = lsn_now;
    tx_table[txid].status = TxStatus::U;
   
    /* update dirty page table */
    if (dirty_page_table.find(page_id) == dirty_page_table.end()) {
        dirty_page_table[page_id] = lsn_now;
    }
    
    return lsn_now;
}



void LogMgr::setStorageEngine(StorageEngine* engine){
    /*
     * Sets this.se to engine.
     */
    se = engine;
}