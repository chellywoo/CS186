package edu.berkeley.cs186.database.recovery;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.concurrency.DummyLockContext;
import edu.berkeley.cs186.database.concurrency.LockType;
import edu.berkeley.cs186.database.io.DiskSpaceManager;
import edu.berkeley.cs186.database.memory.BufferManager;
import edu.berkeley.cs186.database.memory.Page;
import edu.berkeley.cs186.database.recovery.records.*;
import edu.berkeley.cs186.database.table.Record;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Implementation of ARIES.
 */
public class ARIESRecoveryManager implements RecoveryManager {
    // Disk space manager.
    DiskSpaceManager diskSpaceManager;
    // Buffer manager.
    BufferManager bufferManager;

    // Function to create a new transaction for recovery with a given
    // transaction number.
    private Function<Long, Transaction> newTransaction;

    // Log manager
    LogManager logManager;
    // Dirty page table (page number -> recLSN).
    Map<Long, Long> dirtyPageTable = new ConcurrentHashMap<>();
    // Transaction table (transaction number -> entry).
    Map<Long, TransactionTableEntry> transactionTable = new ConcurrentHashMap<>();
    // true if redo phase of restart has terminated, false otherwise. Used
    // to prevent DPT entries from being flushed during restartRedo.
    boolean redoComplete;

    public ARIESRecoveryManager(Function<Long, Transaction> newTransaction) {
        this.newTransaction = newTransaction;
    }

    /**
     * Initializes the log; only called the first time the database is set up.
     * The master record should be added to the log, and a checkpoint should be
     * taken.
     */
    @Override
    public void initialize() {
        this.logManager.appendToLog(new MasterLogRecord(0));
        this.checkpoint();
    }

    /**
     * Sets the buffer/disk managers. This is not part of the constructor
     * because of the cyclic dependency between the buffer manager and recovery
     * manager (the buffer manager must interface with the recovery manager to
     * block page evictions until the log has been flushed, but the recovery
     * manager needs to interface with the buffer manager to write the log and
     * redo changes).
     * @param diskSpaceManager disk space manager
     * @param bufferManager buffer manager
     */
    @Override
    public void setManagers(DiskSpaceManager diskSpaceManager, BufferManager bufferManager) {
        this.diskSpaceManager = diskSpaceManager;
        this.bufferManager = bufferManager;
        this.logManager = new LogManager(bufferManager);
    }

    // Forward Processing //////////////////////////////////////////////////////

    /**
     * Called when a new transaction is started.
     *
     * The transaction should be added to the transaction table.
     *
     * @param transaction new transaction
     */
    @Override
    public synchronized void startTransaction(Transaction transaction) {
        this.transactionTable.put(transaction.getTransNum(), new TransactionTableEntry(transaction));
    }

    /**
     * Called when a transaction is about to start committing.
     *
     * A commit record should be appended, the log should be flushed,
     * and the transaction table and the transaction status should be updated.
     *
     * @param transNum transaction being committed
     * @return LSN of the commit record
     */
    @Override
    public long commit(long transNum) {
        // TODO(proj5): implement
        // 记录写入日志
        TransactionTableEntry entry = transactionTable.get(transNum);
        CommitTransactionLogRecord commit = new CommitTransactionLogRecord(transNum, entry.lastLSN);
        long LSN = logManager.appendToLog(commit);
        // 更新lastLSN
        entry.lastLSN = LSN;
        entry.transaction.setStatus(Transaction.Status.COMMITTING);
        // 刷新到该记录处
        flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a transaction is set to be aborted.
     *
     * An abort record should be appended, and the transaction table and
     * transaction status should be updated. Calling this function should not
     * perform any rollbacks.
     *
     * @param transNum transaction being aborted
     * @return LSN of the abort record
     */
    @Override
    public long abort(long transNum) {
        // TODO(proj5): implement
        // 记录写入日志
        TransactionTableEntry entry = transactionTable.get(transNum);
        AbortTransactionLogRecord commit = new AbortTransactionLogRecord(transNum, entry.lastLSN);
        long LSN = logManager.appendToLog(commit);
        // 更新lastLSN
        entry.lastLSN = LSN;
        entry.transaction.setStatus(Transaction.Status.ABORTING);
        // 刷新到该记录处
//        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a transaction is cleaning up; this should roll back
     * changes if the transaction is aborting (see the rollbackToLSN helper
     * function below).
     *
     * Any changes that need to be undone should be undone, the transaction should
     * be removed from the transaction table, the end record should be appended,
     * and the transaction status should be updated.
     *
     * @param transNum transaction to end
     * @return LSN of the end record
     */
    @Override
    public long end(long transNum) {
        // TODO(proj5): implement
        TransactionTableEntry entry = transactionTable.get(transNum);
        // 判断是否需要回滚
        if(entry.transaction.getStatus().equals(Transaction.Status.ABORTING))
            rollbackToLSN(transNum, entry.lastLSN);
        // 记录写入日志
//        long newLSN = entry.lastLSN;
//        LogRecord record = logManager.fetchLogRecord(entry.lastLSN);
//        while(record != null){
//            record = logManager.fetchLogRecord(newLSN);
//            newLSN = record.LSN;
//        }
        EndTransactionLogRecord rollback = new EndTransactionLogRecord(transNum, logManager.getFlushedLSN() + 1);
        long LSN = logManager.appendToLog(rollback);
        // 更新lastLSN
        entry.lastLSN = LSN;
        entry.transaction.setStatus(Transaction.Status.COMPLETE);
        // 删除事务表中的事务
        transactionTable.remove(transNum);
        return LSN;
    }

    /**
     * Recommended helper function: performs a rollback of all of a
     * transaction's actions, up to (but not including) a certain LSN.
     * Starting with the LSN of the most recent record that hasn't been undone:
     * - while the current LSN is greater than the LSN we're rolling back to:
     *    - if the record at the current LSN is undoable:
     *       - Get a compensation log record (CLR) by calling undo on the record
     *       - Append the CLR
     *       - Call redo on the CLR to perform the undo
     *    - update the current LSN to that of the next record to undo
     *
     * Note above that calling .undo() on a record does not perform the undo, it
     * just creates the compensation log record.
     *
     * @param transNum transaction to perform a rollback for
     * @param LSN LSN to which we should rollback
     */
    private void rollbackToLSN(long transNum, long LSN) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        LogRecord lastRecord = logManager.fetchLogRecord(transactionEntry.lastLSN);
        long lastRecordLSN = lastRecord.getLSN();
        // Small optimization: if the last record is a CLR we can start rolling
        // back from the next record that hasn't yet been undone.
        long currentLSN = lastRecord.getUndoNextLSN().orElse(lastRecordLSN);
        // TODO(proj5) implement the rollback logic described above
        if(lastRecord.type.equals(LogType.UNDO_UPDATE_PAGE) || lastRecord.type.equals(LogType.UNDO_ALLOC_PAGE) || lastRecord.type.equals(LogType.UNDO_ALLOC_PART) || lastRecord.type.equals(LogType.UNDO_FREE_PAGE) || lastRecord.type.equals(LogType.UNDO_FREE_PART))
            return;
        LogRecord currentRecord = logManager.fetchLogRecord(currentLSN);
        long min = 0;
        if(lastRecordLSN == LSN)
            min = 0;
        else
            min =LSN;
        long rollbackLSN = lastRecordLSN;
        while(rollbackLSN > min) {
            // 判断是否可以undo
            if (currentRecord.isUndoable()) {
                // 对记录调用undo来获取补偿日志记录（CLR）
                LogRecord clr = currentRecord.undo(currentLSN);
                logManager.appendToLog(clr);
                clr.redo(this, diskSpaceManager, bufferManager);
                currentLSN = clr.getLSN();
            }
            rollbackLSN = currentRecord.getPrevLSN().get();
            currentRecord = logManager.fetchLogRecord(rollbackLSN);
        }
//        Math.max(lastLSN, logManager.maxLSN(logManager.getLSNPage(lastLSN)))
        flushToLSN(logManager.getFlushedLSN());
    }

    /**
     * Called before a page is flushed from the buffer cache. This
     * method is never called on a log page.
     *
     * The log should be as far as necessary.
     *
     * @param pageLSN pageLSN of page about to be flushed
     */
    @Override
    public void pageFlushHook(long pageLSN) {
        logManager.flushToLSN(pageLSN);
    }

    /**
     * Called when a page has been updated on disk.
     *
     * As the page is no longer dirty, it should be removed from the
     * dirty page table.
     *
     * @param pageNum page number of page updated on disk
     */
    @Override
    public void diskIOHook(long pageNum) {
        if (redoComplete) dirtyPageTable.remove(pageNum);
    }

    /**
     * Called when a write to a page happens.
     *
     * This method is never called on a log page. Arguments to the before and after params
     * are guaranteed to be the same length.
     *
     * The appropriate log record should be appended, and the transaction table
     * and dirty page table should be updated accordingly.
     *
     * @param transNum transaction performing the write
     * @param pageNum page number of page being written
     * @param pageOffset offset into page where write begins
     * @param before bytes starting at pageOffset before the write
     * @param after bytes starting at pageOffset after the write
     * @return LSN of last record written to log
     */
    @Override
    public long logPageWrite(long transNum, long pageNum, short pageOffset, byte[] before,
                             byte[] after) {
        assert (before.length == after.length);
        assert (before.length <= BufferManager.EFFECTIVE_PAGE_SIZE / 2);
        // TODO(proj5): implement
        // 增加日志记录，并相应地更新事务表和脏页表
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new UpdatePageLogRecord(transNum, pageNum, prevLSN, pageOffset, before, after);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        dirtyPage(pageNum, LSN);
//        flushToLSN(LSN - 1);
        return LSN;
    }

    /**
     * Called when a new partition is allocated. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the partition is the log partition.
     *
     * The appropriate log record should be appended, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the allocation
     * @param partNum partition number of the new partition
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logAllocPart(long transNum, int partNum) {
        // Ignore if part of the log.
        if (partNum == 0) return -1L;
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new AllocPartLogRecord(transNum, partNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a partition is freed. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the partition is the log partition.
     *
     * The appropriate log record should be appended, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the partition be freed
     * @param partNum partition number of the partition being freed
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logFreePart(long transNum, int partNum) {
        // Ignore if part of the log.
        if (partNum == 0) return -1L;

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new FreePartLogRecord(transNum, partNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a new page is allocated. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the page is in the log partition.
     *
     * The appropriate log record should be appended, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the allocation
     * @param pageNum page number of the new page
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logAllocPage(long transNum, long pageNum) {
        // Ignore if part of the log.
        if (DiskSpaceManager.getPartNum(pageNum) == 0) return -1L;

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new AllocPageLogRecord(transNum, pageNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a page is freed. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the page is in the log partition.
     *
     * The appropriate log record should be appended, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the page be freed
     * @param pageNum page number of the page being freed
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logFreePage(long transNum, long pageNum) {
        // Ignore if part of the log.
        if (DiskSpaceManager.getPartNum(pageNum) == 0) return -1L;

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new FreePageLogRecord(transNum, pageNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        dirtyPageTable.remove(pageNum);
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Creates a savepoint for a transaction. Creating a savepoint with
     * the same name as an existing savepoint for the transaction should
     * delete the old savepoint.
     *
     * The appropriate LSN should be recorded so that a partial rollback
     * is possible later.
     *
     * @param transNum transaction to make savepoint for
     * @param name name of savepoint
     */
    @Override
    public void savepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);
        transactionEntry.addSavepoint(name);
    }

    /**
     * Releases (deletes) a savepoint for a transaction.
     * @param transNum transaction to delete savepoint for
     * @param name name of savepoint
     */
    @Override
    public void releaseSavepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);
        transactionEntry.deleteSavepoint(name);
    }

    /**
     * Rolls back transaction to a savepoint.
     *
     * All changes done by the transaction since the savepoint should be undone,
     * in reverse order, with the appropriate CLRs written to log. The transaction
     * status should remain unchanged.
     *
     * @param transNum transaction to partially rollback
     * @param name name of savepoint
     */
    @Override
    public void rollbackToSavepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        // All of the transaction's changes strictly after the record at LSN should be undone.
        long savepointLSN = transactionEntry.getSavepoint(name);

        // TODO(proj5): implement
        rollbackToLSN(transNum, savepointLSN);
        return;
    }

    /**
     * Create a checkpoint.
     *
     * First, a begin checkpoint record should be written.
     *
     * Then, end checkpoint records should be filled up as much as possible first
     * using recLSNs from the DPT, then status/lastLSNs from the transactions
     * table, and written when full (or when nothing is left to be written).
     * You may find the method EndCheckpointLogRecord#fitsInOneRecord here to
     * figure out when to write an end checkpoint record.
     *
     * Finally, the master record should be rewritten with the LSN of the
     * begin checkpoint record.
     */
    @Override
    public synchronized void checkpoint() {
        // Create begin checkpoint log record and write to log
        LogRecord beginRecord = new BeginCheckpointLogRecord();
        long beginLSN = logManager.appendToLog(beginRecord);

        Map<Long, Long> chkptDPT = new HashMap<>();
        Map<Long, Pair<Transaction.Status, Long>> chkptTxnTable = new HashMap<>();

        // TODO(proj5): generate end checkpoint record(s) for DPT and transaction table
        // 循环访问DPT并复制条目，如果当前记录过多，生成一个检查点，追加记录到日志中
        for(Map.Entry<Long, Long> entry : dirtyPageTable.entrySet()){
            if(!EndCheckpointLogRecord.fitsInOneRecord(chkptDPT.size() + 1, 0)) {
                LogRecord endRecord = new EndCheckpointLogRecord(chkptDPT, chkptTxnTable);
                logManager.appendToLog(endRecord);
                chkptDPT = new HashMap<>();
            }
            chkptDPT.put(entry.getKey(), entry.getValue());
        }

        // 遍历事务表，复制status以及lastLSN，尽在需要时输出检查点记录
        for (Map.Entry<Long, TransactionTableEntry> entry : transactionTable.entrySet()) {
            if (!EndCheckpointLogRecord.fitsInOneRecord(chkptDPT.size(), chkptTxnTable.size() + 1)) {
                LogRecord endRecord = new EndCheckpointLogRecord(chkptDPT, chkptTxnTable);
                logManager.appendToLog(endRecord);
                chkptTxnTable = new HashMap<>();
                if (!chkptDPT.isEmpty())
                    chkptDPT = new HashMap<>();
            }
            chkptTxnTable.put(entry.getKey(), new Pair<>(entry.getValue().transaction.getStatus(), entry.getValue().lastLSN));
        }
        // Last end checkpoint record
        LogRecord endRecord = new EndCheckpointLogRecord(chkptDPT, chkptTxnTable);
        logManager.appendToLog(endRecord);
        // Ensure checkpoint is fully flushed before updating the master record
        flushToLSN(endRecord.getLSN());

        // Update master record
        MasterLogRecord masterRecord = new MasterLogRecord(beginLSN);
        logManager.rewriteMasterRecord(masterRecord);
    }

    /**
     * Flushes the log to at least the specified record,
     * essentially flushing up to and including the page
     * that contains the record specified by the LSN.
     *
     * @param LSN LSN up to which the log should be flushed
     */
    @Override
    public void flushToLSN(long LSN) {
        this.logManager.flushToLSN(LSN);
    }

    @Override
    public void dirtyPage(long pageNum, long LSN) {
        dirtyPageTable.putIfAbsent(pageNum, LSN);
        // Handle race condition where earlier log is beaten to the insertion by
        // a later log.
        dirtyPageTable.computeIfPresent(pageNum, (k, v) -> Math.min(LSN,v));
    }

    @Override
    public void close() {
        this.checkpoint();
        this.logManager.close();
    }

    // Restart Recovery ////////////////////////////////////////////////////////

    /**
     * Called whenever the database starts up, and performs restart recovery.
     * Recovery is complete when the Runnable returned is run to termination.
     * New transactions may be started once this method returns.
     *
     * This should perform the three phases of recovery, and also clean the
     * dirty page table of non-dirty pages (pages that aren't dirty in the
     * buffer manager) between redo and undo, and perform a checkpoint after
     * undo.
     */
    @Override
    public void restart() {
        this.restartAnalysis();
        this.restartRedo();
        this.redoComplete = true;
        this.cleanDPT();
        this.restartUndo();
        this.checkpoint();
    }

    /**
     * This method performs the analysis pass of restart recovery.
     *
     * First, the master record should be read (LSN 0). The master record contains
     * one piece of information: the LSN of the last successful checkpoint.
     *
     * We then begin scanning log records, starting at the beginning of the
     * last successful checkpoint.
     *
     * If the log record is for a transaction operation (getTransNum is present)
     * - update the transaction table
     *
     * If the log record is page-related (getPageNum is present), update the dpt
     *   - update/undoupdate page will dirty pages
     *   - free/undoalloc page always flush changes to disk
     *   - no action needed for alloc/undofree page
     *
     * If the log record is for a change in transaction status:
     * - update transaction status to COMMITTING/RECOVERY_ABORTING/COMPLETE
     * - update the transaction table
     * - if END_TRANSACTION: clean up transaction (Transaction#cleanup), remove
     *   from txn table, and add to endedTransactions
     *
     * If the log record is an end_checkpoint record:
     * - Copy all entries of checkpoint DPT (replace existing entries if any)
     * - Skip txn table entries for transactions that have already ended
     * - Add to transaction table if not already present
     * - Update lastLSN to be the larger of the existing entry's (if any) and
     *   the checkpoint's
     * - The status's in the transaction table should be updated if it is possible
     *   to transition from the status in the table to the status in the
     *   checkpoint. For example, running -> aborting is a possible transition,
     *   but aborting -> running is not.
     *
     * After all records in the log are processed, for each ttable entry:
     *  - if COMMITTING: clean up the transaction, change status to COMPLETE,
     *    remove from the ttable, and append an end record
     *  - if RUNNING: change status to RECOVERY_ABORTING, and append an abort
     *    record
     *  - if RECOVERY_ABORTING: no action needed
     */
    void restartAnalysis() {
        // Read master record
        LogRecord record = logManager.fetchLogRecord(0L);
        // Type checking
        assert (record != null && record.getType() == LogType.MASTER);
        MasterLogRecord masterRecord = (MasterLogRecord) record;
        // Get start checkpoint LSN
        long LSN = masterRecord.lastCheckpointLSN;
        // Set of transactions that have completed
        Set<Long> endedTransactions = new HashSet<>();
        // TODO(proj5): implement
        Iterator<LogRecord> iterator = logManager.scanFrom(LSN);
        while(iterator.hasNext()){
            LogRecord preRecord = iterator.next();
            Long lsn = preRecord.LSN;
            if(preRecord.getTransNum().isPresent()){
                long transNum = preRecord.getTransNum().get();
                if(!transactionTable.containsKey(transNum)) {
                    Transaction newTransaction1 = newTransaction.apply(transNum);
                    startTransaction(newTransaction1);
                    transactionTable.put(transNum, new TransactionTableEntry(newTransaction1));
                }
                transactionTable.get(transNum).lastLSN = preRecord.getLSN();
            }
            if(preRecord.getPageNum().isPresent()){
                long pageNum = preRecord.getPageNum().get();
                if(preRecord.type.equals(LogType.UPDATE_PAGE) || preRecord.type.equals(LogType.UNDO_UPDATE_PAGE)){
                    dirtyPage(pageNum, lsn);
                } else if(preRecord.type.equals(LogType.FREE_PAGE) || preRecord.type.equals(LogType.UNDO_ALLOC_PAGE)) {
                    dirtyPageTable.remove(pageNum);
                }
            }
            if(preRecord.type.toString().endsWith("_TRANSACTION")){
                Long transNum = preRecord.getTransNum().get();
                // preRecord.type.equals(LogType.COMMIT_TRANSACTION) || preRecord.type.equals(LogType.ABORT_TRANSACTION) || preRecord.type.equals(LogType.END_TRANSACTION)
                if(preRecord.type.equals(LogType.COMMIT_TRANSACTION))
                    transactionTable.get(transNum).transaction.setStatus(Transaction.Status.COMMITTING);
                else if(preRecord.type.equals(LogType.ABORT_TRANSACTION))
                    transactionTable.get(transNum).transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
                else {
                    TransactionTableEntry entry = transactionTable.get(transNum);
                    entry.transaction.cleanup();
                    entry.transaction.setStatus(Transaction.Status.COMPLETE);
                    transactionTable.remove(transNum);
                    endedTransactions.add(transNum);
                }
            }
            if(preRecord.type.equals(LogType.END_CHECKPOINT)){
                Map<Long, Long> chkptDPT = preRecord.getDirtyPageTable();
                Map<Long, Pair<Transaction.Status, Long>> chkptTxnTable = preRecord.getTransactionTable();
                for(Map.Entry<Long, Long> entry : chkptDPT.entrySet()){
                    dirtyPageTable.put(entry.getKey(), entry.getValue());
                }
                for(Map.Entry<Long, Pair<Transaction.Status, Long>> entry : chkptTxnTable.entrySet()){
                    if(endedTransactions.contains(entry.getKey()))
                        continue;
                    if(!transactionTable.containsKey(entry.getKey())) {
                        Transaction transaction = newTransaction.apply(entry.getKey());
                        startTransaction(transaction);
                        transactionTable.put(entry.getKey(), new TransactionTableEntry(transaction));
                    }
                    Pair<Transaction.Status, Long> value = entry.getValue();
                    TransactionTableEntry changeTrans = transactionTable.get(entry.getKey());
                    if(value.getSecond().compareTo(changeTrans.lastLSN) > 0)
                        changeTrans.lastLSN = value.getSecond();
                    Transaction.Status first = value.getFirst();
                    Transaction.Status status = changeTrans.transaction.getStatus();
                    if(status.equals(Transaction.Status.RUNNING) && first.equals(Transaction.Status.ABORTING)){
                        changeTrans.transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
                    }else if(status.equals(Transaction.Status.RUNNING) && first.equals(Transaction.Status.COMMITTING)){
                        changeTrans.transaction.setStatus(Transaction.Status.COMMITTING);
                    }else if(first.equals(Transaction.Status.COMPLETE) || status.equals(Transaction.Status.COMPLETE)){
                        // (status.equals(Transaction.Status.COMMITTING) || status.equals(Transaction.Status.ABORTING))&&
                        // 貌似不需要做判断，因为这已经是最终状态了
                        changeTrans.transaction.cleanup();
                        changeTrans.transaction.setStatus(Transaction.Status.COMPLETE);
                        transactionTable.remove(entry.getKey());
                        endedTransactions.add(entry.getKey());
                    }
                }

            }
        }
        for(Map.Entry<Long, TransactionTableEntry> entry : transactionTable.entrySet()){
            Transaction transaction = entry.getValue().transaction;
            if(transaction.getStatus().equals(Transaction.Status.COMMITTING)) {
                transaction.cleanup();
                transaction.setStatus(Transaction.Status.COMPLETE);
                transactionTable.remove(entry.getKey());
                endedTransactions.add(entry.getKey());
                EndTransactionLogRecord endTransactionLogRecord = new EndTransactionLogRecord(transaction.getTransNum(), entry.getValue().lastLSN);
                logManager.appendToLog(endTransactionLogRecord);
            }else if(transaction.getStatus().equals(Transaction.Status.RUNNING)) {
                transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
                AbortTransactionLogRecord abortTransactionLogRecord = new AbortTransactionLogRecord(transaction.getTransNum(), entry.getValue().lastLSN);
                long lsn = logManager.appendToLog(abortTransactionLogRecord);
                entry.getValue().lastLSN = lsn;
            }else if (transaction.getStatus().equals(Transaction.Status.COMPLETE)) {
                transaction.cleanup();
                transactionTable.remove(entry.getKey());
                endedTransactions.add(entry.getKey());
            }
        }
//        logManager.flushToLSN(LSN);
        return;
    }

    /**
     * This method performs the redo pass of restart recovery.
     *
     * First, determine the starting point for REDO from the dirty page table.
     *
     * Then, scanning from the starting point, if the record is redoable and
     * - partition-related (Alloc/Free/UndoAlloc/UndoFree..Part), always redo it
     * - allocates a page (AllocPage/UndoFreePage), always redo it
     * - modifies a page (Update/UndoUpdate/Free/UndoAlloc....Page) in
     *   the dirty page table with LSN >= recLSN, the page is fetched from disk,
     *   the pageLSN is checked, and the record is redone if needed.
     */
    void restartRedo() {
        // TODO(proj5): implement
        long recLSN = 99999;
        for(Map.Entry<Long, Long> entry : dirtyPageTable.entrySet()){
            if(recLSN > entry.getValue())
                recLSN = entry.getValue();
        }
        Iterator<LogRecord> iterator = logManager.scanFrom(recLSN);
        while (iterator.hasNext()) {
            LogRecord preRecord = iterator.next();
            if(!preRecord.isRedoable())
                continue;
            if (preRecord.type.toString().endsWith("PART")) {
                preRecord.redo(this, diskSpaceManager, bufferManager);
            }else if(preRecord.type.equals(LogType.ALLOC_PAGE) || preRecord.type.equals(LogType.UNDO_FREE_PAGE)){
                preRecord.redo(this, diskSpaceManager, bufferManager);
            }else if(preRecord.type.toString().endsWith("PAGE")){
                long pageNum = preRecord.getPageNum().get();
                if(!dirtyPageTable.containsKey(pageNum))
                    continue;
                if(preRecord.LSN < dirtyPageTable.get(pageNum))
                    continue;
                Page page = bufferManager.fetchPage(new DummyLockContext(), pageNum);
                try {
                    long pageLSN = page.getPageLSN();
                    if(pageLSN < preRecord.LSN)
                        preRecord.redo(this, diskSpaceManager, bufferManager);
                } finally {
                    page.unpin();
                }
            }
        }
        return;
    }

    /**
     * This method performs the undo pass of restart recovery.

     * First, a priority queue is created sorted on lastLSN of all aborting
     * transactions.
     *
     * Then, always working on the largest LSN in the priority queue until we are done,
     * - if the record is undoable, undo it, and append the appropriate CLR
     * - replace the entry with a new one, using the undoNextLSN if available,
     *   if the prevLSN otherwise.
     * - if the new LSN is 0, clean up the transaction, set the status to complete,
     *   and remove from transaction table.
     */
    void restartUndo() {
        // TODO(proj5): implement
        PriorityQueue<Pair<Long, TransactionTableEntry>> priorityQueue = new PriorityQueue<>(20, new PairFirstReverseComparator());
        for (Map.Entry<Long, TransactionTableEntry> entry : transactionTable.entrySet()) {
            if (!entry.getValue().transaction.getStatus().equals(Transaction.Status.RECOVERY_ABORTING))
                continue;
            long lastLSN = entry.getValue().lastLSN;
            priorityQueue.add(new Pair<>(lastLSN, entry.getValue()));
        }
        while (!priorityQueue.isEmpty()) {
            Pair<Long, TransactionTableEntry> pair= priorityQueue.poll();
            long peek = pair.getFirst();
            LogRecord logRecord = logManager.fetchLogRecord(peek);
            if (logRecord.isUndoable()) {
                TransactionTableEntry entry = pair.getSecond();
                LogRecord clr = logRecord.undo(entry.lastLSN);
                long lsn = logManager.appendToLog(clr);
                entry.lastLSN = lsn;
                clr.redo(this, diskSpaceManager, bufferManager);
            }
            Long newL = 0l;
            if(logRecord.getUndoNextLSN().isPresent()) {
                // 如果上一个LSN不可用，则使用undoNextLSN（如果可用）将条目替换为新条目
                newL = logRecord.getUndoNextLSN().get();
            }else
                newL = logRecord.getPrevLSN().get();
            if(newL == 0l){
                TransactionTableEntry entry = pair.getSecond();
                entry.transaction.cleanup();
                entry.transaction.setStatus(Transaction.Status.COMPLETE);
                transactionTable.remove(entry.transaction.getTransNum());
                EndTransactionLogRecord endTransactionLogRecord = new EndTransactionLogRecord(entry.transaction.getTransNum(), entry.lastLSN);
                logManager.appendToLog(endTransactionLogRecord);
            }else
                priorityQueue.add(new Pair<>(newL, pair.getSecond()));
        }
        return;
    }

    /**
     * Removes pages from the DPT that are not dirty in the buffer manager.
     * This is slow and should only be used during recovery.
     */
    void cleanDPT() {
        Set<Long> dirtyPages = new HashSet<>();
        bufferManager.iterPageNums((pageNum, dirty) -> {
            if (dirty) dirtyPages.add(pageNum);
        });
        Map<Long, Long> oldDPT = new HashMap<>(dirtyPageTable);
        dirtyPageTable.clear();
        for (long pageNum : dirtyPages) {
            if (oldDPT.containsKey(pageNum)) {
                dirtyPageTable.put(pageNum, oldDPT.get(pageNum));
            }
        }
    }

    // Helpers /////////////////////////////////////////////////////////////////
    /**
     * Comparator for Pair<A, B> comparing only on the first element (type A),
     * in reverse order.
     */
    private static class PairFirstReverseComparator<A extends Comparable<A>, B> implements
            Comparator<Pair<A, B>> {
        @Override
        public int compare(Pair<A, B> p0, Pair<A, B> p1) {
            return p1.getFirst().compareTo(p0.getFirst());
        }
    }
}
