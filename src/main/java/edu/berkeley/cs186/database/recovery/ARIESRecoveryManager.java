package edu.berkeley.cs186.database.recovery;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.Transaction.Status;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.concurrency.LockContext;
import edu.berkeley.cs186.database.concurrency.LockType;
import edu.berkeley.cs186.database.concurrency.LockUtil;
import edu.berkeley.cs186.database.io.DiskSpaceManager;
import edu.berkeley.cs186.database.memory.BufferManager;
import edu.berkeley.cs186.database.memory.Page;
import java.util.stream.Collectors; 
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Implementation of ARIES.
 */
public class ARIESRecoveryManager implements RecoveryManager {
    // Lock context of the entire database.
    private LockContext dbContext;
    // Disk space manager.
    DiskSpaceManager diskSpaceManager;
    // Buffer manager.
    BufferManager bufferManager;

    // Function to create a new transaction for recovery with a given transaction number.
    private Function<Long, Transaction> newTransaction;
    // Function to update the transaction counter.
    protected Consumer<Long> updateTransactionCounter;
    // Function to get the transaction counter.
    protected Supplier<Long> getTransactionCounter;

    // Log manager
    LogManager logManager;
    // Dirty page table (page number -> recLSN).
    Map<Long, Long> dirtyPageTable = new ConcurrentHashMap<>();
    // Transaction table (transaction number -> entry).
    Map<Long, TransactionTableEntry> transactionTable = new ConcurrentHashMap<>();

    // List of lock requests made during recovery. This is only populated when locking is disabled.
    List<String> lockRequests;

    public ARIESRecoveryManager(LockContext dbContext, Function<Long, Transaction> newTransaction,
                                Consumer<Long> updateTransactionCounter, Supplier<Long> getTransactionCounter) {
        this(dbContext, newTransaction, updateTransactionCounter, getTransactionCounter, false);
    }

    ARIESRecoveryManager(LockContext dbContext, Function<Long, Transaction> newTransaction,
                         Consumer<Long> updateTransactionCounter, Supplier<Long> getTransactionCounter,
                         boolean disableLocking) {
        this.dbContext = dbContext;
        this.newTransaction = newTransaction;
        this.updateTransactionCounter = updateTransactionCounter;
        this.getTransactionCounter = getTransactionCounter;
        this.lockRequests = disableLocking ? new ArrayList<>() : null;
    }

    /**
     * Initializes the log; only called the first time the database is set up.
     *
     * The master record should be added to the log, and a checkpoint should be taken.
     */
    @Override
    public void initialize() {
        this.logManager.appendToLog(new MasterLogRecord(0));
        this.checkpoint();
    }

    /**
     * Sets the buffer/disk managers. This is not part of the constructor because of the cyclic dependency
     * between the buffer manager and recovery manager (the buffer manager must interface with the
     * recovery manager to block page evictions until the log has been flushed, but the recovery
     * manager needs to interface with the buffer manager to write the log and redo changes).
     * @param diskSpaceManager disk space manager
     * @param bufferManager buffer manager
     */
    @Override
    public void setManagers(DiskSpaceManager diskSpaceManager, BufferManager bufferManager) {
        this.diskSpaceManager = diskSpaceManager;
        this.bufferManager = bufferManager;
        this.logManager = new LogManagerImpl(bufferManager);
    }

    // Forward Processing ////////////////////////////////////////////////////////////////////

    /**
     * Transaction status logic
     * There is an internal state kept in Transaction trait
     * When transaction is created, the status is running
     * Then user will either call commit() or rollback()
     * Take commit() as an example:
     *   Transaction.commit() -> AbstractTransaction.startCommit() -> RecoveryManager.commit() -> (TransactionImpl.cleanUp() -> RecoveryManage.end())
     */

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
     * A commit record should be emitted, the log should be flushed,
     * and the transaction table and the transaction status should be updated.
     *
     * @param transNum transaction being committed
     * @return LSN of the commit record
     */
    @Override
    public long commit(long transNum) {
        // TODO(proj5): implement
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new CommitTransactionLogRecord(transNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        transactionEntry.lastLSN = LSN;
        logManager.flushToLSN(LSN);
        transactionEntry.transaction.setStatus(Status.COMMITTING);
        return LSN;
    }

    /**
     * Called when a transaction is set to be aborted.
     *
     * An abort record should be emitted, and the transaction table and transaction
     * status should be updated. No CLRs should be emitted.
     *
     * @param transNum transaction being aborted
     * @return LSN of the abort record
     */
    @Override
    public long abort(long transNum) {
        // TODO(proj5): implement
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new AbortTransactionLogRecord(transNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        transactionEntry.lastLSN = LSN;
        // logManager.flushToLSN(LSN); // Looks like abort and end do not require log to be flushed
        transactionEntry.transaction.setStatus(Status.ABORTING);
        return LSN;
    }

    /**
     * Called when a transaction is cleaning up; this should roll back
     * changes if the transaction is aborting.
     *
     * Any changes that need to be undone should be undone, the transaction should
     * be removed from the transaction table, the end record should be emitted,
     * and the transaction status should be updated.
     *
     * @param transNum transaction to end
     * @return LSN of the end record
     */
    @Override
    public long end(long transNum) {
        // TODO(proj5): implement
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);
        Transaction transaction = transactionEntry.transaction;

        if (transaction.getStatus() == Status.ABORTING) {
            // we need to undo 
            rollbackToRecord(transactionEntry, 0l);
        }
        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new EndTransactionLogRecord(transNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        this.transactionTable.remove(transNum);
        transaction.setStatus(Status.COMPLETE);
        return LSN;
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
        dirtyPageTable.remove(pageNum);
    }

    /**
     * Called when a write to a page happens.
     *
     * This method is never called on a log page. Arguments to the before and after params
     * are guaranteed to be the same length.
     *
     * The appropriate log record should be emitted; if the number of bytes written is
     * too large (larger than BufferManager.EFFECTIVE_PAGE_SIZE / 2), then two records
     * should be written instead: an undo-only record followed by a redo-only record.
     * 
     * If we need to split the update log into 2, we write 2 UpdatePageLogRecord instead of
     * 1 UndoUpdatePageLogRecord and 1 UpdatePageLogRecord. The first log only contains before 
     * field, so it is undo only and the second only contains after field so it is redo only.
     * 
     * For a simple example.
     * 
     * 1 - 2 - 3_1(U) - 3_2(R) - 4
     * 
     * If we want to abort, it becomes
     * 
     * 1 - 2 - 3_1(U) - 3_2(R) - 4 - ABORT - U4
     * 
     * Now the lastLSN points to 3_2(R),which is not undoable, so we skip
     * this one and go to 3_1(U) which is undoable, so we undo it, and the log becomes
     * 
     * 1 - 2 - 3_1(U) - 3_2(R) - 4 - ABORT - U4 - U3_1 - U2 - U1
     *
     * Both the transaction table and dirty page table should be updated accordingly.
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

        // TODO(proj5): implement                       
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert transactionEntry != null;
        
        long prevLSN = transactionEntry.lastLSN;
        if (before.length > BufferManager.EFFECTIVE_PAGE_SIZE / 2) {
            LogRecord undoRecord = new UpdatePageLogRecord(transNum, pageNum, prevLSN, pageOffset, before, new byte[0]);
            prevLSN = logManager.appendToLog(undoRecord);
            LogRecord redoRecord = new UpdatePageLogRecord(transNum, pageNum, prevLSN, pageOffset, new byte[0], after);
            prevLSN = logManager.appendToLog(redoRecord);
        } else {
            LogRecord updateRecord = new UpdatePageLogRecord(transNum, pageNum, prevLSN, pageOffset, before, after);
            prevLSN = logManager.appendToLog(updateRecord);
        }
        // update transaction entry and dirty page table
        transactionEntry.lastLSN = prevLSN;
        transactionEntry.touchedPages.add(pageNum);
        updateDirtyPageTable(pageNum, prevLSN);
        return prevLSN;
    }

    /**
     * Called when a new partition is allocated. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the partition is the log partition.
     *
     * The appropriate log record should be emitted, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the allocation
     * @param partNum partition number of the new partition
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logAllocPart(long transNum, int partNum) {
        // Ignore if part of the log.
        if (partNum == 0) {
            return -1L;
        }

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
     * The appropriate log record should be emitted, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the partition be freed
     * @param partNum partition number of the partition being freed
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logFreePart(long transNum, int partNum) {
        // Ignore if part of the log.
        if (partNum == 0) {
            return -1L;
        }

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
     * The appropriate log record should be emitted, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the allocation
     * @param pageNum page number of the new page
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logAllocPage(long transNum, long pageNum) {
        // Ignore if part of the log.
        if (DiskSpaceManager.getPartNum(pageNum) == 0) {
            return -1L;
        }

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new AllocPageLogRecord(transNum, pageNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN, touchedPages
        transactionEntry.lastLSN = LSN;
        transactionEntry.touchedPages.add(pageNum);
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
     * The appropriate log record should be emitted, and the log flushed.
     * The transaction table should be updated accordingly.
     * 
     * Only in this method should we remove entries from dirty page table
     * beacuase this method is called when buffer page gets flushed to disk,
     * resulting in this page no longer dirty.
     *
     * @param transNum transaction requesting the page be freed
     * @param pageNum page number of the page being freed
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logFreePage(long transNum, long pageNum) {
        // Ignore if part of the log.
        if (DiskSpaceManager.getPartNum(pageNum) == 0) {
            return -1L;
        }

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new FreePageLogRecord(transNum, pageNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN, touchedPages
        transactionEntry.lastLSN = LSN;
        transactionEntry.touchedPages.add(pageNum);
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
        long LSN = transactionEntry.getSavepoint(name);

        // TODO(proj5): implement
        rollbackToRecord(transactionEntry, LSN);
        return;
    }

    /**
     * Create a checkpoint.
     *
     * First, a begin checkpoint record should be written.
     *
     * Then, end checkpoint records should be filled up as much as possible,
     * using recLSNs from the DPT, then status/lastLSNs from the transactions table,
     * and then finally, touchedPages from the transactions table, and written
     * when full (or when done).
     *
     * Finally, the master record should be rewritten with the LSN of the
     * begin checkpoint record.
     */
    @Override
    public void checkpoint() {
        // Create begin checkpoint log record and write to log
        LogRecord beginRecord = new BeginCheckpointLogRecord(getTransactionCounter.get());
        long beginLSN = logManager.appendToLog(beginRecord);

        Map<Long, Long> dpt = new HashMap<>();
        Map<Long, Pair<Transaction.Status, Long>> txnTable = new HashMap<>();
        Map<Long, List<Long>> touchedPages = new HashMap<>();
        // we only need this field when we are adding touched pages, and this is the last step
        // so this field is always 0 when we are adding dirty pages and transactions
        // so we can keep this field as an internal state inside checkpointAddTouchedPages()
        // int numTouchedPages = 0; 

        // TODO(proj5): generate end checkpoint record(s) for DPT and transaction table
        checkpointAddDirtyPages(dpt, txnTable, touchedPages);
        checkpointAddTransactions(dpt, txnTable, touchedPages);
        checkpointAddTouchedPages(dpt, txnTable, touchedPages);

        // Last end checkpoint record
        LogRecord endRecord = new EndCheckpointLogRecord(dpt, txnTable, touchedPages);
        logManager.appendToLog(endRecord);

        // Update master record
        MasterLogRecord masterRecord = new MasterLogRecord(beginLSN);
        logManager.rewriteMasterRecord(masterRecord);
    }

    // TODO(proj5): add any helper methods needed

    // steps: 
    // 1. add dirty pages, no transactions and touched pages will be added so we just leave them as is
    // 2. add transactions, the remainder of dirty pages will also be added to the first record, not touched pages will be added
    // 3. add touched pages, the remainder of dirty pages (if there are any) and transactions will also be added to the first record

    private void checkpointAddTouchedPages(Map<Long, Long> dpt, Map<Long, Pair<Transaction.Status, Long>> txnTable, Map<Long, List<Long>> touchedPages) {
        int numTouchedPages = 0;
        for (Map.Entry<Long, TransactionTableEntry> entry : transactionTable.entrySet()) {
            long transNum = entry.getKey();
            for (long pageNum : entry.getValue().touchedPages) {
                boolean fitsAfterAdd;
                if (!touchedPages.containsKey(transNum)) {
                    fitsAfterAdd = EndCheckpointLogRecord.fitsInOneRecord(
                                       dpt.size(), txnTable.size(), touchedPages.size() + 1, numTouchedPages + 1);
                } else {
                    fitsAfterAdd = EndCheckpointLogRecord.fitsInOneRecord(
                                       dpt.size(), txnTable.size(), touchedPages.size(), numTouchedPages + 1);
                }

                if (!fitsAfterAdd) {
                    // everything is copied instead of using referrence so it's ok that we clear afterwards
                    LogRecord endRecord = new EndCheckpointLogRecord(dpt, txnTable, touchedPages);
                    logManager.appendToLog(endRecord);

                    dpt.clear();
                    txnTable.clear();
                    touchedPages.clear();
                    numTouchedPages = 0;
                }

                touchedPages.computeIfAbsent(transNum, t -> new ArrayList<>());
                touchedPages.get(transNum).add(pageNum);
                ++numTouchedPages;
            }
        }
    }

    private void checkpointAddTransactions(Map<Long, Long> dpt, Map<Long, Pair<Transaction.Status, Long>> txnTable, Map<Long, List<Long>> touchedPages) {
        for (Map.Entry<Long, TransactionTableEntry> entry : transactionTable.entrySet()) {
            long transNum = entry.getKey();
            boolean fitsAfterAdd = EndCheckpointLogRecord.fitsInOneRecord(
                                       dpt.size(), txnTable.size() + 1, touchedPages.size(), 0);
            if (!fitsAfterAdd) {
                LogRecord endRecord = new EndCheckpointLogRecord(dpt, txnTable, touchedPages);
                logManager.appendToLog(endRecord);

                dpt.clear();
                txnTable.clear();
            }
            TransactionTableEntry transactionTableEntry = entry.getValue();
            Pair<Transaction.Status, Long> transaction = new Pair<>(transactionTableEntry.transaction.getStatus(), transactionTableEntry.lastLSN);
            txnTable.put(transNum, transaction);
        }
    }

    private void checkpointAddDirtyPages(Map<Long, Long> dpt, Map<Long, Pair<Transaction.Status, Long>> txnTable, Map<Long, List<Long>> touchedPages) {
        for (Map.Entry<Long, Long> entry : dirtyPageTable.entrySet()) {
            boolean fitsAfterAdd = EndCheckpointLogRecord.fitsInOneRecord(
                                       dpt.size() + 1, txnTable.size(), touchedPages.size(), 0);
            if (!fitsAfterAdd) {
                LogRecord endRecord = new EndCheckpointLogRecord(dpt, txnTable, touchedPages);
                logManager.appendToLog(endRecord);

                dpt.clear();
            }
            dpt.put(entry.getKey(), entry.getValue());
        }
    }

    private void rollbackToRecord(TransactionTableEntry transactionEntry, long LSN) {
        long undoNextLSN = transactionEntry.lastLSN;
        while (undoNextLSN != LSN) {
            undoNextLSN = undoOneRecord(transactionEntry, undoNextLSN);
        }
    }

    private long undoOneRecord(TransactionTableEntry transactionEntry, long undoNextLSN) {
        LogRecord prevLog = logManager.fetchLogRecord(undoNextLSN);
        long lastLSN = transactionEntry.lastLSN;
        if (prevLog.isUndoable()) {
            Pair<LogRecord, Boolean> clrPair = prevLog.undo(lastLSN);
            LogRecord record = clrPair.getFirst();
            assert record.isRedoable();
            record.redo(diskSpaceManager, bufferManager);
            lastLSN = logManager.appendToLog(record);
            transactionEntry.lastLSN = lastLSN;
            // needs to update page table accordingly
            switch (record.type) {
                case UNDO_UPDATE_PAGE:
                    updateDirtyPageTable(record.getPageNum().get(), record.LSN);
                    break;
                case UNDO_ALLOC_PAGE:
                    dirtyPageTable.remove(record.getPageNum().get());
                    break;
                default:
                    break;
            }
            if (clrPair.getSecond()) {
                logManager.flushToLSN(lastLSN); // this clr log needs to be flushed immediately
            }
        }
        return prevLog.getUndoNextLSN().orElse(prevLog.getPrevLSN().orElse(0l));
    }

    private void updateDirtyPageTable(long pageNum, long recLSN) {
        dirtyPageTable.computeIfAbsent(pageNum, x -> recLSN);
        dirtyPageTable.computeIfPresent(pageNum, (x, LSN) -> LSN < recLSN ? LSN : recLSN);
    }

    @Override
    public void close() {
        this.checkpoint();
        this.logManager.close();
    }

    // Restart Recovery //////////////////////////////////////////////////////////////////////

    /**
     * Called whenever the database starts up, and performs restart recovery. Recovery is
     * complete when the Runnable returned is run to termination. New transactions may be
     * started once this method returns.
     *
     * This should perform the three phases of recovery, and also clean the dirty page
     * table of non-dirty pages (pages that aren't dirty in the buffer manager) between
     * redo and undo, and perform a checkpoint after undo.
     *
     * This method should return right before undo is performed.
     *
     * @return Runnable to run to finish restart recovery
     */
    @Override
    public Runnable restart() {
        // TODO(proj5): implement
        restartAnalysis();

        restartRedo();
        bufferManager.iterPageNums((pageNum, isDirty) -> {
            if (!isDirty)
                dirtyPageTable.remove(pageNum);
        });
        return () -> {restartUndo(); checkpoint();};
    }

    /**
     * This method performs the analysis pass of restart recovery.
     *
     * First, the master record should be read (LSN 0). The master record contains
     * one piece of information: the LSN of the last successful checkpoint.
     *
     * We then begin scanning log records, starting at the begin checkpoint record.
     *
     * If the log record is for a transaction operation:
     * - update the transaction table
     * - if it's page-related (as opposed to partition-related),
     *   - add to touchedPages
     *   - acquire X lock
     *   - update DPT (alloc/free/undoalloc/undofree always flushes changes to disk)
     *
     * If the log record is for a change in transaction status:
     * - clean up transaction (Transaction#cleanup) if END_TRANSACTION
     * - update transaction status to COMMITTING/RECOVERY_ABORTING/COMPLETE
     * - update the transaction table
     *
     * If the log record is a begin_checkpoint record:
     * - Update the transaction counter
     *
     * If the log record is an end_checkpoint record:
     * - Copy all entries of checkpoint DPT (replace existing entries if any)
     * - Update lastLSN to be the larger of the existing entry's (if any) and the checkpoint's;
     *   add to transaction table if not already present.
     * - Add page numbers from checkpoint's touchedPages to the touchedPages sets in the
     *   transaction table if the transaction has not finished yet, and acquire X locks.
     *
     * Then, cleanup and end transactions that are in the COMMITING state, and
     * move all transactions in the RUNNING state to RECOVERY_ABORTING.
     */
    void restartAnalysis() {
        assert (dirtyPageTable.size() == 0);
        assert (transactionTable.size() == 0);
        // Read master record
        LogRecord record = logManager.fetchLogRecord(0L);
        assert (record != null);
        // Type casting
        assert (record.getType() == LogType.MASTER);
        MasterLogRecord masterRecord = (MasterLogRecord) record;
        // Get start checkpoint LSN
        long LSN = masterRecord.lastCheckpointLSN;

        // TODO(proj5): implement
        Iterator<LogRecord> it = logManager.scanFrom(LSN);
        while(it.hasNext()) {
            LogRecord logRecord = it.next();
            processRecord(logRecord);
        }

        // needs to do the clean up afterwards since we cannot remove the entry from map
        // while we are iterating it
        List<Long> committingTransactions = new ArrayList<>();
        for (Entry<Long, TransactionTableEntry> entry : transactionTable.entrySet()) {
            long transNum = entry.getKey();
            TransactionTableEntry transactionEntry = entry.getValue();
            switch (transactionEntry.transaction.getStatus()) {
                case RUNNING:
                    abort(transNum);
                    transactionEntry.transaction.setStatus(Status.RECOVERY_ABORTING);
                    break;

                case COMMITTING:
                    committingTransactions.add(transNum);
                    break;

                case ABORTING:
                case COMPLETE:
                    throw new UnsupportedOperationException("should not arrive here"); 
                
                case RECOVERY_ABORTING:
                    // do nothing
                    break;

                default:
                    break;
            }
        }
        committingTransactions.stream().forEach(transNum -> {
            TransactionTableEntry transactionEntry = transactionTable.get(transNum);
            // the order of function calls should be fine.
            // since it's a recovery transaction, end() will not be called in cleanup()
            transactionEntry.transaction.cleanup();
            end(transNum);
        });
        
        return;
    }

    /**
     * This method performs the redo pass of restart recovery.
     *
     * First, determine the starting point for REDO from the DPT.
     *
     * Then, scanning from the starting point, if the record is redoable and
     * - about a page (Update/Alloc/Free/Undo..Page) in the DPT with LSN >= recLSN,
     *   the page is fetched from disk and the pageLSN is checked, and the record is redone.
     * - about a partition (Alloc/Free/Undo..Part), redo it.
     * 
     * In redo phase we don't need to update the dpt because in analysis phase we have already done that
     */
    void restartRedo() {
        // TODO(proj5): implement
        long LSN = dirtyPageTable.entrySet().stream().max(Comparator.comparing(Map.Entry::getValue)).get().getValue();
        Iterator<LogRecord> it = logManager.scanFrom(LSN);
        while(it.hasNext()) {
            LogRecord record = it.next();
            if (shouldRedo(record)) {
                record.redo(diskSpaceManager, bufferManager);
            }
        }
        return;
    }

    /**
     * This method performs the redo pass of restart recovery.

     * First, a priority queue is created sorted on lastLSN of all aborting transactions.
     *
     * Then, always working on the largest LSN in the priority queue until we are done,
     * - if the record is undoable, undo it, emit the appropriate CLR, and update tables accordingly;
     * - replace the entry in the set should be replaced with a new one, using the undoNextLSN
     *   (or prevLSN if none) of the record; and
     * - if the new LSN is 0, end the transaction and remove it from the queue and transaction table.
     */
    void restartUndo() {
        // TODO(proj5): implement
        Set<Pair<Long, TransactionTableEntry>> abortingTrans = transactionTable
            .entrySet()
            .stream()
            .map(Entry::getValue)
            .filter(x -> x.transaction.getStatus() == Status.RECOVERY_ABORTING)
            .map(x -> new Pair<Long, TransactionTableEntry>(x.lastLSN, x))
            .collect(Collectors.toSet());
        PriorityQueue<Pair<Long, TransactionTableEntry>> queue = new PriorityQueue<>(ARIESRecoveryManager.PairFirstReverseComparator);
        queue.addAll(abortingTrans);

        while (!queue.isEmpty()) {
            Pair<Long, TransactionTableEntry> pair = queue.poll();
            TransactionTableEntry transactionEntry = pair.getSecond();
            long undoNextLSN = pair.getFirst();
            undoNextLSN = undoOneRecord(transactionEntry, undoNextLSN);
            if (undoNextLSN == 0l) {
                transactionEntry.transaction.cleanup();
                end(transactionEntry.transaction.getTransNum());
            } else {
                Pair<Long, TransactionTableEntry> newPair = new Pair<>(undoNextLSN, transactionEntry);
                queue.add(newPair);
            }
        }
        return;
    }

    // TODO(proj5): add any helper methods needed

    // Helpers ///////////////////////////////////////////////////////////////////////////////

    Optional<TransactionTableEntry> updateTransactionTable(LogRecord logRecord) {
        return logRecord
            .getTransNum()
            .map(transNum -> {
                // we don't want to create the transaction if it already exists
                transactionTable.computeIfAbsent(transNum, newTransaction.andThen(TransactionTableEntry::new));
                TransactionTableEntry entry = transactionTable.get(transNum);
                entry.lastLSN = logRecord.getLSN();
                return entry;
            });
    }

    void processRecord(LogRecord logRecord) {
        Optional<TransactionTableEntry> transactionEntry = updateTransactionTable(logRecord);
        switch (logRecord.type) {
            case BEGIN_CHECKPOINT:
                // what's this for?
                logRecord.getMaxTransactionNum().ifPresent(updateTransactionCounter);
                break;

            case COMMIT_TRANSACTION:
            case ABORT_TRANSACTION:
            case END_TRANSACTION:
                assert (transactionEntry.isPresent());
                transactionEntry.ifPresent( entry -> {
                    Transaction transaction = entry.transaction;
                    switch (logRecord.type) {
                        case COMMIT_TRANSACTION:
                            //  we will clean up all transaction in committing status afterwards in this function
                            transaction.setStatus(Status.COMMITTING);
                            break;
                        case ABORT_TRANSACTION:
                            transaction.setStatus(Status.RECOVERY_ABORTING);
                            break;
                        case END_TRANSACTION:
                            // if transaction is a recovery transaciton 
                            // then end() method will not be called
                            // so no duplicate end record will be written
                            // we have to write the status ourselves
                            transaction.cleanup(); 
                            transaction.setStatus(Status.COMPLETE);
                            break;
                        default:
                            break;
                    }
                });
                break;

            case ALLOC_PAGE:
            case UNDO_ALLOC_PAGE:
            case FREE_PAGE:
            case UNDO_FREE_PAGE:
            case UPDATE_PAGE:
            case UNDO_UPDATE_PAGE:
                assert (transactionEntry.isPresent());
                transactionEntry.ifPresent(entry -> {
                    assert (logRecord.getPageNum().isPresent());
                    logRecord.getPageNum().ifPresent(pageNum -> {
                        entry.touchedPages.add(pageNum);
                        acquireTransactionLock(
                            entry.transaction, getPageLockContext(pageNum), LockType.X);
                        if (logRecord.type == LogType.UPDATE_PAGE || logRecord.type == LogType.UNDO_UPDATE_PAGE) {
                            updateDirtyPageTable(pageNum, logRecord.LSN);
                        } else if (logRecord.type == LogType.FREE_PAGE || logRecord.type == LogType.UNDO_ALLOC_PAGE) {
                            dirtyPageTable.remove(pageNum);
                        } 
                        // else logRecord.type == LogType.ALLOC_PAGE || logRecord.type == LogType.UNDO_FREE_PAGE
                        // we do nothing because these records only loads page from disk to memory
                        // so they neither flush page to disk nor make the page dirty
                    });
                });
                break;

            case ALLOC_PART:
            case UNDO_ALLOC_PART:
                // we don't need to do anything other than update the transaction table 
                // if it is a partition related log
                break;

            case END_CHECKPOINT:
                /**
                 * One thing to be clear, what we saw in log is possibly newer than what we have in checkpoint
                 * but NEVER older, since when we save things in checkpoint they must have been reflected in log
                 * but in the process of saving things in checkpoint, change are also taking place.
                 */

                /**
                 * Step 1: dirty page table
                 * 
                 * We should replace everything in the dirty page table even if the entry exists.
                 *
                 * There are two possibilities:
                 * 1. recLSN in dirty page table is SMALLER than recLSN in checkpoint
                 * 2. recLSN in dirty page table is LARGER than recLSN in checkpoint
                 * 
                 * Notice that recLSN is the FIRST log that makes the page dirty, so we don't care if one log came later.
                 * As we mentioned at the begining of this section, things in checkpoint is possible older, but never newer
                 * than things in log. So concerning dirty page table we are more interesting in older things.
                 * 
                 * If 2 happens, this means the LSN in checkpoint is earlier than what we already have in dirty page table
                 * so we should update.
                 * If 1 happens, notice that it is only possible that the page got flushed, reloaded to buffer and
                 * got dirty again between the dirty page table LSN and checkpoint LSN like this
                 * dirty page table LSN -> flushed to disk -> reloaded to buffer -> got dirty -> checkpoint saved LSN
                 * other wise the dirty page table will not be updated
                 * so we should update to checkpoint LSN because it reflects the most recent flush-change
                 */
                dirtyPageTable.putAll(logRecord.getDirtyPageTable());

                /**
                 * Step 2: trnasaction status
                 * 
                 * On the other hand, for transactions, if the transaction status in checkpoint record is different from what
                 * we already have in transaction table
                 *   
                 * To be clear, the life cycle of a transaction is
                 * RUNNING -> COMMITTING/ABORTING -> ENDING
                 *   
                 * 1. RUNNING(checkpoint) vs COMMITTING/ABORTING/ENDING(transaction table)
                 *     possible if when we write this end checkpoint log the commit/abort/end record has been added to the log, but
                 *     was not reflected in the transaction table in checkpoint
                 *     i.e.
                 *     start collecting transaction table, save this transaction status as RUNNING
                 *       -> commiting transaction, commit record appended to logs
                 *         -> end checkpoint record appended to logs
                 *     in this case we don't update since what in transaction table is newer
                 *   
                 * 2. ABORTING(checkpoint) vs RUNNING(transaction table)
                 *     possible if the abort record is before begin checkpoint record so we don't see it
                 *     since we write the abort record before we write any undo record, if we miss that abort record
                 *     we will think that the transaction is still RUNNIG
                 *     abort record(we didn't see this ) -> begin checkpoint -> undo record(now we think it's still RUNNING) -> end checkpoint
                 *     however, we don't care in this case because any RUNNING transaction will be turned into RECOVERY_ABORTING at the end of analysis
                 *     so it is fine
                 *   
                 * 3. Any other reverse order is not possible. 
                 *     again, what in checkpoint is possibly order, but never newer than what we saw in logs. So for example if
                 *     we have COMMITING(checkpoint) vs RUNNING(transaction table)
                 *     we should have seen the commit record because the transaction status will be set to COMMITING only if 
                 *     the commit record is appended to logs. And this one will come before we see the checkpoint record
                 *   
                 * So in conclusion, we don't care the transaction status if it is already in the transaction table
                 */
                for (Entry<Long, Pair<Transaction.Status, Long>> transaction : logRecord.getTransactionTable().entrySet()) {
                    long transNum = transaction.getKey();
                    Transaction.Status status = transaction.getValue().getFirst();
                    long lastLSN = transaction.getValue().getSecond();
                    // if the status is already COMPLETE we do nothing
                    if (status != Status.COMPLETE) {
                        transactionTable.computeIfAbsent(transNum, newTransaction.andThen(trans -> {
                            switch (status) {
                                case ABORTING:
                                    trans.setStatus(Status.RECOVERY_ABORTING);
                                    break;
                                case COMMITTING:
                                    trans.setStatus(Status.COMMITTING);
                                    break;
                                default:
                                    break;
                            }
                            return trans;
                        }).andThen(TransactionTableEntry::new));
                        transactionTable.computeIfPresent(transNum, (num, entry) -> {
                            entry.lastLSN = lastLSN > entry.lastLSN ? lastLSN : entry.lastLSN;
                            return entry;
                        });
                    }
                }

                // Step 3: update touched pages
                // Notice at this time we should finished our analysis to transaction table.
                // So any transaction that is not in transaction table will be regarded as finished
                for (Entry<Long, List<Long>> touchedPages : logRecord.getTransactionTouchedPages().entrySet()) {
                    long transNum = touchedPages.getKey();
                    List<Long> pages = touchedPages.getValue();
                    transactionTable.computeIfPresent(transNum, (num, entry) -> {
                        entry.touchedPages.addAll(pages);
                        pages.stream().forEach(pageNum -> 
                            acquireTransactionLock(
                                entry.transaction, getPageLockContext(pageNum), LockType.X)
                        );
                        return entry;
                    });
                }
                break;
            
            default:
                throw new UnsupportedOperationException("should not arrive here");
        }
    }

    Boolean shouldRedo(LogRecord record) {
        long recLSN = record.LSN;
        if (!record.isRedoable())
            return false;
        switch (record.type) {
            case ALLOC_PAGE:
            case UNDO_ALLOC_PAGE:
            case FREE_PAGE:
            case UNDO_FREE_PAGE:
            case UPDATE_PAGE:
            case UNDO_UPDATE_PAGE:
                long pageLSN = bufferManager.fetchPage(dbContext, record.getPageNum().get(), false).getPageLSN();
                return dirtyPageTable.containsKey(recLSN)
                    && recLSN >= dirtyPageTable.get(recLSN)
                    && pageLSN < recLSN;
            
            case ALLOC_PART:
            case UNDO_ALLOC_PART:
                return true;
        
            default:
                return false;
        }
    }

    /**
     * Returns the lock context for a given page number.
     * @param pageNum page number to get lock context for
     * @return lock context of the page
     */
    private LockContext getPageLockContext(long pageNum) {
        int partNum = DiskSpaceManager.getPartNum(pageNum);
        return this.dbContext.childContext(partNum).childContext(pageNum);
    }

    /**
     * Locks the given lock context with the specified lock type under the specified transaction,
     * acquiring locks on ancestors as needed.
     * @param transaction transaction to request lock for
     * @param lockContext lock context to lock
     * @param lockType type of lock to request
     */
    private void acquireTransactionLock(Transaction transaction, LockContext lockContext,
                                        LockType lockType) {
        acquireTransactionLock(transaction.getTransactionContext(), lockContext, lockType);
    }

    /**
     * Locks the given lock context with the specified lock type under the specified transaction,
     * acquiring locks on ancestors as needed.
     * @param transactionContext transaction context to request lock for
     * @param lockContext lock context to lock
     * @param lockType type of lock to request
     */
    private void acquireTransactionLock(TransactionContext transactionContext,
                                        LockContext lockContext, LockType lockType) {
        TransactionContext.setTransaction(transactionContext);
        try {
            if (lockRequests == null) {
                LockUtil.ensureSufficientLockHeld(lockContext, lockType);
            } else {
                lockRequests.add("request " + transactionContext.getTransNum() + " " + lockType + "(" +
                                 lockContext.getResourceName() + ")");
            }
        } finally {
            TransactionContext.unsetTransaction();
        }
    }

    /**
     * Comparator for Pair<A, B> comparing only on the first element (type A), in reverse order.
     */
    private static class PairFirstReverseComparator<A extends Comparable<A>, B> implements
        Comparator<Pair<A, B>> {
        @Override
        public int compare(Pair<A, B> p0, Pair<A, B> p1) {
            return p1.getFirst().compareTo(p0.getFirst());
        }
    }
}
