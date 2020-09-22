package edu.berkeley.cs186.database.query;

import java.util.*;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.memory.Page;
import edu.berkeley.cs186.database.table.Record;

class BNLJOperator extends JoinOperator {
    protected int numBuffers;

    BNLJOperator(QueryOperator leftSource,
                 QueryOperator rightSource,
                 String leftColumnName,
                 String rightColumnName,
                 TransactionContext transaction) {
        super(leftSource, rightSource, leftColumnName, rightColumnName, transaction, JoinType.BNLJ);

        this.numBuffers = transaction.getWorkMemSize();

        this.stats = this.estimateStats();
        this.cost = this.estimateIOCost();
    }

    @Override
    public Iterator<Record> iterator() {
        return new BNLJIterator();
    }

    @Override
    public int estimateIOCost() {
        //This method implements the IO cost estimation of the Block Nested Loop Join
        int usableBuffers = numBuffers - 2;
        int numLeftPages = getLeftSource().getStats().getNumPages();
        int numRightPages = getRightSource().getStats().getNumPages();
        return ((int) Math.ceil((double) numLeftPages / (double) usableBuffers)) * numRightPages +
               numLeftPages;
    }

    /**
     * BNLJ: Block Nested Loop Join
     *  See lecture slides.
     *
     * An implementation of Iterator that provides an iterator interface for this operator.
     *
     * Word of advice: try to decompose the problem into distinguishable sub-problems.
     *    This means you'll probably want to add more methods than those given.
     */
    private class BNLJIterator extends JoinIterator {
        // Iterator over pages of the left relation
        private BacktrackingIterator<Page> leftIterator;
        // Iterator over pages of the right relation
        private BacktrackingIterator<Page> rightIterator;
        // Iterator over records in the current block of left pages
        private BacktrackingIterator<Record> leftRecordIterator = null;
        // Iterator over records in the current right page
        private BacktrackingIterator<Record> rightRecordIterator = null;
        // The current record on the left page
        private Record leftRecord = null;

        private Record rightRecord = null;
        // The next record to return
        private Record nextRecord = null;

        private BNLJIterator() {
            super();

            this.rightIterator = BNLJOperator.this.getPageIterator(this.getRightTableName());
            if (!this.rightIterator.hasNext()){
                this.leftRecord = null;
                this.nextRecord = null;
                return;
            }
            this.rightIterator.markNext();
            fetchNextRightPage();

            this.leftIterator = BNLJOperator.this.getPageIterator(this.getLeftTableName());
            fetchNextLeftBlock();
            if (this.leftRecordIterator != null)
                this.leftRecord = this.leftRecordIterator.next();

            try {
                this.fetchNextRecord();
            } catch (NoSuchElementException e) {
                this.nextRecord = null;
            }
        }

        /**
         * Fetch the next non-empty block of B - 2 pages from the left relation. leftRecordIterator
         * should be set to a record iterator over the next B - 2 pages of the left relation that
         * have a record in them, and leftRecord should be set to the first record in this block.
         *
         * If there are no more pages in the left relation with records, both leftRecordIterator
         * and leftRecord should be set to null.
         * 
         */
        private void fetchNextLeftBlock() {
            // TODO(proj3_part1): implement
            this.leftRecordIterator = BNLJOperator.this.getBlockIterator(this.getLeftTableName(), this.leftIterator, BNLJOperator.this.numBuffers - 2);
            if (!this.leftRecordIterator.hasNext()) {
                this.leftRecordIterator = null;
            } else {
                this.leftRecordIterator.markNext();
            }
        }

        private void resetLeftRecordIterator() {
            if (this.leftRecordIterator != null)
                this.leftRecordIterator.reset();
        }

        /**
         * Fetch the next non-empty page from the right relation. rightRecordIterator
         * should be set to a record iterator over the next page of the right relation that
         * has a record in it.
         *
         * If there are no more pages in the left relation with records, rightRecordIterator
         * should be set to null.
         */
        private void fetchNextRightPage() {
            // TODO(proj3_part1): implement
            this.rightRecordIterator = BNLJOperator.this.getBlockIterator(this.getRightTableName(), this.rightIterator, 1);
            assert this.rightRecordIterator.hasNext();
            this.rightRecordIterator.markNext();
        }

        
        private void resetRightRecordIterator() {
            this.rightRecordIterator.reset();
        }

        private void resetRightPageIterator() {
            this.rightIterator.reset();
        }

        /**
         * Fetches the next record to return, and sets nextRecord to it. If there are no more
         * records to return, a NoSuchElementException should be thrown.
         * 
         * ALGORITHM OVERVIEW
         * 
         * for LBlock in LData:
         *   for RPage in RData:
         *     for LRecord in LBlock:
         *       for RRecord in RPage:
         *         return (LRecord, RRecord)
         * 
         * 
         * @throws NoSuchElementException if there are no more Records to yield
         */
        private void fetchNextRecord() {
            // TODO(proj3_part1): implement

            // Order matters!
            // We have to iterate at least once in each fetchNextRecord
            // but we also have to call it once in initialization
            // so if we call after loop, this method will be called one more time
            // which will cause a mismatch
            
            iterateOnce();
            if (this.leftRecord == null) { throw new NoSuchElementException("No new record to fetch"); }
            while (!checkMatch()) {
                iterateOnce();
                if (this.leftRecord == null) { throw new NoSuchElementException("No new record to fetch"); }
            }
            this.nextRecord = joinRecords(this.leftRecord, this.rightRecord);
        }

        private Boolean checkMatch(){
            DataBox leftJoinValue = this.leftRecord.getValues().get(BNLJOperator.this.getLeftColumnIndex());
            DataBox rightJoinValue = this.rightRecord.getValues().get(BNLJOperator.this.getRightColumnIndex());

            return leftJoinValue.equals(rightJoinValue);
        }

        private void iterateOnce() {
            if (!this.rightRecordIterator.hasNext()){
                if (!this.leftRecordIterator.hasNext()){
                    if (!this.rightIterator.hasNext()){
                        fetchNextLeftBlock();
                        resetRightPageIterator();
                    }
                    if (this.leftRecordIterator != null){
                        fetchNextRightPage();
                        resetLeftRecordIterator();
                    }
                }
                resetRightRecordIterator();
                if (this.leftRecordIterator == null) {
                    this.leftRecord = null;
                } else {
                    this.leftRecord = this.leftRecordIterator.next();
                }
            }
            
            if (this.rightRecordIterator.hasNext())
                this.rightRecord = this.rightRecordIterator.next();
        }

        /**
         * Helper method to create a joined record from a record of the left relation
         * and a record of the right relation.
         * @param leftRecord Record from the left relation
         * @param rightRecord Record from the right relation
         * @return joined record
         */
        private Record joinRecords(Record leftRecord, Record rightRecord) {
            List<DataBox> leftValues = new ArrayList<>(leftRecord.getValues());
            List<DataBox> rightValues = new ArrayList<>(rightRecord.getValues());
            leftValues.addAll(rightValues);
            return new Record(leftValues);
        }

        /**
         * Checks if there are more record(s) to yield
         *
         * @return true if this iterator has another record to yield, otherwise false
         */
        @Override
        public boolean hasNext() {
            return this.nextRecord != null;
        }

        /**
         * Yields the next record of this iterator.
         *
         * @return the next Record
         * @throws NoSuchElementException if there are no more Records to yield
         */
        @Override
        public Record next() {
            if (!this.hasNext()) {
                throw new NoSuchElementException();
            }

            Record nextRecord = this.nextRecord;
            try {
                this.fetchNextRecord();
            } catch (NoSuchElementException e) {
                this.nextRecord = null;
            }
            return nextRecord;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
