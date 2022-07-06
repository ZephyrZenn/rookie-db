package edu.berkeley.cs186.database.query.join;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.query.JoinOperator;
import edu.berkeley.cs186.database.query.QueryOperator;
import edu.berkeley.cs186.database.table.Record;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Performs an equijoin between two relations on leftColumnName and
 * rightColumnName respectively using the Block Nested Loop Join algorithm.
 */
public class BNLJOperator extends JoinOperator {
    protected int numBuffers;

    public BNLJOperator(QueryOperator leftSource,
                        QueryOperator rightSource,
                        String leftColumnName,
                        String rightColumnName,
                        TransactionContext transaction) {
        super(leftSource, materialize(rightSource, transaction),
                leftColumnName, rightColumnName, transaction, JoinType.BNLJ
        );
        this.numBuffers = transaction.getWorkMemSize();
        this.stats = this.estimateStats();
    }

    @Override
    public Iterator<Record> iterator() {
        return new BNLJIterator();
    }

    @Override
    public int estimateIOCost() {
        //This method implements the IO cost estimation of the Block Nested Loop Join
        int usableBuffers = numBuffers - 2;
        int numLeftPages = getLeftSource().estimateStats().getNumPages();
        int numRightPages = getRightSource().estimateIOCost();
        return ((int) Math.ceil((double) numLeftPages / (double) usableBuffers)) * numRightPages +
               getLeftSource().estimateIOCost();
    }

    /**
     * A record iterator that executes the logic for a simple nested loop join.
     * Look over the implementation in SNLJOperator if you want to get a feel
     * for the fetchNextRecord() logic.
     */
    private class BNLJIterator implements Iterator<Record>{
        // Iterator over all the records of the left source
        private Iterator<Record> leftSourceIterator;
        // Iterator over all the records of the right source
        private BacktrackingIterator<Record> rightSourceIterator;
        // Iterator over records in the current block of left pages
        private BacktrackingIterator<Record> leftBlockIterator;
        // Iterator over records in the current right page
        private BacktrackingIterator<Record> rightPageIterator;
        // The current record from the left relation
        private Record leftRecord;
        // The next record to return
        private Record nextRecord;

        private BNLJIterator() {
            super();
            this.leftSourceIterator = getLeftSource().iterator();
            this.fetchNextLeftBlock();

            this.rightSourceIterator = getRightSource().backtrackingIterator();
            this.rightSourceIterator.markNext();
            this.fetchNextRightPage();

            this.nextRecord = null;
        }

        /**
         * Fetch the next block of records from the left source.
         * leftBlockIterator should be set to a backtracking iterator over up to
         * B-2 pages of records from the left source, and leftRecord should be
         * set to the first record in this block.
         *
         * If there are no more records in the left source, this method should
         * do nothing.
         *
         * You may find QueryOperator#getBlockIterator useful here.
         * Make sure you pass in the correct schema to this method.
         */
        private void fetchNextLeftBlock() {
            // 左表遍历完毕，退出
            if (!leftSourceIterator.hasNext()) {
//                leftRecord = null;
                return;
            }
            int limit = numBuffers - 2;
            this.leftBlockIterator = getBlockIterator(leftSourceIterator, getLeftSource().getSchema(), limit);
            this.leftBlockIterator.markNext();
            leftRecord = leftBlockIterator.next();
            // TODO(proj3_part1): implement
        }

        /**
         * Fetch the next page of records from the right source.
         * rightPageIterator should be set to a backtracking iterator over up to
         * one page of records from the right source.
         *
         * If there are no more records in the right source, this method should
         * do nothing.
         *
         * You may find QueryOperator#getBlockIterator useful here.
         * Make sure you pass in the correct schema to this method.
         */
        private void fetchNextRightPage() {
            // TODO(proj3_part1): implement
            if (!rightSourceIterator.hasNext()) {
                return;
            }

            this.rightPageIterator = getBlockIterator(rightSourceIterator, getRightSource().getSchema(), 1);
            this.rightPageIterator.markNext();
        }

        /**
         * Returns the next record that should be yielded from this join,
         * or null if there are no more records to join.
         *
         * You may find JoinOperator#compare useful here. (You can call compare
         * function directly from this file, since BNLJOperator is a subclass
         * of JoinOperator).
         */
        private Record fetchNextRecord() {
            // TODO(proj3_part1): implement
//            while (leftRecord != null || leftSourceIterator.hasNext()) {
//                // 左表数据块迭代器与右表所有的页迭代器的循环
//                while (rightSourceIterator.hasNext() || rightPageIterator.hasNext()) {
//                    // 针对某一块左表记录的循环
//                    while (leftRecord != null) {
//                        // 针对某一左表记录的循环
//                        // 扫描右表当前页的记录，寻找匹配
//                        while (rightPageIterator.hasNext()) {
//                            Record rightRecord = rightPageIterator.next();
//                            if (compare(this.leftRecord, rightRecord) == 0) {
//                                return leftRecord.concat(rightRecord);
//                            }
//                        }
//                        // 换到下一个左表记录
//                        leftRecord = leftBlockIterator.hasNext() ? leftBlockIterator.next() : null;
//                        // 重置右表迭代器
//                        rightPageIterator.reset();
//                        rightPageIterator.markNext();
//                    }
//                    // 重置左表迭代器
//                    leftBlockIterator.reset();
//                    leftBlockIterator.markNext();
//                    leftRecord = leftBlockIterator.hasNext() ? leftBlockIterator.next() : null;
//                    // 获取新的右表迭代器
//                    fetchNextRightPage();
//                }
//                // 获取新的左表块迭代器
//                fetchNextLeftBlock();
//                // 重置整个右表迭代器
//                rightSourceIterator.reset();
//                rightSourceIterator.markNext();
//                // 重新获取右表迭代器
//                fetchNextRightPage();
//            }
//            return null;

            // 上方的写法看上去差不多，但是如果被驱动表被读取完毕，而驱动表块迭代器没有迭代完，就会直接去fetch下一个驱动表块迭代器
            // 其判断逻辑为：先看驱动表有没有剩余数据，再看被驱动表有无剩余数据，然后检查驱动表块有无剩余数据，最后匹配。
            // 因为被驱动表必定会先被读完，因此每块只有第一个数据能完整探索被驱动表
            while (true) {

                if (rightPageIterator.hasNext()) {
                    Record rightRecord = rightPageIterator.next();
                    if (compare(leftRecord, rightRecord) == 0) {
                        return leftRecord.concat(rightRecord);
                    }
                } else if (leftBlockIterator.hasNext()) {
                    leftRecord = leftBlockIterator.next();
                    rightPageIterator.reset();
                    rightPageIterator.markNext();
                } else if (rightSourceIterator.hasNext()) {
                    fetchNextRightPage();
                    leftBlockIterator.reset();
                    leftBlockIterator.markNext();
                    leftRecord = leftBlockIterator.next();
                } else if (leftSourceIterator.hasNext()) {
                    fetchNextLeftBlock();
                    rightSourceIterator.reset();
                    rightSourceIterator.markNext();
                    fetchNextRightPage();
                } else {
                    return null;
                }
            }
        }

        /**
         * @return true if this iterator has another record to yield, otherwise
         * false
         */
        @Override
        public boolean hasNext() {
            if (this.nextRecord == null) this.nextRecord = fetchNextRecord();
            return this.nextRecord != null;
        }

        /**
         * @return the next record from this iterator
         * @throws NoSuchElementException if there are no more records to yield
         */
        @Override
        public Record next() {
            if (!this.hasNext()) throw new NoSuchElementException();
            Record nextRecord = this.nextRecord;
            this.nextRecord = null;
            return nextRecord;
        }
    }
}
