package com.splicemachine.si.impl.compaction;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.List;

/**
 * Abstraction for maintaining the state of a single row during Compaction. This is <em>only</em>
 * to be used by a RowCompactor as a convenient bit of shared code.
 *
 * This is mainly an abstraction around a specialized doubly-linked list, along with some convenience methods
 * to operate against it.
 *
 * @author Scott Fines
 *         Date: 11/24/15
 */
class CompactionRow{
    private Node head;
    private Node tail;
    /*
     * An efficiency improvement: We need to insert commit timestamps
     * backwards through potentially thousands of records. Obviously, we want to do this
     * efficiently, but a search through a LinkedList requires O(number of checkpoints) + O(number of
     * commit timestamp cells higher than us) to find the proper location. These two pointers shave
     * that value to O(1)
     *
     * The idea is this: we get records in reverse timestamp order, which means that, once we visit a given
     * version of data, we will not re-visit it (within that column). As a result, we can advance the commit
     * timestamp pointer when we add in a commit timestamp, which will allow us to immediately point to
     * the next value, and therefore saving the need to traverse the list for insertion
     *
     * However, tombstones may do some commit timestamp addition as well as user data, which means that when
     * we switch from tombstones to user data, we may go back to previously visited timestamps. Thus, we keep
     * a pointer to the first commit timestamp in the list as well, and when we transition from tombstone column
     * to user column, we reset the commitTimestampPointer to the firstCommitTimestamp
     *
     */
    private Node firstCommitTimestamp;
    private Node commitTimestampPointer;

    void resetCommitTimestampPointers(){
        commitTimestampPointer = firstCommitTimestamp;
    }

    void clearDiscardedData(long discardVersion){
        /*
         * It is possible that there are some elements here which can still be discarded (if a tombstone
         * has advanced the discardPoint). Thus, we apply a secondary filter here to make sure that the node
         * we have should not be discarded
         */
        Node n = head;
        while(n!=null){
            if(n.data.getTimestamp()<discardVersion){
                if(n.previous!=null)
                    n.previous.next = n.next;
                if(n.next!=null)
                    n.next.previous = n.previous;
                if(n==head)
                    head = n.next;
                if(n==tail)
                    tail = n.previous;
            }
            n = n.next;
        }
    }

    void insert(Cell cell){
        insert(head,cell);
    }

    void append(Cell cell){
        /*
         * Append the cell to the end of the linked list. Only use this when you know you are maintaining
         * proper sort order
         */
        if(head==null){
            //the list is empty, start a new one
            head = tail = new Node(cell);
        }else{
            tail.linkAfter(new Node(cell));
            tail = tail.next;
        }
    }

    void insertCommitTimestamp(Cell ct){
        Node n = head;
        if(commitTimestampPointer!=null)
            n = commitTimestampPointer;
        n =insert(n,ct);
        commitTimestampPointer = n;
        if(firstCommitTimestamp==null)
            firstCommitTimestamp = n;
    }

    void appendCommitTimestamp(Cell ct){
        append(ct);
        if(firstCommitTimestamp==null){
            firstCommitTimestamp = tail;
            commitTimestampPointer = firstCommitTimestamp;
        }
    }

    boolean isEmpty(){
        return head==null;
    }

    void drain(List<Cell> destination){
        while(head!=null){
            destination.add(head.data);
            head = head.next;
            if(head!=null)
                head.previous=null;
        }
        tail = null;
    }

    boolean drain(List<Cell> destination, int limit){
        assert limit>0: "Programmer error: use drain() instead";
        int added=0;
        while(head!=null && added<limit){
            destination.add(head.data);
            head = head.next;
            if(head!=null)
                head.previous = null;
            added++;
        }

        if(head==null){
            tail=null;
            return false;
        }else return true;
    }

    /* ****************************************************************************************************************/
    /*private helper methods and classes*/

    private Node insert(Node start, Cell ct){
        /*
         * Insert the cell into the linked list at the correct sorted position, starting at the specified node
         */
        Node n = start;
        while(n!=null && n.compare(ct)<0){
            n = n.next;
        }
        if(n==null){
            append(ct);
            return tail;
        }else if(n.compare(ct)!=0){
            n.linkBefore(new Node(ct));
            if(n==head)
                head = n.previous;
        }

        return n;
    }
    /*
     * Internal Linked List.
     *
     * Because compaction is underwrit by a sorted scanner, it's more efficient for us to maintain
     * a sorted list of records than it is to maintain something like a TreeSet (since that has to perform
     * rebalancing actions).
     *
     * So why not use an ArrayList? Two reasons:
     *
     * 1. Memory. If we have one row which has lots and lots of versions, then
     * we would end up with a very large underlying array. We would need to either shrink it after each run (resulting
     * in lots of memory churn as we repeatedly resize the underlying array), or we would hold on to an exceptionally
     * large array constantly, even if subsequent rows are not nearly as large.
     *
     * 2. Efficient sorted insertion. In an ArrayList, to insert an element at a given position requires us to
     * move all the references down exactly one position. Even though this is done using a memcpy(System.arraycopy),
     * you still are constantly moving records around when it comes time to insert a Commit timestamp (and the likelihood
     * is that you are likely going to insert a LOT of commit timestamps for very large rows).
     *
     * A linked list avoids both of those constraints: it uses less memory (O(the current row) rather than O(largest row)),
     * and allows for efficient sorted insertion when it comes times to insert commit timestamps and checkpoint cells.
     *
     * So why use a custom LinkedList implementation? Why not just use LinkedList<Cell>? We do this because it allows
     * us direct access to the underlying Node instances, which allows us to do things like "delete all cells from
     * this column which have a version < some value" efficiently (just unlink the first and the last element matching
     * the range).
     */
    private static class Node {
        private Cell data;
        private Node next;
        private Node previous;

        public Node(Cell data){
            this.data=data;
        }

        public void linkAfter(Node n){
            n.next = next;
            n.previous = this;
            if(next!=null)
                next.previous = n;
            next = n;
        }

        public void linkBefore(Node n){
            n.previous = previous;
            n.next = this;
            if(previous!=null)
                previous.next = n;
            previous = n;
        }

        public int compare(Cell ct){
            int c = Bytes.compareTo(data.getQualifierArray(),data.getQualifierOffset(),data.getQualifierLength(),
                    ct.getQualifierArray(),ct.getQualifierOffset(),ct.getQualifierLength());
            if(c==0){
                c = -1*Long.compare(data.getTimestamp(),ct.getTimestamp()); //reverse timestamp order
            }
            return c;
        }
    }
}
