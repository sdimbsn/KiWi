package kiwi;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by dbasin on 11/24/15.
 */
public class Rebalancer<K extends Comparable<? super K>,V> {

    public static double MAX_AFTER_MERGE_PART= 0.5;

    /********** Policy *******************/
    abstract class Policy
    {
        abstract public Chunk<K,V> findNextCandidate();
        abstract void updateRangeView();
        abstract Chunk<K,V> getFirstChunkInRange();
        abstract Chunk<K,V > getLastChunkInRange();
    }

    class PolicyImpl extends Policy{
        private final int  RebalanceSize = KiWi.RebalanceSize;



        private int chunksInRange;
        private int itemsInRange;

        private int maxAfterMergeItems;


        public Chunk<K,V> first;
        public Chunk<K,V> last;

        public PolicyImpl(Chunk<K,V> startChunk)
        {
            if(startChunk == null) throw new IllegalArgumentException("startChunk is null in policy");
            first = startChunk;
            last = startChunk;
            chunksInRange = 1;
            itemsInRange = startChunk.getStatistics().getCompactedCount();
            maxAfterMergeItems = (int)(Chunk.MAX_ITEMS * MAX_AFTER_MERGE_PART);
        }

        /***
         * verifies that the chunk is not engaged and not null
         * @param chunk candidate chunk for range extension
         * @return true if not engaged and not null
         */
        private boolean isCandidate(Chunk<K,V> chunk)
        {
            // do not take chunks that are engaged with another rebalancer or infant
            if(chunk == null || !chunk.isEngaged(null) || chunk.isInfant()) return false;
            return true;
        }

        /***
         *
         * @return
         */
        @Override
        public Chunk<K, V> findNextCandidate() {

            updateRangeView();

            // allow up to RebalanceSize chunks to be engaged
            if(chunksInRange >= RebalanceSize) return null;

            Chunk<K,V> next = Rebalancer.this.chunkIterator.getNext(last);
            Chunk<K,V> prev = Rebalancer.this.chunkIterator.getPrev(first);
            Chunk<K,V> candidate = null;

            if(!isCandidate(next)) next = null;
            if(!isCandidate(prev)) prev = null;

            if(next == null && prev == null) return null;

            if(next == null) {
                candidate = prev;
            }
            else if(prev == null)
            {
                candidate = next;
            }
            else {
                candidate = prev.getStatistics().getCompactedCount() < next.getStatistics().getCompactedCount() ? prev : next;
            }


            int newItems = candidate.getStatistics().getCompactedCount();
            int totalItems = itemsInRange + newItems;


            int chunksAfterMerge = (int)Math.ceil(((double)totalItems)/maxAfterMergeItems);

            // if the the chosen chunk may reduce the number of chunks -- return it as candidate
            if( chunksAfterMerge < chunksInRange + 1) {
                return candidate;
            } else
            {
                return null;
            }
        }

        @Override
        public void updateRangeView()
        {
            
            updateRangeFwd();
            updateRangeBwd();
            
        }

        @Override
        public Chunk<K, V> getFirstChunkInRange() {
            return first;
        }

        @Override
        public Chunk<K, V> getLastChunkInRange() {
            return last;
        }

        private void addToCounters(Chunk<K,V> chunk)
        {
            itemsInRange += chunk.getStatistics().getCompactedCount();
            chunksInRange++;
        }

        private void updateRangeFwd()
        {

            while(true) {
                Chunk<K, V> next = chunkIterator.getNext(last);
                if (next == null || !next.isEngaged(Rebalancer.this)) break;
                last = next;
                addToCounters(last);
            }
        }
        
        private void updateRangeBwd()
        {
             while(true)
             {
                 Chunk<K,V> prev = Rebalancer.this.chunkIterator.getPrev(first);
                 if(prev == null || !prev.isEngaged(Rebalancer.this)) break;
                 // double check here, after we know that prev is engaged, thus cannot be updated
                 if(prev.next.getReference() == first) {
                     first = prev;
                     addToCounters(first);
                 }
             }
        }
    }

    Policy createPolicy(Chunk<K,V> startChunk)
    {
        return new PolicyImpl(startChunk);
    }

    /******** Members ************/


    private AtomicReference<Chunk<K,V>> nextToEngage;
    private Chunk<K,V> startChunk;
    private ChunkIterator<K,V> chunkIterator;

    private AtomicReference<List<Chunk<K,V>>> compactedChunks = new AtomicReference<>(null);
    private AtomicReference<List<Chunk<K,V>>> engagedChunks = new AtomicReference<>(null);
    private AtomicBoolean freezedItems = new AtomicBoolean(false);


   /******* Constructors *********/

    public Rebalancer(Chunk<K,V> chunk, ChunkIterator<K,V> chunkIterator)
    {
        if(chunk == null || chunkIterator == null) throw new IllegalArgumentException("Rebalancer construction with null args");
        
        nextToEngage = new AtomicReference<>(chunk);
        this.startChunk = chunk;
        this.chunkIterator = chunkIterator;
    }

    /******* Public methods **********/

    // assumption -- chunk once engaged remains with the same rebalance object forever, till GC
    public Rebalancer<K,V> engageChunks()
    {
        // the policy object will store first, last refs of engaged range
        Policy p = createPolicy(startChunk);

        while(true)
        {
            Chunk<K,V> next = nextToEngage.get();
            if(next == null) break;

            next.engage(this);

            if(!next.isEngaged(this) && next == startChunk)
                return next.getRebalancer().engageChunks();

            // policy caches last discovered  interval [first, last] of engaged range
            // to get next candidate policy traverses from first backward,
            //  from last forward to find non-engaged chunks connected to the engaged interval
            // if we return null here the policy decided to terminate the engagement loop

            Chunk candidate = p.findNextCandidate();

            // if fail to CAS here, another thread has updated next candidate
            // continue to while loop and try to engage it
            nextToEngage.compareAndSet(next, candidate);
        }

        p.updateRangeView();
        List<Chunk<K,V>> engaged = createEngagedList(p.getFirstChunkInRange());

        if(engagedChunks.compareAndSet(null,engaged) && Parameters.countCompactions) {
            Parameters.compactionsNum.getAndIncrement(); // if CAS fails here - another thread has updated it
            Parameters.engagedChunks.addAndGet(engaged.size());
        }

       return this;
    }


    /***
     * Freeze the engaged chunks. Should be called after engageChunks.
     * Marks chunks as freezed, prevents future updates of the engagead chunks
     * @return total number of items in the freezed range
     */
    public Rebalancer freeze()
    {
        if(isFreezed()) return this;


        for(Chunk<K,V> chunk : getEngagedChunks()){
            chunk.freeze();
        }

        freezedItems.set(true);

        return this;
    }


    public Rebalancer compact(ScanIndex<K> scanIndex)
    {
        if(isCompacted()) return this;

        Compactor c = new CompactorImpl();
        List<Chunk<K,V>> compacted =  c.compact(getEngagedChunks(),scanIndex);

        // if fail here, another thread succeeded
        compactedChunks.compareAndSet(null,compacted);

        return this;
    }

    public boolean isCompacted()
    {
        return compactedChunks.get() != null;
    }

    public boolean isFreezed()
    {
        return freezedItems.get();
    }

    public boolean isEngaged()
    {
        return engagedChunks.get() != null;
    }

    public  List<Chunk<K,V>> getCompactedChunks() {
        if(!isCompacted()) throw new IllegalStateException("Trying to get compacted chunks before compaction stage completed");

        return compactedChunks.get();
    }

    public List<Chunk<K,V>> getEngagedChunks()
    {
        List<Chunk<K,V>> engaged = engagedChunks.get();
        if(engaged == null) throw new IllegalStateException("Trying to get engaged before engagement stage completed");

        return engaged;
    }


    private List<Chunk<K,V>> createEngagedList(Chunk<K, V> firstChunkInRange) {
        Chunk<K,V> current = firstChunkInRange;
        List<Chunk<K,V>> engaged = new LinkedList<>();

        while(current != null && current.isEngaged(this))
        {
            engaged.add(current);
            current = current.next.getReference();
        }

        if(engaged.isEmpty()) throw new IllegalStateException("Engaged list cannot be empty");

        return engaged;
    }


}
