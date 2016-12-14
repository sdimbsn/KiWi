package kiwi;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

/**
 * Created by dbasin on 12/1/15.
 */

public class CompactorImpl<K extends Comparable<K>,V> implements Compactor<K,V>
{
    private final int LOW_THRESHOLD = Chunk.MAX_ITEMS/2;
    private final int HIGH_THRESHOLD = Chunk.MAX_ITEMS - KiWi.MAX_THREADS; // max number of pending scan versions
    private final int MAX_RANGE_TO_APPEND = (int)(0.2*Chunk.MAX_ITEMS);
    private Chunk<K,V> lastCheckedForAppend = null;



    public List<Chunk<K,V>> compact(List<Chunk<K,V>> frozenChunks, ScanIndex<K> scanIndex)
    {
        ListIterator<Chunk<K,V>> iterFrozen = frozenChunks.listIterator();

        Chunk<K,V> firstFrozen = iterFrozen.next();
        Chunk<K,V> currFrozen = firstFrozen;
        Chunk<K,V> currCompacted = firstFrozen.newChunk(firstFrozen.minKey);

        int oi = firstFrozen.getFirstItemOrderId();

        List<Chunk<K,V>> compacted = new LinkedList<>();

        while(true)
        {
            oi = currCompacted.copyPart(currFrozen, oi, LOW_THRESHOLD, scanIndex);

            // if completed reading curr freezed chunk
            if(oi == Chunk.NONE)
            {
                if(!iterFrozen.hasNext())
                    break;

                currFrozen = iterFrozen.next();
                oi = currFrozen.getFirstItemOrderId();
            }
            else // filled compacted chunk up to LOW_THRESHOLD
            {
                List<Chunk<K,V>> frozenSuffix = frozenChunks.subList(iterFrozen.previousIndex(), frozenChunks.size());

                // try to look ahead and add frozen suffix
                if(canAppendSuffix(oi, frozenSuffix, MAX_RANGE_TO_APPEND))
                {
                    completeCopy(currCompacted, oi, frozenSuffix, scanIndex);
                    break;

                } else
                {
                    Chunk<K,V> c = firstFrozen.newChunk(currFrozen.readKey(oi));
                    currCompacted.next.set(c,false);

                    compacted.add(currCompacted);
                    currCompacted = c;
                }
            }

        }

        compacted.add(currCompacted);

        return compacted;
    }

    private boolean canAppendSuffix(int oi, List<Chunk<K,V>> frozenSuffix, int maxCount)
    {
        MultiChunkIterator iter = new MultiChunkIterator(oi, frozenSuffix);
        int counter = 1;

        while(iter.hasNext() && counter < maxCount)
        {
            iter.next();
            counter++;
        }

        return counter < maxCount;
    }

    private void completeCopy(Chunk<K,V> dest, int oi, List<Chunk<K,V>> srcChunks, ScanIndex<K> scanIndex)
    {
        Iterator<Chunk<K,V>> iter = srcChunks.iterator();
        Chunk<K,V> src = iter.next();
        dest.copyPart(src,oi, Chunk.MAX_ITEMS, scanIndex);

        while(iter.hasNext())
        {

            src = iter.next();
            oi = src.getFirstItemOrderId();
            dest.copyPart(src,oi, Chunk.MAX_ITEMS, scanIndex);
        }
    }

}

