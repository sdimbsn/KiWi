package kiwi;

import java.util.List;

/**
 * Created by dbasin on 11/30/15.
 * Iterator used to iterate over items of multiple chunks.
 */
public class MultiChunkIterator<K extends Comparable<? super K>,V> {
    private Chunk<K,V> first;
    private Chunk<K,V> last;
    private Chunk<K,V> current;
    private Chunk<K,V>.ItemsIterator iterCurrItem;
    private boolean hasNextInChunk;

    private MultiChunkIterator() {}

    /***
     *
     * @param chunks - Range of chunks to be iterated
     */
    public MultiChunkIterator(List<Chunk<K,V>> chunks)
    {
        if(chunks == null || chunks.size() == 0) throw new IllegalArgumentException("Iterator should have at least one item");
        first = chunks.get(0);
        last = chunks.get(chunks.size() -1);
        current = first;
        iterCurrItem = current.itemsIterator();
        hasNextInChunk = iterCurrItem.hasNext();
    }

    public MultiChunkIterator(int oi, List<Chunk<K, V>> chunks) {
        if(chunks == null || chunks.size() == 0) throw new IllegalArgumentException("Iterator should have at least one item");
        first = chunks.get(0);
        last = chunks.get(chunks.size() -1);
        current = first;

        iterCurrItem = current.itemsIterator(oi);
        hasNextInChunk = iterCurrItem.hasNext();
    }


    public boolean hasNext() {
        if(iterCurrItem.hasNext()) return true;

        // cache here the information to improve next()'s performance
        hasNextInChunk = false;

        Chunk<K,V> nonEmpty =  findNextNonEmptyChunk();
        if(nonEmpty == null) return false;

        return true;
    }

    private Chunk<K,V> findNextNonEmptyChunk() {
        if(current == last) return null;

        Chunk<K,V> chunk = current.next.getReference();
        if(chunk == null) return null;

        while(chunk != null)
        {
            Chunk.ItemsIterator iter =  chunk.itemsIterator();
            if(iter.hasNext()) return chunk;

            if(chunk == last) return null;

            chunk = chunk.next.getReference();
        }

        return null;
    }

    /**
     * After next() iterator points to some item.
     * The item's Key, Value and Version can be fetched by corresponding getters.
     */
    public void next() {
        if(hasNextInChunk)
        {
            iterCurrItem.next();
            return;
        }

        Chunk<K,V> nonEmpty = findNextNonEmptyChunk();

        current = nonEmpty;

        iterCurrItem = nonEmpty.itemsIterator();
        iterCurrItem.next();

        hasNextInChunk = iterCurrItem.hasNext();
    }


    public K getKey() {
        return iterCurrItem.getKey();
    }


    public V getValue() {
        return iterCurrItem.getValue();
    }


    public int getVersion() {
        return iterCurrItem.getVersion();
    }

    /***
     * Fetches VersionsIterator to iterate versions of current key.
     * Will help to separate versions and keys data structures in future optimizations.
     * @return VersionsIterator object
     */
    public Chunk.ItemsIterator.VersionsIterator versionsIterator()
    {
        return iterCurrItem.versionsIterator();
    }

    /***
     *
     * @return A copy  with the same state.
     */
    public MultiChunkIterator<K, V> cloneIterator() {
        MultiChunkIterator<K,V> newIterator = new MultiChunkIterator<>();
        newIterator.first = this.first;
        newIterator.last = this.last;
        newIterator.current = this.current;
        newIterator.hasNextInChunk = this.hasNextInChunk;

        newIterator.iterCurrItem = iterCurrItem.cloneIterator();

        return  newIterator;
    }
}
