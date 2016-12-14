package kiwi;

import java.util.Iterator;

/**
 * Created by dbasin on 11/25/15.
 */
public interface ChunkIterator<K extends Comparable<? super K>, V> {
    Chunk<K,V> getNext(Chunk<K,V> chunk);
    Chunk<K,V> getPrev(Chunk<K,V> chunk);
}
