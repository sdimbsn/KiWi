package kiwi;
import java.util.List;

/**
 * Created by dbasin on 12/1/15.
 */
public interface Compactor<K extends Comparable<K>,V> {
    List<Chunk<K, V>> compact(List<Chunk<K, V>> frozenChunks, ScanIndex<K> scanIndex);
}
