package kiwi;

import kiwi.ThreadData.PutData;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.SortedMap;

public class ScanIterator<K extends Comparable<? super K>,V> implements Iterator<V>
{
	private final K 							maxKey;	// max key (inclusive) for this scan - beyond it the iterator is finished
	private final int							version;// version for this scan- larger versions are ignored
	private final SortedMap<K,PutData<K,V>>		items;	// items map - for items that are currently added (from thread-array)
	private final Iterator<K>					iter;	// iterator over items map keys
	
	private Chunk<K,V>							chunk;	// current chunk, or 'null' if no more items from chunks
	
	private K									keyItems; // key of current item (next to be returned) in items map
	private K									keyChunk; // key of current item from the chunks
	private int									idxChunk; // index of current item in current chunk
	private V									nextVal;  // next value that should be returned, null if done
	
	public ScanIterator(K min, K max, int version, Chunk<K,V> chunk, SortedMap<K,PutData<K,V>> items)
	{
		this.maxKey = max;
		this.version = version;
		this.items = items;
		
		this.chunk = chunk;		
		this.iter = items.keySet().iterator();
		
		// find first items in the data structure (chunks)
		// get the first index equal or larger-than minKey (matching version)
		idxChunk = chunk.findFirst(min, version);
		handleChunksItem();
		
		// find first item in the thread-array (items map)
		// the items map is a sorted map, so just get the first item-- if it exists
		if (iter.hasNext())
			keyItems = iter.next();
		
		// update next value to hold first actual (non-deleted) value
		updateNextValue();
	}
	
	/** handles current chunk item - proceeding to next chunk if needed (item is NONE), and updating keyChunks */
	private void handleChunksItem()
	{
		while (idxChunk == Chunk.NONE)
		{
			// proceed to next chunk
			chunk = chunk.next.getReference();
			
			// if no next chunk - we're done
			if (chunk == null)
			{
				break;
			}
			// otherwise check first item in chunk
			else
			{
				// if chunk's min key isn't too large - find matching item in it
				if (chunk.minKey.compareTo(maxKey) <= 0)
				{
					idxChunk = chunk.getFirst(version);
				}
				// otherwise we're done, nullify chunk variable
				else
				{
					chunk = null;
					break;
					
				}
			}
		}
		
		// if some item was found, update chunk key variable for "merge-sort" in next() method
		if (chunk != null)
		{
			keyChunk = chunk.readKey(idxChunk);
			
			// make sure we didn't exceed max key
			if (keyChunk.compareTo(maxKey) > 0)
				chunk = null;
		}
	}

	private boolean hasNextValue()
	{
		return ((chunk != null) || (keyItems != null));
	}
	
	@Override
	public boolean hasNext()
	{
		return (nextVal != null);
	}
	
	private void updateNextValue()
	{
		// reset next value to null, to start searching for an actual (non-deleted) next value
		nextVal = null;
		
		// find next value, while skipping null (i.e., deleted) items/values
		while ((nextVal == null) && (hasNextValue()))
		{
			nextVal = getNextValue();
		}
	}

	@Override
	public V next()
	{
		if (nextVal == null)
			throw new NoSuchElementException();
		
		// set to return our next-value
		V retVal = nextVal;
		
		// update next value to actual (non-deleted) next value
		updateNextValue();
		
		// return previously-stored value
		return retVal;
	}
	
	private V getNextValue()
	{
		/*
		// "merge-sort" chunk items and sortedmap items
		// no items (hasNext() is false), so this is an error
		if ((chunk == null) && (keyItems == null))
		{
			return null;
		}
		// items map finished - return from chunks
		else if (keyItems == null)
		{
			return nextFromChunks();
		}
		// chunks items finished - return from items map
		else if (chunk == null)
		{
			return nextFromMap();
		}
		// both have items - compare keys
		else
		{
			int cmp = keyChunk.compareTo(keyItems);
			
			// chunks key is smaller
			if (cmp < 0)
				return nextFromChunks();
			// items map key is smaller
			if (cmp > 0)
				return nextFromMap();
			
			// keys are equal - check versions
			PutData<K,V> pd = items.get(keyItems);
			int verChunk = chunk.getVersion(idxChunk);
			int verItems = pd.chunk.getVersion(pd.orderIndex);
			
			// we move both next since they're the same item - both need to proceed and only one should be returned
			V valChunk = nextFromChunks();
			V valItems = nextFromMap();
			
			// chunk's version is newer
			if (verChunk > verItems)
				return valChunk;
			// items map version is newer
			else if (verChunk < verItems)
				return valItems;
			
			// if PutData's chunk is different- then chunks item is newer
			if (pd.chunk != chunk)
				return valChunk;
			
			// otherwise in same chunk - decide according to item index
			if (idxChunk > pd.orderIndex)
				return valChunk;
			else
				return valItems;
		}
		*/
		throw new NotImplementedException();
	}
	
	private V nextFromMap()
	{
		/*
		PutData<K,V> pd = items.get(keyItems);

		while (true)
		{
			if (iter.hasNext())
			{
				K oldKey = keyItems;
				keyItems = iter.next();
				
				if (oldKey.compareTo(keyItems) != 0)
					break;
			}
			else
			{
				keyItems = null;
				break;
			}
		}
		
		return pd.chunk.getData(pd.orderIndex);
		*/
		throw new NotImplementedException();
	}
	private V nextFromChunks()
	{
		V val = chunk.getData(idxChunk);
		
		// if last key was max, no need to search further
		if (keyChunk.compareTo(maxKey) == 0)
		{
			chunk = null;
		}
		// otherwise find next key in chunks and handle it
		else
		{
			idxChunk = chunk.findNext(idxChunk, version, keyChunk);
			handleChunksItem();
		}

		return val;
	}
	
	@Override
	public void remove()
	{
		throw new NotImplementedException();
	}
}
