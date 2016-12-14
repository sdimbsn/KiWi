package kiwi;

import kiwi.ThreadData.PutData;
import sun.misc.Unsafe;

import java.lang.reflect.Constructor;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicMarkableReference;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by msulamy on 7/27/15.
 */
public abstract class Chunk<K extends Comparable<? super K>,V>
{
	/***************	Constants			***************/
	protected static final int		NONE = 0;	// constant for "no version", "no index", etc. MUST BE 0!
	protected static final int		FREEZE_VERSION	= 1;
	protected static final int 		CANCELED_REMOVE_NEXT = -1;
	
	// order_size(4) = next + version + key + data
	protected static final int		ORDER_SIZE = 4;		// # of fields in each item of order array
	protected static final int		OFFSET_NEXT = 0;
	protected static final int		OFFSET_VERSION = 1;	// POSITIVE means item is linked, otherwise might not be linked yet
	protected static final int		OFFSET_KEY = 2;
	protected static final int		OFFSET_DATA = 3;
	
	// location of the first (head) node - just a next pointer
	private static final int		HEAD_NODE = 0;
	// index of first item in order-array, after head (not necessarily first in list!)
	private static final int		FIRST_ITEM = 1;
	
	public static int				MAX_ITEMS = 4500;
	public static boolean			ALLOW_DUPS = true;
	//private static final int		MIN_ITEMS = 0;

	/***************	Members				***************/
	static final Unsafe					unsafe;
	private final int[]							orderArray;	// array is initialized to 0, i.e., NONE - this is important!
	protected final Object[]							dataArray;

	private final AtomicInteger					orderIndex;	// points to next free index of order array
	private final AtomicInteger					dataIndex;	// points to next free index of data array 

	public K									minKey;		// minimal key that can be put in this chunk

	public AtomicMarkableReference<Chunk<K,V>>	next;
	public AtomicReference<Rebalancer<K,V>>		rebalancer;	//

	public Chunk<K,V>							creator;	// in split/compact process, represents parent of split (can be null!)
	public AtomicReference<List<Chunk<K,V>>> 	children;

	protected int									sortedCount;// # of sorted items at order-array's beginning (resulting from split)

	private Statistics statistics;
	private int orderIndexSerial;
	private int dataIndexSerial;

	private PutData<K,V>[] putArray;
	private static final int PAD_SIZE = 100;

	public final int getOrderIndexSerial()
	{
		return orderIndexSerial;
	}

	public final int getFirstItemOrderId()
	{
		return get(HEAD_NODE,OFFSET_NEXT);
	}

	public final boolean isFreezed() {
		return orderIndex.get() >= orderArray.length;
	}

	public final boolean tryFreezeItem(final int oi) {
		return cas(oi,OFFSET_VERSION, NONE, FREEZE_VERSION);
	}

	public abstract int copyValues(Object[] result, final int idx, final int myVer, final K min, final K max, final SortedMap<K,PutData<K,V>> items);

	/** this method is used by scan operations (ONLY) to help pending put operations set a version
	 * @return sorted map of items matching key range of any currently-pending put operation */

	public SortedMap<K,PutData<K,V>> helpPutInScan(int myVersion, K min, K max)
	{
		SortedMap<K,PutData<K,V>> items = new TreeMap<>();

		// go over thread data of all threads
		for (int i = 0; i < KiWi.MAX_THREADS; ++i)
		{
			// make sure data is for a Put operatio
			PutData<K,V> currPut = putArray[pad(i)];
			if (currPut == null)
				continue;

			// if put operation's key is not in key range - skip it
			K currKey = readKey(currPut.orderIndex);
			if ((currKey.compareTo(min) < 0) || (currKey.compareTo(max) > 0))
				continue;

			// read the current version of the item
			int currVer = getVersion(currPut.orderIndex);

			// if empty, try to set to my version
			if (currVer == NONE)
				currVer = setVersion(currPut.orderIndex, myVersion);

			// if item is frozen or beyond my version - skip it
			if ((currVer == Chunk.FREEZE_VERSION) || (currVer > myVersion))
				continue;

			// get found item matching current key
			PutData<K,V> item = items.get(currKey);

			// is there such an item we previously found? check if we need to replace it
			if (item != null)
			{
				// get its version
				int itemVer = getVersion(item.orderIndex);

				// existing item is newer - don't replace
				if (itemVer > currVer)
				{
					continue;
				}
				// if same versions - continue checking (otherwise currVer is newer so will replace item)
				else if (itemVer == currVer)
				{
					// same chunk & version but items's index is larger - don't replace
					if (item.orderIndex > currPut.orderIndex)
						continue;
				}

			}

			// if we've reached here then curr is newer than item, and we replace it
			items.put(currKey, currPut);
		}

		return items;
	}


	private final int pad(int idx)
	{
		return (PAD_SIZE + idx*PAD_SIZE);
	}


	/** this method is used by get operations (ONLY) to help pending put operations set a version
	 * @return newest item matching myKey of any currently-pending put operation */
	public PutData<K,V> helpPutInGet(int myVersion, K myKey)
	{
		// marks the most recent put that was found in the thread-array
		PutData<K,V> newestPut = null;
		int newestVer = Chunk.NONE;

		// go over thread data of all threads
		for (int i = 0; i < KiWi.MAX_THREADS; ++i)
		{
			// make sure data is for a Put operation
			PutData<K,V> currPut = putArray[pad(i)];
			if (currPut == null)
				continue;

			// if put operation's key is not same as my key - skip it
			K currKey = readKey(currPut.orderIndex);
			if (currKey.compareTo(myKey) != 0)
				continue;

			// read the current version of the item
			int currVer = getVersion(currPut.orderIndex);

			// if empty, try to set to my version
			if (currVer == Chunk.NONE)
				currVer = setVersion(currPut.orderIndex, myVersion);

			// if item is frozen - skip it
			if (currVer == Chunk.FREEZE_VERSION)
				continue;

			// current item has newer version than newest item - replace
			if (currVer > newestVer)
			{
				newestVer = currVer;
				newestPut = currPut;
			}
			// same version for both item - check according to chunk
			else if (currVer == newestVer)
			{
					// same chunk & version but current's index is larger - it is newer
					if (currPut.orderIndex > newestPut.orderIndex)
						newestPut = currPut;
			}
		}

		// return item if its chunk.child1 is null, otherwise return null
		if ((newestPut == null) || (isRebalanced()))
			return null;
		else
			return newestPut;
	}


	/** publish data into thread array - use null to clear **/
	public void publishPut(PutData<K,V> data)
	{
		// get index of current thread
		// since thread IDs are increasing and changing, we assume threads are created one after another (sequential IDs).
		// thus, (ThreadID % MAX_THREADS) will return a unique index for each thread in range [0, MAX_THREADS)
		int idx = (int) (Thread.currentThread().getId() % KiWi.MAX_THREADS);

		// TODO verify the assumption about sequential IDs

		// publish into thread array
		putArray[pad(idx)] = data;
		Chunk.unsafe.storeFence();

	}

	public void debugCalcCounters(DebugStats ds) {
		ItemsIterator iter = itemsIterator();
		ds.sortedCells += sortedCount;
		ds.occupiedCells += orderIndex.get()/ORDER_SIZE;

		int curr = NONE;
		int prev = NONE;
		int DATA_SIZE = 1;

		int prevDataId = NONE;
		int currDataId = NONE;

		K key = null;
		K prevKey = null;

		while(iter.hasNext()) {

			iter.next();
			prev = curr;
			curr = iter.current;
			prevDataId = currDataId;
			currDataId = get(curr, OFFSET_DATA);

			if (prev + ORDER_SIZE != curr)
				ds.jumpKeyCount++;

			if (Math.abs(prevDataId) + DATA_SIZE != Math.abs(currDataId))
				ds.jumpValCount++;

			prevKey = key;
			key = iter.getKey();

			V val = iter.getValue();
			int version = iter.getVersion();

			if (val == null)
				ds.nulItemsCount++;

			ds.itemCount++;

			if (prevKey != null){
				if (key.compareTo(prevKey) == 0) {
					ds.duplicatesCount++;
				} else if (val == null) {
					ds.removedItems++;
				}
			}


		}
	}

	/***
	 * The class contains approximate information about chunk utilization.
	 */
	public class Statistics
	{
		private AtomicInteger dupsCount = new AtomicInteger(0);
		/***
		 *
		 * @return Maximum number of items the chunk can hold
         */
		public int getMaxItems()
		{
			return Chunk.this.MAX_ITEMS;
		}

		/***
		 *
		 * @return Number of items inserted into the chunk
         */
		public int getFilledCount()
		{
			return Chunk.this.orderIndex.get()/Chunk.this.ORDER_SIZE;
		}

		/***
		 *
		 * @return Approximate number of items chunk may contain after compaction.
         */
		public int getCompactedCount()
		{
			return getFilledCount() - getDuplicatesCount();
		}

		public void incrementDuplicates()
		{
			dupsCount.addAndGet(1);
		}

		public int getDuplicatesCount()
		{
			return dupsCount.get();
		}
	}

	/***************	Constructors		***************/
	/**
	 * Create a new chunk
	 * @param minKey	minimal key to be placed in chunk, used by KiWi
	 * @param dataItemSize	expected avg. size (in BYTES!) of items in data-array. can be an estimate
	 */
	public Chunk(K minKey, int dataItemSize, Chunk<K,V> creator)
	{

		// allocate space for head item (only "next", starts pointing to NONE==0)
		this.orderIndex = new AtomicInteger(FIRST_ITEM);
		this.dataIndex = new AtomicInteger(FIRST_ITEM);	// index 0 in data is "NONE"
		this.orderIndexSerial = FIRST_ITEM;
		this.dataIndexSerial = FIRST_ITEM;

		// allocate space for MAX_ITEMS, and add FIRST_ITEM (size of head) for order array
		//this.orderArray = new AtomicIntegerArray(MAX_ITEMS * ORDER_SIZE + FIRST_ITEM);	// initialized to 0, i.e., NONE
		this.orderArray = new int[MAX_ITEMS * ORDER_SIZE + FIRST_ITEM];
		this.dataArray = new Object[MAX_ITEMS+ 1];
		this.putArray = new PutData[KiWi.MAX_THREADS * (PAD_SIZE + 1)];

		this.children = new AtomicReference<>(null);

		this.next = new AtomicMarkableReference<>(null, false);
		this.minKey = minKey;
		this.creator = creator;
		this.sortedCount = 0;		// no sorted items at first
		this.rebalancer = new AtomicReference<Rebalancer<K,V>>(null); // to be updated on rebalance
		this.statistics = new Statistics();

		// TODO allocate space for minKey inside chunk (pointed by skiplist)?
	}
	
	/** static constructor - access and create a new instance of Unsafe */
	static
	{
		try
		{
			Constructor<Unsafe> unsafeConstructor = Unsafe.class.getDeclaredConstructor();
			unsafeConstructor.setAccessible(true);
			unsafe = unsafeConstructor.newInstance();
		}
		catch (Exception ex)
		{
			throw new RuntimeException(ex);
		}
	}
	
	/***************	Abstract Methods	***************/
	public abstract int				allocate(K key, V data);
	public abstract int 			allocateSerial(int key, V data);


	public abstract K				readKey(int orderIndex);
	public abstract Object readData(int orderIndex, int dataIndex);

	/** should CLONE minKey as needed */
	public abstract Chunk<K,V>	newChunk(K minKey);

	/***************	Methods				***************/

	public void finishSerialAllocation()
	{
		orderIndex.set(orderIndexSerial);
		dataIndex.set(dataIndexSerial);
	}

	public class ItemsIterator
	{
		int current;
		VersionsIterator iterVersions;

		public ItemsIterator() {
			current = HEAD_NODE;
			iterVersions = new VersionsIterator();
		}

		final public boolean hasNext() {
				return get(current, OFFSET_NEXT) != NONE;
		}

		final public void next() {
			current = get(current, OFFSET_NEXT);
			iterVersions.justStarted = true;
		}

		final public K getKey() {
			return readKey(current);
		}

		final public V getValue() {
			return getData(current);
		}

		final public int getVersion() {
			return Chunk.this.getVersion(current);
		}

		final public ItemsIterator cloneIterator() {
			ItemsIterator newIterator = new ItemsIterator();
			newIterator.current = this.current;
			return newIterator;
		}

		final public VersionsIterator versionsIterator() {
			return iterVersions;
		}

		public class VersionsIterator
		{
			boolean justStarted = true;


			final public V getValue() {
				return getData(current);
			}


			final public int getVersion() {
				return Chunk.this.getVersion(current);
			}


			final public boolean hasNext() {
				if(justStarted) return true;
				int next = get(current,OFFSET_NEXT);

				if(next == Chunk.NONE) return false;
				return readKey(current).compareTo(readKey(next)) == 0;
			}


			final public void next() {
				if(justStarted)
				{
					justStarted = false;
					return;
				}
				else
				{
					current = get(current,OFFSET_NEXT);
				}
			}
		}

	}

	public ItemsIterator itemsIterator()
	{
		return new ItemsIterator();
	}

	public ItemsIterator itemsIterator(int oi)
	{
		ItemsIterator iter = new ItemsIterator();
		iter.current = oi;

		return iter;
	}

	/** gets the data for the given item, or 'null' if it doesn't exist */
	public V getData(int orderIndex)
	{
		// get index of data in data-array (abs- it might be negative)
		int di = get(orderIndex, OFFSET_DATA);
		
		// if no data for item - return null
		if (di < 0)
			return null;
		else
			return (V)readData(orderIndex, di);
	}

	public boolean isInfant()
	{
		return creator != null;
	}

	public boolean isRebalanced()
	{
		Rebalancer<K,V> r = getRebalancer();
		if(r == null) return false;

		return r.isCompacted();

	}
	/** gets the field of specified offset for given item */
	protected int get(int item, int offset)
	{
		return orderArray[item+offset];
	}
	/** sets the field of specified offset to 'value' for given item */
	protected void set(int item, int offset, int value)
	{
		orderArray[item+offset] = value;
	}

	/** performs CAS from 'expected' to 'value' for field at specified offset of given item */
	private boolean cas(int item, int offset, int expected, int value)
	{
		return unsafe.compareAndSwapInt(orderArray,
			Unsafe.ARRAY_INT_BASE_OFFSET + (item + offset) * Unsafe.ARRAY_INT_INDEX_SCALE,
			expected, value);
	}
	
	/** binary search for largest-entry smaller than 'key' in sorted part of order-array.
	 * @return the index of the entry from which to start a linear search -
	 * if key is found, its previous entry is returned! */
	private int binaryFind(K key)
	{
		// if there are no sorted keys,or the first item is already larger than key -
		// return the head node for a regular linear search
		if ((sortedCount == 0) || (readKey(FIRST_ITEM).compareTo(key) >= 0))
			return HEAD_NODE;
		
		// TODO check last key to avoid binary search?

		int start = 0;
		int end = sortedCount;
		
		while (end - start > 1)
		{
			int curr = start + (end - start) / 2;
			
			if (readKey(curr * ORDER_SIZE + FIRST_ITEM).compareTo(key) >= 0)
				end = curr;
			else
				start = curr;
		}
		
		return start * ORDER_SIZE + FIRST_ITEM;
	}

	/***
	 * Engage the chunk to a rebalancer r.
	 *
	 * @param r -- a rebalancer to engage with
	 * @return rebalancer engaged with the chunk
     */
	public Rebalancer engage(Rebalancer r)
	{
		rebalancer.compareAndSet(null,r);
		return rebalancer.get();
	}

	/***
	 * Checks whether the chunk is engaged with a given rebalancer.
	 * @param r -- a rebalancer object. If r is null, verifies that the chunk is not engaged to any rebalancer
	 * @return true if the chunk is engaged with r, false otherwise
     */
	public boolean isEngaged(Rebalancer r)
	{
		return rebalancer.get() == r;
	}

	/***
	 * Fetch a rebalancer engaged with the chunk.
	 * @return rebalancer object or null if not engaged.
     */
	public Rebalancer getRebalancer()
	{
		return rebalancer.get();
	}

	/***
	 *
	 * @return statistics object containing approximate utilization information.
     */
	public Statistics getStatistics()
	{
		return statistics;
	}

	/** marks this chunk's next pointer so this chunk is marked as deleted
	 * @return the next chunk pointed to once marked (will not change) */
	public Chunk<K,V> markAndGetNext()
	{
		// new chunks are ready, we mark frozen chunk's next pointer so it won't change
		// since next pointer can be changed by other split operations we need to do this in a loop - until we succeed
		while (true)
		{
			// if chunk is marked - that is ok and its next pointer will not be changed anymore
			// return whatever chunk is set as next
			if (next.isMarked())
			{
				return next.getReference();
			}
			// otherwise try to mark it
			else
			{
				// read chunk's current next
				Chunk<K,V> savedNext = next.getReference();
				
				// try to mark next while keeping the same next chunk - using CAS
				// if we succeeded then the next pointer we remembered is set and will not change - return it
				if (next.compareAndSet(savedNext, savedNext, false, true))
					return savedNext;
			}
		}
	}
	
	/** freezes chunk so no more changes can be done in it. also marks pending items as frozen
	 * @return number of items in this chunk */
	public void freeze()
	{
		int numItems = 0;

		// prevent new puts to the chunk
		orderIndex.addAndGet(orderArray.length);

		// go over thread data of all threads
		for (int i = 0; i < KiWi.MAX_THREADS; ++i) {
			// make sure data is for a Put operatio
			PutData<K, V> currPut = putArray[pad(i)];
			if (currPut == null)
				continue;
/*
			if(currPut.chunk != this)
			{
				continue;
			}
*/
			int idx = currPut.orderIndex;
			int version = getVersion(idx);

			// if item is frozen, ignore it - so only handle non-frozen items
			if (version != FREEZE_VERSION)
			{
				// if item has no version, try to freeze it
				if (version == NONE)
				{
					// set version to FREEZE so put op knows to restart
					// if succeded - item will not be in this chunk, we can continue to next item
					if ((cas(idx, OFFSET_VERSION, NONE, FREEZE_VERSION)) ||
							(getVersion(idx) == FREEZE_VERSION))
					{
						continue;
					}
				}

				// if we reached here then item has a version - we need to help by adding item to chunk's list
				// we need to help the pending put operation add itself to the list before proceeding
				// to make sure a frozen chunk is actually frozen - all items are fully added
				addToList(idx, readKey(idx));

			}
		}

	}
	
	/** finds and returns the index of the first item that is equal or larger-than the given min key
	 * with max version that is equal or less-than given version.
	 * returns NONE if no such key exists */
	public int findFirst(K minKey, int version)
	{
		// binary search sorted part of order-array to quickly find node to start search at
		// it finds previous-to-key so start with its next
		int curr = get(binaryFind(minKey), OFFSET_NEXT);
		
		// iterate until end of list (or key is found)
		while (curr != NONE)
		{
			K key = readKey(curr);

			// if item's key is larger or equal than min - we've found a matching key
			if (key.compareTo(minKey) >= 0)
			{
				// check for valid version
				if (getVersion(curr) <= version)
				{
					return curr;
				}
			}
			
			curr = get(curr, OFFSET_NEXT);
		}
		
		return NONE;
	}
	
	/** returns the index of the first item in this chunk with a version <= version */
	public int getFirst(int version)
	{
		int curr = get(HEAD_NODE, OFFSET_NEXT);
		
		// iterate over all items
		while (curr != NONE)
		{
			// if current item is of matching version - return it
			if (getVersion(curr) <= version)
			{
				return curr;
			}
			
			// proceed to next item
			curr = get(curr, OFFSET_NEXT);
		}
		return NONE;
	}
	
	final public int findNext(int curr, int version, K key)
	{
		curr = get(curr, OFFSET_NEXT);
		
		while (curr != NONE)
		{
			K currKey = readKey(curr);
			
			// if in a valid version, and a different key - found next item
			if ((currKey.compareTo(key) != 0) && (getVersion(curr) <= version))
			{
				return curr;
			}
			
			// otherwise proceed to next
			curr = get(curr, OFFSET_NEXT);
		} 
		
		return NONE;
	}
	
	/** finds and returns the value for the given key, or 'null' if no such key exists */
	public V find(K key, PutData<K,V> item)
	{
		// binary search sorted part of order-array to quickly find node to start search at
		// it finds previous-to-key so start with its next
		int curr = get(binaryFind(key), OFFSET_NEXT);

		// iterate until end of list (or key is found)
		while (curr != NONE)
		{
			// compare current item's key to searched key
			int cmp = readKey(curr).compareTo(key);
			
			// if item's key is larger - we've exceeded our key
			// it's not in chunk - no need to search further
			if (cmp > 0)
				return null;
			// if keys are equal - we've found the item
			else if (cmp == 0)
				return chooseNewer(curr, item);
			// otherwise- proceed to next item
			else
				curr = get(curr, OFFSET_NEXT);
		}
		
		return null;
	}
	
	private V chooseNewer(int item, PutData<K,V> pd)
	{
		// if pd is empty or in different chunk, then item is definitely newer
		// it's true since put() publishes after finding a chunk, and get() finds chunk only after reading thread-array
		// so get() definitely sees the same chunks put() sees, or NEWER chunks
		if ((pd == null)
				//|| (pd.chunk != this)
				)
			return getData(item);
		
		// if same chunk then regular comparison (version, then orderIndex)
		int itemVer = getVersion(item);
		int dataVer = getVersion(pd.orderIndex);

		if (itemVer > dataVer)
			return getData(item);
		else if (dataVer > itemVer)
			return getData(pd.orderIndex);
		else
			// same version - return latest item by order in order-array
			return getData(Math.max(item, pd.orderIndex));
	}
	
	/** add the given item (allocated in this chunk) to the chunk's linked list
	 * @param orderIndex index of item in order-array
	 * @param key given for convenience */
	public final void addToList(final int orderIndex, K key)
	{
		int prev, curr;
		int ancor = -1;

		// retry adding to list until successful
		// no items are removed from list - so we don't need to restart on failures
		// so if we CAS some node's next and fail, we can continue from it
		// --retry so long as version is negative (which means item isn't in linked-list)
		while (get(orderIndex, OFFSET_VERSION) < 0)
		{
			// remember next pointer in entry we're trying to add
			int savedNext = get(orderIndex, OFFSET_NEXT);
			
			 // start iterating from quickly-found node (by binary search) in sorted part of order-array
			if(ancor == -1) ancor = binaryFind(key);
			curr = ancor;

			int cmp = -1;

			// iterate items until key's position is found
			while (true)
			{
				prev = curr;
				curr = get(prev, OFFSET_NEXT);	// index of next item in list
				
				// if no item, done searching - add to end of list
				if (curr == NONE)
					break;
				
				// if found item we're trying to insert - already inserted by someone else, so we're done
				if (curr == orderIndex)
					return;
					//TODO also update version to positive?
				
				// compare current item's key to ours
				cmp = readKey(curr).compareTo(key);
				
				// if current item's key is larger, done searching - add between prev and curr
				if (cmp > 0)
					break;

				// if same key - check according to version and location in array
				if (cmp == 0)
				{
					// if duplicate values aren't allowed - do not add value
					if (!ALLOW_DUPS) {
						return;
					}

					int verMine = getVersion(orderIndex);
					int verNext = getVersion(curr);
					
					// if current item's version is smaller, done searching - larger versions are first in list
					if (verNext < verMine)
						break;
					
					// same versions but i'm later in chunk's array - i'm first in list
					if (verNext == verMine)
					{
						int newDataIdx = get(orderIndex, OFFSET_DATA);
						int oldDataIdx = get(curr, OFFSET_DATA);

						while((Math.abs(newDataIdx) > Math.abs(oldDataIdx)) && !cas(curr,OFFSET_DATA,oldDataIdx, newDataIdx)) {
							oldDataIdx = get(curr,OFFSET_DATA);
						}

						return;
					}
				}
			}

			if(savedNext == CANCELED_REMOVE_NEXT) return;
			if(cmp != 0 && savedNext == NONE && get(orderIndex, OFFSET_DATA) < 0)
			{
				if(cas(orderIndex,OFFSET_NEXT,savedNext, CANCELED_REMOVE_NEXT))
					return;
				else
					continue;
			}

			// try to CAS update my next to current item ("curr" variable)
			// using CAS from saved next since someone else might help us
			// and we need to avoid race conditions with other put ops and helpers
			if (cas(orderIndex, OFFSET_NEXT, savedNext, curr))
			{
				// try to CAS curr's next to point from "next" to me
				// if successful - we're done, exit loop. Otherwise retry (return to "while true" loop)
				if (cas(prev, OFFSET_NEXT, curr, orderIndex))
				{
					// if some CAS failed we restart, if both successful - we're done
					// update version to positive (getVersion() always returns positive number) to mark item is linked
					set(orderIndex, OFFSET_VERSION, getVersion(orderIndex));

					// if adding version for existing key -- update duplicates statistics
					if(cmp == 0)
					{
						statistics.incrementDuplicates();
					}

					break;
				}
			}
		}
	}

	private boolean casData(int curr, Object currData, Object data) {

		return unsafe.compareAndSwapObject(dataArray,
				Unsafe.ARRAY_OBJECT_BASE_OFFSET + curr * Unsafe.ARRAY_OBJECT_INDEX_SCALE, currData,
				data);

	}


	/***
	 * Appends a new item to the end of items array. The function assumes that the array is sorted in ascending order by (Key, -Version)
	 * The method is not thread safe!!!  Should be called for  chunks accessible by single thread only.
	 *
	 * @param key the key of the new item
	 * @param value the value of the new item
	 * @param version the version of the new item
     */
	public final void appendItem(int key, V value, int version)
	{
		int oiDest = allocateSerial(key,value);

		// update to item's version (since allocation gives NONE version)
		// version is positive so item is marked as linked
		set(oiDest, Chunk.OFFSET_VERSION, version);

		// update binary searches range
		sortedCount++;

		// handle adding of first item to empty chunk
		int prev = oiDest - ORDER_SIZE;
		if(prev < 0) {
			set(HEAD_NODE,OFFSET_NEXT,oiDest);
			return;
		}

		// updated dest chunk's linked list with new item
		set(prev, OFFSET_NEXT, oiDest);

	}

	public int getNumOfItems()
	{
		return orderIndex.get()/ORDER_SIZE;
	}

	public final int getNumOfItemsSerial()
	{
		return orderIndexSerial/ORDER_SIZE;
	}

	/***
	 * Copies items from srcChunk performing compaction on the fly.
	 * @param srcChunk -- chunk to copy from
	 * @param oi -- start position for copying
	 * @param maxCapacity -- max number of items "this" chunk can contain after copy
     * @return order index of next to the last copied item, NONE if all items were copied
     */
	public final int copyPart(Chunk<K,V> srcChunk, int oi, int maxCapacity, ScanIndex<K> scanIndex)
	{


		int maxIdx = maxCapacity*ORDER_SIZE + 1;

		if(orderIndexSerial >= maxIdx) return oi;

		assert oi < orderArray.length - ORDER_SIZE;

		if(orderIndexSerial != FIRST_ITEM) {
			set(orderIndexSerial - ORDER_SIZE, OFFSET_NEXT, orderIndexSerial);
		} else
		{
			set(HEAD_NODE,OFFSET_NEXT,FIRST_ITEM);
		}

		int sortedSize = srcChunk.sortedCount*ORDER_SIZE + 1;
		int orderStart = oi;
		int orderEnd = orderStart - 1;

		int currKey = NONE;
		int prevKey = Integer.MIN_VALUE;

		int currDataId = NONE;
		int prevDataId = NONE;

		int currVersion = NONE;

		int oiPrev = NONE;

		boolean isFirst = true;

		while(true)
		{
			currKey = srcChunk.get(oi,OFFSET_KEY);
			currDataId = srcChunk.get(oi, OFFSET_DATA);

			int itemsToCopy = orderEnd - orderStart + 1;

			if(		(currDataId > 0) &&
					(isFirst
					||
						(
								(oiPrev < sortedSize)
										&&
								(prevKey != currKey)
										&&
								(oiPrev + ORDER_SIZE == oi)
										&&
								(orderIndexSerial +itemsToCopy*ORDER_SIZE <= maxIdx)
									&&
								(prevDataId + 1 == currDataId)
						)))
			{
				orderEnd++;
				isFirst = false;

				prevKey = currKey;
				oiPrev = oi;
				oi = srcChunk.get(oi, OFFSET_NEXT);
				prevDataId = currDataId;

				if(oi != NONE)	continue;

			}

			// copy continuous interval by arrayCopy
			itemsToCopy = orderEnd - orderStart + 1;
			//System.arraycopy(srcChunk.orderArray, orderStart, orderArray, orderIndexSerial, itemsToCopy*ORDER_SIZE );
			if(itemsToCopy > 0) {
				for (int i = 0; i < itemsToCopy; ++i) {
					int offset = i * ORDER_SIZE;
					int oIdx = orderIndexSerial;

					// next should point to the next item
					orderArray[oIdx + offset + OFFSET_NEXT] = oIdx + offset + ORDER_SIZE;
					orderArray[oIdx + offset + OFFSET_VERSION] = Math.abs(srcChunk.orderArray[orderStart + offset + OFFSET_VERSION]);
					orderArray[oIdx + offset + OFFSET_DATA] = dataIndexSerial + i;
					orderArray[oIdx + offset + OFFSET_KEY] = srcChunk.orderArray[orderStart + offset + OFFSET_KEY];
				}

				orderIndexSerial = orderIndexSerial + itemsToCopy * ORDER_SIZE;

				int dataIdx = srcChunk.get(orderStart, OFFSET_DATA);

				if (itemsToCopy == 1) {
					dataArray[dataIndexSerial] = srcChunk.dataArray[dataIdx];
				} else {

					System.arraycopy(srcChunk.dataArray, dataIdx, dataArray, dataIndexSerial, itemsToCopy);
				}

				dataIndexSerial = dataIndexSerial + itemsToCopy;
			}

			scanIndex.reset(currKey);
			//first item already copied or null
			scanIndex.savedVersion(NONE);

			currVersion = srcChunk.getVersion(oi);

			int removedVersion = NONE;

			// the case when we start from deleted item
			if(prevKey != currKey && currDataId < 0) {
				// remove the item if it doesn't have versions to keep
				removedVersion = currVersion;

				// move to next item
				oiPrev = oi;
				prevDataId = currDataId;

				oi = srcChunk.get(oi, OFFSET_NEXT);
				currDataId = srcChunk.get(oi,OFFSET_DATA);

				currVersion = srcChunk.getVersion(oi);
				prevKey = currKey;
				currKey = srcChunk.get(oi, OFFSET_KEY);
			}

			// copy versions of currKey if required by scanIndex, or skip to next key
			while (oi != NONE && prevKey == currKey )
			{
					if(scanIndex.shouldKeep(currVersion) )
					{
						if(currDataId < 0)
						{
							removedVersion = currVersion;
							scanIndex.savedVersion(currVersion);
						}
						else if(currVersion != removedVersion){
							if (removedVersion != NONE) {
								appendItem(currKey, null, removedVersion);
								set(orderIndexSerial - ORDER_SIZE, OFFSET_NEXT, orderIndexSerial);
								scanIndex.savedVersion(removedVersion);
								removedVersion = NONE;
							}


							appendItem(currKey, (V) srcChunk.dataArray[currDataId], currVersion);
							set(orderIndexSerial - ORDER_SIZE, OFFSET_NEXT, orderIndexSerial);
							scanIndex.savedVersion(currVersion);
						}
					}

					oiPrev = oi;
					prevDataId = currDataId;

					oi = srcChunk.get(oi, OFFSET_NEXT);
					currDataId = srcChunk.get(oi,OFFSET_DATA);

					currVersion = srcChunk.getVersion(oi);
					prevKey = currKey;
					currKey = srcChunk.get(oi, OFFSET_KEY);
			}

			if(oi == NONE || orderIndexSerial > maxIdx)
				break;


			orderStart = oi;
			orderEnd = orderStart - 1 ;
			isFirst = true;
		}


		int setIdx = orderIndexSerial > FIRST_ITEM ? orderIndexSerial - ORDER_SIZE : HEAD_NODE;
		set(setIdx,OFFSET_NEXT, NONE);

		orderIndex.set(orderIndexSerial);
		dataIndex.set(dataIndexSerial);
		sortedCount = orderIndexSerial/ORDER_SIZE;

		return oi;
	}

	/** base allocate method for use in allocation by implementing classes
	 * @return index of allocated order-array item (can be used to get data-array index) */
	protected final int baseAllocate(int dataSize)
	{
		// increment order array to get new index in it
		int oi = orderIndex.getAndAdd(ORDER_SIZE);
		if (oi+ORDER_SIZE > orderArray.length)
			return -1;

		// increment data array to get new index in it
		int di = dataIndex.getAndIncrement();
		if (di >= dataArray.length)
			return -1;

		// if there's data - allocate room for it
		// otherwise DATA field of order-item is left as NONE
		// write base item data location (offset of data-array) to order-array
		// since NONE==0, item's version and next are already set to NONE
		di = dataSize > 0? di : -di;

		set(oi, OFFSET_DATA, di);

		// return index of allocated order-array item
		return oi;
	}

	protected final int baseAllocateSerial(int dataSize) {

		int oi = orderIndexSerial;
		orderIndexSerial += ORDER_SIZE;

		if (oi + ORDER_SIZE > orderArray.length)
			return -1;

		// if there's data - allocate room for it
		// otherwise DATA field of order-item is left as NONE
		// increment data array to get new index in it
		int di = dataIndexSerial;
		if (di >= dataArray.length)
			return -1;

		di = dataSize > 0 ? di : -di;

		dataIndexSerial++;

		// write base item data location (offset of data-array) to order-array
		// since NONE==0, item's version and next are already set to NONE
		set(oi, OFFSET_DATA, di);

		// return index of allocated order-array item
		return oi;

	}

	/** gets the current version of the given order-item */
	public int getVersion(int orderIndex)
	{
		return Math.abs(get(orderIndex, OFFSET_VERSION));
	}
	/** tries to set (CAS) the version of order-item to specified version
	 * @return whatever version is successfuly set (by this thread or another)	 */
	public int setVersion(int orderIndex, int version)
	{		
		// try to CAS version from NO_VERSION to desired version
		if (cas(orderIndex, OFFSET_VERSION, NONE, -version))
			return version;
		// if failed (someone else's CAS succeeded) - read version again and return it
		else
			return getVersion(orderIndex);
	}

	public int debugCountKeysTotal()
	{
		int curr = get(HEAD_NODE, OFFSET_NEXT);
		//int prev = curr;
		int keys = 0;

		if (curr != NONE)
		{
			keys = 1;
			curr = get(curr,OFFSET_NEXT);

			while (curr != NONE)
			{
				//if (readKey(curr).compareTo(readKey(prev)) != 0)
				++keys;
				//prev = curr;
				curr = get(curr,OFFSET_NEXT);
			}
		}
		return keys;
	}

	public int debugCountKeys()
	{
		int curr = get(HEAD_NODE, OFFSET_NEXT);
		int prev = curr;
		int keys = 0;
		
		if (curr != NONE)
		{
			keys = 1;
			curr = get(curr,OFFSET_NEXT);
			
			while (curr != NONE)
			{
				if (readKey(curr).compareTo(readKey(prev)) != 0)
					++keys;
				prev = curr;
				curr = get(curr,OFFSET_NEXT);
			}
		}
		return keys;
	}
	public int debugCountDups()
	{
		int curr = get(HEAD_NODE, OFFSET_NEXT);
		int prev = curr;
		int dups = 0;
		
		if (curr != NONE)
		{
			curr = get(curr,OFFSET_NEXT);
			
			while (curr != NONE)
			{
				if (readKey(curr).compareTo(readKey(prev)) == 0)
					++dups;
				prev = curr;
				curr = get(curr,OFFSET_NEXT);
			}
		}
		return dups;
	}

	public void debugCompacted()
	{
		int prevKey = get(FIRST_ITEM,OFFSET_KEY);
		int prevVersion = get(FIRST_ITEM, OFFSET_VERSION);
		int prevNext = get(FIRST_ITEM, OFFSET_NEXT);

		if(get(HEAD_NODE,OFFSET_NEXT) != FIRST_ITEM)
		{
			throw new IllegalStateException();
		}

		for(int oi = FIRST_ITEM + ORDER_SIZE; oi < orderArray.length - ORDER_SIZE; oi += ORDER_SIZE)
		{
			int key = get(oi,OFFSET_KEY);
			int version = get(oi, OFFSET_VERSION);

			if(prevKey >= key)
			{
				throw new IllegalStateException();
			}

			if(prevNext != oi)
			{
				throw new IllegalStateException();
			}
			prevKey = key;
			prevVersion = version;
			prevNext = get(oi, OFFSET_NEXT);

			if(prevNext == NONE) break;
		}
	}

	public void debugPrint()
	{
		System.out.print(this.minKey + " :: ");
		int curr = get(HEAD_NODE, OFFSET_NEXT);
		
		while (curr != NONE)
		{
			System.out.print("(" + readKey(curr) + "," + getData(curr) + "," + curr + ") ");
			curr = get(curr, OFFSET_NEXT);
		}
	}
}
