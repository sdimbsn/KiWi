package kiwi;

import kiwi.ThreadData.PutData;
import kiwi.ThreadData.ScanData;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

public class KiWi<K extends Comparable<? super K>, V> implements ChunkIterator<K,V>
{
	/*************** Constants ***************/
	public static int MAX_THREADS = 32;
	public static final int PAD_SIZE = 640;
	public static int RebalanceSize = 2;

	/*************** Members ***************/
	private final ConcurrentSkipListMap<K , Chunk<K, V>>	skiplist;		// skiplist of chunks for fast navigation
	protected AtomicInteger 							version;		// current version to add items with
	private final boolean								withScan;		// support scan operations or not (scans add thread-array)
	private final ScanData[]		scanArray;


	/*************** Constructors ***************/
	public KiWi(Chunk<K,V> head)
	{
		this(head, false);
	}
	
	@SuppressWarnings("unchecked")
	public KiWi(Chunk<K,V> head, boolean withScan)
	{
		this.skiplist = new ConcurrentSkipListMap<>();
		this.version = new AtomicInteger(2);	// first version is 2 - since 0 means NONE, and -1 means FREEZE

		this.skiplist.put(head.minKey, head);	// add first chunk (head) into skiplist
		this.withScan = withScan;

		if (withScan) {
			//this.threadArray = new ThreadData[MAX_THREADS];
			this.scanArray = new ScanData[MAX_THREADS * (PAD_SIZE + 1)];
		}
		else {
			//this.threadArray = null;
			this.scanArray = null;
		}
	}

	/*************** Methods ***************/

	public static int pad(int idx)
	{
		return (PAD_SIZE + idx*PAD_SIZE);
	}
	
	public V get(K key)
	{
		// find chunk matching key
		Chunk<K,V> c = skiplist.floorEntry(key).getValue();
		c = iterateChunks(c, key);

		// help concurrent put operations (helpPut) set a version
		PutData<K,V> pd = null;
		pd = c.helpPutInGet(version.get(), key);

		// find item matching key inside chunk
		return c.find(key, pd);
	}

	public void put(K key, V val)
	{
		// find chunk matching key
		Chunk<K,V> c = skiplist.floorEntry(key).getValue();
		
		// repeat until put operation is successful
		while (true)
		{
			// the chunk we have might have been in part of split so not accurate
			// we need to iterate the chunks to find the correct chunk following it
			c = iterateChunks(c, key);
			
			// if chunk is infant chunk (has a parent), we can't add to it
			// we need to help finish compact for its parent first, then proceed
			{
				Chunk<K,V> parent = c.creator;
				if (parent != null) {
					if (rebalance(parent) == null)
						return;
				}
			}

			// allocate space in chunk for key & value
			// this also writes key&val into the allocated space
			int oi = c.allocate(key, val);
			
			// if failed - chunk is full, compact it & retry
			if (oi < 0)
			{
				c = rebalance(c);
				if (c == null)
					return;
				continue;
			}
			
			if (withScan)
			{
				// publish put operation in thread array
				// publishing BEFORE setting the version so that other operations can see our value and help
				// this is in order to prevent us from adding an item with an older version that might be missed by others (scan/get)
				c.publishPut(new PutData<>(c, oi));


				if(c.isFreezed())
				{
					// if succeeded to freeze item -- it is not accessible, need to reinsert it in rebalanced chunk
					if(c.tryFreezeItem(oi)) {
						c.publishPut(null);
						c = rebalance(c);

						continue;
					}
				}
			}
			
			// try to update the version to current version, but use whatever version is successfuly set
			// reading & setting version AFTER publishing ensures that anyone who sees this put op has
			// a version which is at least the version we're setting, or is otherwise setting the version itself
			int myVersion = c.setVersion(oi, this.version.get());
			
			// if chunk is frozen, clear published data, compact it and retry
			// (when freezing, version is set to FREEZE)
			if (myVersion == Chunk.FREEZE_VERSION)
			{
				// clear thread-array item if needed
				c.publishPut(null);
				c = rebalance(c);
				continue;
			}
			
			// allocation is done (and published) and has a version
			// all that is left is to insert it into the chunk's linked list
			c.addToList(oi, key);
			
			// delete operation from thread array - and done
			c.publishPut(null);

			if(shouldRebalance(c))
				rebalance(c);

			break;
		}
	}

	private boolean shouldRebalance(Chunk<K, V> c) {
		// perform actual check only in for pre defined percentage of puts
		if(ThreadLocalRandom.current().nextInt(100) > Parameters.rebalanceProbPerc) return false;

		// if another thread already runs rebalance -- skip it
		if(!c.isEngaged(null)) return false;
		int numOfItems = c.getNumOfItems();

		if((c.sortedCount == 0 && numOfItems << 3 > Chunk.MAX_ITEMS ) ||
				(c.sortedCount > 0 && (c.sortedCount * Parameters.sortedRebalanceRatio) < numOfItems) )
		{
			return true;
		}

		return false;
	}

	public void compactAllSerial()
	{
		Chunk<K,V> c  = skiplist.firstEntry().getValue();
		while(c!= null)
		{
			c = rebalance(c);
			c =c.next.getReference();
		}

		c = skiplist.firstEntry().getValue();

		while(c!= null)
		{
			Chunk.ItemsIterator iter = c.itemsIterator();
			Comparable prevKey = null;
			int prevVersion = 0;

			if(iter.hasNext()) {
				iter.next();
				prevKey = iter.getKey();
				prevVersion = iter.getVersion();
			}

			while(iter.hasNext()) {
				iter.next();

				Comparable key = iter.getKey();
				int version = iter.getVersion();

				int cmp = prevKey.compareTo(key);
				if (cmp >= 0)
				{
					throw new IllegalStateException();
				}
				else if (cmp == 0)
				{
					if(prevVersion < version)
					{
						throw new IllegalStateException();
					}
				}



			}

			c = c.next.getReference();
		}
		return;
	}

	public int scan(V[] result, K min, K max) {
		// get current version and increment version (atomically) for this scan
		// all items beyond my version are ignored by this scan
		// the newVersion() method is used to ensure my version is published correctly,
		// so concurrent split ops will not compact items with this version (ensuring linearizability)
		int myVer = newVersion(min, max);


		// find chunk matching min key, to start iterator there
		Chunk<K,V> c = skiplist.floorEntry(min).getValue();
		c = iterateChunks(c, min);

		int itemsCount = 0;
		while(true)
		{

			if(c == null || c.minKey.compareTo(max)>0)
				break;

			// help pending put ops set a version - and get in a sorted map for use in the scan iterator
			// (so old put() op doesn't suddently set an old version this scan() needs to see,
			//  but after the scan() passed it)
			SortedMap<K,PutData<K,V>> items = c.helpPutInScan(myVer, min, max);

			itemsCount += c.copyValues(result, itemsCount, myVer, min, max, items);
			c = c.next.getReference();
		}

		// remove scan from scan array
		publishScan(null);

		return itemsCount;
	}

	@Override
	public Chunk<K,V> getNext(Chunk<K,V> chunk)
	{
		return chunk.next.getReference();
	}

	@Override
	public Chunk<K,V> getPrev(Chunk<K,V> chunk)
	{
		return null;
/*
		Map.Entry<K, Chunk<K, V>> kChunkEntry = skiplist.lowerEntry(chunk.minKey);
		if(kChunkEntry == null) return null;
		Chunk<K,V> prev = kChunkEntry.getValue();

		while(true)
		{
			Chunk<K,V> next = prev.next.getReference();
			if(next == chunk) break;
			if(next == null) {
				prev = null;
				break;
			}

			prev = next;
		}

		return prev;
*/
	}

	/** fetch-and-add for the version counter. in a separate method because scan() ops need to use
	 * thread-array for this, to make sure concurrent split/compaction ops are aware of the scan() */
	private int newVersion(K min, K max)
	{
		// create new ScanData and publish it - in it the scan's version will be stored
		ScanData sd = new ScanData(min, max);
		publishScan(sd);
		
		// increment global version counter and get latest
		int myVer = version.getAndIncrement();
		
		// try to set it as this scan's version - return whatever is successfuly set
		if (sd.version.compareAndSet(Chunk.NONE, myVer))
			return myVer;
		else
			return sd.version.get();
	}
	
	/** finds and returns the chunk where key should be located, starting from given chunk */
	private Chunk<K,V> iterateChunks(Chunk<K,V> c, K key)
	{
		// find chunk following given chunk (next)
		Chunk<K,V> next = c.next.getReference();
			
		// found chunk might be in split process, so not accurate
		// since skiplist isn't updated atomically in split/compcation, our key might belong in the next chunk
		// next chunk might itself already be split, we need to iterate the chunks until we find the correct one
		while ((next != null) && (next.minKey.compareTo(key) <= 0))
		{
			c = next;
			next = c.next.getReference();
		}
		
		return c;
	}

	private ArrayList<ScanData> getScansArray(int myVersion)
	{

		ArrayList<ScanData> pScans = new ArrayList<>(MAX_THREADS);
		boolean isIncremented = false;
		int ver = -1;

		// read all pending scans
		for(int i = 0; i < MAX_THREADS; ++i)
		{
			ScanData scan = scanArray[pad(i)];
			if(scan != null)  pScans.add(scan);
		}


		for(ScanData sd : pScans)
		{
			if(sd.version.get() == Chunk.NONE)
			{
				if(!isIncremented) {
					// increments version only once
					// if at least one pending scan has no version assigned
					ver = version.getAndIncrement();
					isIncremented = true;
				}

				sd.version.compareAndSet(Chunk.NONE,ver);
			}
		}

		return pScans;
	}

	private TreeSet<Integer> getScans(int myVersion)
	{
		TreeSet<Integer> scans = new TreeSet<>();
		
		// go over thread data of all threads
		for (int i = 0; i < MAX_THREADS; ++i)
		{
			// make sure data is for a Scan operation
			ScanData currScan = scanArray[pad(i)];
			if (currScan == null)
				continue;
			
			// if scan was published but didn't yet CAS its version - help it
			if (currScan.version.get() == Chunk.NONE)
			{
				// TODO: understand if we need to increment here
				int ver = version.getAndIncrement();
				currScan.version.compareAndSet(Chunk.NONE, ver);
			}
			
			// read the scan version (which is now set)
			int verScan = currScan.version.get();
			if (verScan < myVersion)
			{
				scans.add(verScan);
			}
		}
		
		return scans;
	}

	private Chunk<K,V> rebalance(Chunk<K,V> chunk)
	{
		Rebalancer<K,V> rebalancer = new Rebalancer<>(chunk, this);

		rebalancer = rebalancer.engageChunks();

		// freeze all the engaged range.
		// When completed, all update (put, next pointer update) operations on the engaged range
		// will be redirected to help the rebalance procedure
		rebalancer.freeze();

		List<Chunk<K,V>> engaged = rebalancer.getEngagedChunks();
		// before starting compaction -- check if another thread has completed this stage
		if(!rebalancer.isCompacted()) {
			ScanIndex<K> index = updateAndGetPendingScans(version.get(), engaged);
			rebalancer.compact(index);
		}

		// the children list may be generated by another thread

		List<Chunk<K,V>> compacted = rebalancer.getCompactedChunks();


		connectToChunkList(engaged, compacted);
		updateIndex(engaged, compacted);

		return compacted.get(0);
	}

	private ScanIndex updateAndGetPendingScans(int currVersion, List<Chunk<K, V>> engaged) {
		// TODO: implement versions selection by key
		K minKey = engaged.get(0).minKey;
		Chunk<K,V> nextToRange= engaged.get(engaged.size() -1).next.getReference();
		K maxKey =  nextToRange == null ? null : nextToRange.minKey;

		return new ScanIndex(getScansArray(currVersion), currVersion, minKey, maxKey);
	}

	private void updateIndex(List<Chunk<K,V>> engagedChunks, List<Chunk<K,V>> compacted)
	{
		Iterator<Chunk<K,V>> iterEngaged = engagedChunks.iterator();
		Iterator<Chunk<K,V>> iterCompacted = compacted.iterator();

		Chunk<K,V> firstEngaged = iterEngaged.next();
		Chunk<K,V> firstCompacted = iterCompacted.next();

		skiplist.replace(firstEngaged.minKey,firstEngaged, firstCompacted);

		// update from infant to normal
		firstCompacted.creator = null;
		Chunk.unsafe.storeFence();

		// remove all old chunks from index.
		// compacted chunks are still accessible through the first updated chunk
		while(iterEngaged.hasNext())
		{
			Chunk<K,V> engagedToRemove = iterEngaged.next();
			skiplist.remove(engagedToRemove.minKey,engagedToRemove);
		}

		// for simplicity -  naive lock implementation
		// can be implemented without locks using versions on next pointer in  skiplist

		while(iterCompacted.hasNext())
		{
			Chunk<K,V> compactedToAdd = iterCompacted.next();

			synchronized (compactedToAdd)
			{
				skiplist.putIfAbsent(compactedToAdd.minKey,compactedToAdd);
				compactedToAdd.creator = null;
			}
		}
	}

	private void connectToChunkList(List<Chunk<K, V>> engaged, List<Chunk<K, V>> children) {

		updateLastChild(engaged,children);

		Chunk<K,V> firstEngaged = engaged.get(0);

		// replace in linked list - we now need to find previous chunk to our chunk
		// and CAS its next to point to c1, which is the same c1 for all threads who reach this point.
		// since prev might be marked (in compact itself) - we need to repeat this until successful
		while (true)
		{
			// start with first chunk (i.e., head)

			Map.Entry<K,Chunk<K,V>> lowerEntry = skiplist.lowerEntry(firstEngaged.minKey);

			Chunk<K,V> prev = lowerEntry != null ? lowerEntry.getValue() : null;
			Chunk<K,V> curr = (prev != null) ? prev.next.getReference() : null;

			// if didn't succeed to find preve through the skip list -- start from the head
			if(prev == null || curr != firstEngaged) {
				prev = null;
				curr = skiplist.firstEntry().getValue();    // TODO we can store&update head for a little efficiency
				// iterate until found chunk or reached end of list
				while ((curr != firstEngaged) && (curr != null)) {
					prev = curr;
					curr = curr.next.getReference();
				}
			}

			// chunk is head or not in list (someone else already updated list), so we're done with this part
			if ((curr == null) || (prev == null))
				break;

			// if prev chunk is marked - it is deleted, need to help split it and then continue
			if (prev.next.isMarked())
			{
				rebalance(prev);
				continue;
			}

			// try to CAS prev chunk's next - from chunk (that we split) into c1
			// c1 is the old chunk's replacement, and is already connected to c2
			// c2 is already connected to old chunk's next - so all we need to do is this replacement
			if ((prev.next.compareAndSet(firstEngaged, children.get(0), false, false)) ||
					(!prev.next.isMarked()))
				// if we're successful, or we failed but prev is not marked - so it means someone else was successful
				// then we're done with loop
				break;
		}

	}

	private void updateLastChild(List<Chunk<K, V>> engaged, List<Chunk<K, V>> children) {
		Chunk<K,V> lastEngaged = engaged.get(engaged.size()-1);
		Chunk<K,V> nextToLast =  lastEngaged.markAndGetNext();
		Chunk<K,V> lastChild = children.get(children.size() -1);

		lastChild.next.compareAndSet(null, nextToLast, false, false);
	}

	/** publish data into thread array - use null to clear **/
	private void publishScan(ScanData data)
	{
		// get index of current thread
		// since thread IDs are increasing and changing, we assume threads are created one after another (sequential IDs).
		// thus, (ThreadID % MAX_THREADS) will return a unique index for each thread in range [0, MAX_THREADS)
		int idx = (int) (Thread.currentThread().getId() % MAX_THREADS);
		
		// publish into thread array
		scanArray[pad(idx)] = data;
		//Chunk.unsafe.storeFence();
	}


	public int debugCountKeys()
	{
		int keys = 0;
		Chunk<K,V> chunk = skiplist.firstEntry().getValue();
		
		while (chunk != null)
		{
			keys += chunk.debugCountKeys();
			chunk = chunk.next.getReference();
		}
		return keys;
	}

	public int debugCountKeysTotal()
	{
		int keys = 0;
		Chunk<K,V> chunk = skiplist.firstEntry().getValue();

		while (chunk != null)
		{
			keys += chunk.debugCountKeysTotal();
			chunk = chunk.next.getReference();
		}
		return keys;
	}
	public int debugCountDups()
	{
		int dups = 0;
		Chunk<K,V> chunk = skiplist.firstEntry().getValue();
		
		while (chunk != null)
		{
			dups += chunk.debugCountDups();
			chunk = chunk.next.getReference();
		}
		return dups;
	}
	public void debugPrint()
	{
		Chunk<K,V> chunk = skiplist.firstEntry().getValue();
		
		while (chunk != null)
		{
			System.out.print("[ ");
			chunk.debugPrint();
			System.out.print("]\t");
			
			chunk = chunk.next.getReference();
		}
		System.out.println();
	}

	public void printDebugStats(DebugStats ds)
	{
		System.out.println("Chunks count: " + ds.chunksCount);
		System.out.println();

		System.out.println("Sorted size: " + ds.sortedCells/ds.chunksCount);
		System.out.println("Item count: " + ds.itemCount/ds.chunksCount);
		System.out.println("Occupied count: " + ds.occupiedCells/ds.chunksCount);
		System.out.println();

		System.out.println("Key jumps count: " + ds.jumpKeyCount/ds.chunksCount);
		System.out.println("Val jumps count: " + ds.jumpValCount/ds.chunksCount);
		System.out.println();

		System.out.println("Null items: " + ds.nulItemsCount/ds.chunksCount);
		System.out.println("Removed items: " + ds.removedItems/ds.chunksCount);

		System.out.println("Duplicates count: "  + ds.duplicatesCount/ds.chunksCount);
		System.out.println();

	}

	public DebugStats calcChunkStatistics()
	{
		Chunk<K,V> curr = skiplist.firstEntry().getValue();
		DebugStats ds = new DebugStats();

		while(curr != null)
		{
			curr.debugCalcCounters(ds);
			ds.chunksCount++;

			curr = curr.next.getReference();
		}

		return ds;
	}

	public int debugCountDuplicates() {
		List<Chunk<K,V>> chunks = new LinkedList<>();
		Chunk<K,V> curr= skiplist.firstEntry().getValue();

		while(curr != null)
		{
			//curr.debugCompacted();

			chunks.add(curr);
			curr = curr.next.getReference();
		}

		MultiChunkIterator<K,V> iter = new MultiChunkIterator<>(chunks);
		if(!iter.hasNext()) return -1;
		iter.next();

		K prevKey = iter.getKey();
		int prevVersion = iter.getVersion();
		int duplicates = 0;
		int total = 0;
		int nullItems = 0;

		while(iter.hasNext())
		{
			total++;
			iter.next();
			K currKey = iter.getKey();
			V val = iter.getValue();

			int currVersion = iter.getVersion();

			if(currKey.equals(prevKey))
				duplicates++;

			if(val == null)
				nullItems++;
			prevKey = currKey;
			prevVersion = currVersion;
		}



		System.out.println("Number of chunks: " + chunks.size());
		System.out.println("Total number of elements: " + total);
		System.out.println("Total number of null items: " + nullItems);
		return duplicates;
	}


}
