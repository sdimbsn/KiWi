package kiwi;

import java.util.SortedMap;
import java.util.concurrent.atomic.AtomicInteger;

public class ChunkInt extends Chunk<Integer,Integer>
{
	private static AtomicInteger nextChunk;
	private static ChunkInt[] chunks;
	public static void setPoolSize(int numChunks)
	{
		chunks = new ChunkInt[numChunks];
		
	}
	public static void initPool()
	{
		if (chunks != null)
		{
			nextChunk = new AtomicInteger(0);
			for (int i = 0; i < chunks.length; ++i)
				chunks[i] = new ChunkInt(null, null);
		}
	}
	
	private static final int DATA_SIZE = 1;//Integer.SIZE/8;	// average # of BYTES of item in data array (guesstimate)
	
	public ChunkInt()
	{
		this(Integer.MIN_VALUE, null);
	}
	public ChunkInt(Integer minKey, ChunkInt creator)
	{
		super(minKey, DATA_SIZE, creator);
	}
	@Override
	public Chunk<Integer,Integer> newChunk(Integer minKey)
	{
		if (chunks == null)
		{
			return new ChunkInt(minKey, this);
		}
		else
		{
			int next = nextChunk.getAndIncrement();
			ChunkInt chunk = chunks[next];
			chunks[next] = null;
			chunk.minKey = minKey;
			chunk.creator = this;
			return chunk;
		}
	}
	
	@Override
	public Integer readKey(int orderIndex)
	{
		return get(orderIndex, OFFSET_KEY);
	}
	@Override
	public Object readData(int oi, int di)
	{
		/*
		// get data - convert next 4 bytes to Integer data
		int data = dataArray[di] << 24 | (dataArray[di+1] & 0xFF) << 16 |
			(dataArray[di+2] & 0xFF) << 8 | (dataArray[di+3] & 0xFF);
		*/

		return dataArray[di];
	}

	@Override
	public int copyValues(Object[] result, int idx, int myVer, Integer min, Integer max, SortedMap<Integer, ThreadData.PutData<Integer,Integer>> items) {
		int oi = 0;

		if(idx == 0)
		{
			oi = findFirst(min,myVer);
			if(oi == NONE) return 0;

		} else
		{
			oi = getFirst(myVer);
		}

		int sortedSize = sortedCount*ORDER_SIZE;
		int prevKey = NONE;
		int currKey = NONE;
		int maxKey = max;

		int dataStart = get(oi,OFFSET_DATA);
		int dataEnd = dataStart -1;
		int itemCount = 0;
		int oiPrev = NONE;
		int prevDataId = NONE;
		int currDataId = dataStart;


	    boolean isFirst = dataStart > 0 ? true : false ;

		while(oi != NONE)
		{

			if(currKey > max)
				break;

			if(isFirst ||
					(
							(oiPrev < sortedSize)
							&&
							(prevKey != currKey)
									&&
									currKey <= maxKey
							&&
							(prevDataId + DATA_SIZE == currDataId)
							&&
							(getVersion(oi) <= myVer)
					))
			{
				if(isFirst) {
					dataEnd++;
					isFirst = false;
				} else
				{
					dataEnd = currDataId < 0 ? dataEnd : dataEnd+1;
				}

				oiPrev = oi;
				oi = get(oi, OFFSET_NEXT);

				prevKey = currKey;
				prevDataId = currDataId;
				currKey = get(oi,OFFSET_KEY);
				currDataId = get(oi,OFFSET_DATA);

			}
			else
			{
				// copy continuous range of data
				int itemsToCopy = dataEnd -dataStart + 1;

				if(itemsToCopy == 1)
				{
					result[idx+itemCount] =  dataArray[dataStart];
				}
				else if(itemsToCopy > 1)
				{
					System.arraycopy(dataArray, dataStart, result, idx + itemCount, itemsToCopy);
				}

				itemCount += itemsToCopy;

				// find next item to start copy interval
				while (oi != NONE)
				{

					if(currKey > max) return itemCount;

					// if in a valid version, and a different key - found next item
					if ((prevKey != currKey) && (getVersion(oi) <= myVer))
					{
						if(currDataId < 0)
						{
							// the value is NULL, the item was removed -- skip it
							prevKey = currKey;

						} else {
							break;
						}
					}

					// otherwise proceed to next
					oiPrev = oi;
					oi = get(oi, OFFSET_NEXT);
					currKey = get(oi,OFFSET_KEY);

					prevDataId = currDataId;
					currDataId = get(oi,OFFSET_DATA);
				}

				if(oi == NONE) return itemCount;

				dataStart = currDataId;//get(oi, OFFSET_DATA);
				dataEnd = dataStart -1;
				isFirst = true;
			}

		}

		int itemsToCopy = dataEnd -dataStart + 1;
		System.arraycopy(dataArray,dataStart, result, idx + itemCount, itemsToCopy);

		itemCount+= itemsToCopy;



		return itemCount;
	}

	@Override
	public int allocate(Integer key, Integer data)
	{
		// allocate items in order and data array => data-array only contains int-sized data
		int oi = baseAllocate( data == null ? 0 : DATA_SIZE);

		if (oi >= 0)
		{
			// write integer key into (int) order array at correct offset
			set(oi, OFFSET_KEY, (int) key);

			// get data index
			if(data != null) {
				int di = get(oi, OFFSET_DATA);
				dataArray[di] = data;
			}

		}

		// return order-array index (can be used to get data-array index)
		return oi;
	}

	@Override
	public int allocateSerial(int key, Integer data)
	{
		// allocate items in order and data array => data-array only contains int-sized data
		int oi = baseAllocateSerial(data == null ? 0 : DATA_SIZE);

		if (oi >= 0)
		{
			// write integer key into (int) order array at correct offset
			set(oi, OFFSET_KEY, key);

			if(data != null) {
				int di = get(oi, OFFSET_DATA);
				dataArray[di] = data;
			}
		}

		// return order-array index (can be used to get data-array index)
		return oi;
	}

}
