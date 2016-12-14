package kiwi;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * Created by msulamy on 7/27/15.
 */
public class KiWiMap implements CompositionalMap<Integer,Integer>
{
	/***************	Constants			***************/
	
	/***************	Members				***************/
	public static boolean			SupportScan = true;
    public static int               RebalanceSize = 2;

	public KiWi<Integer,Integer>	kiwi;
    
    /***************	Constructors		***************/
    public KiWiMap()
    {
    	ChunkInt.initPool();
        KiWi.RebalanceSize = RebalanceSize;
    	this.kiwi = new KiWi<>(new ChunkInt(), SupportScan);
    }
    
    /***************	Methods				***************/

    /** same as put - always puts the new value! even if the key is not absent, it is updated */
    @Override
    public Integer putIfAbsent(Integer k, Integer v)
    {
    	kiwi.put(k, v);
        return null;	// can implement return value but not necessary
    }
    
    /** requires full scan() for atomic size() */
    @Override
    public int size()
    {
        return -1;
    }
    
    /** not implemented ATM - can be implemented with chunk.findFirst() */
    @Override
    public boolean isEmpty()
    {
        return false;
    }

    @Override
    public Integer get(Object o)
    {
    	return kiwi.get((Integer)o);
    }

    @Override
    public Integer put(Integer k, Integer v)
    {
    	kiwi.put(k, v);
        return null;
    }

    /** same as put(key,null) - which signifies to KiWi that the item is removed */
    @Override
    public Integer remove(Object o)
    {
    	kiwi.put((Integer)o, null);
        return null;
    }

    @Override
    public int getRange(Integer[] result, Integer min, Integer max)
    {
        return kiwi.scan(result,min,max);
/*
    	Iterator<Integer> iter = kiwi.scan(min, max);
    	int i;
    	
    	for (i = 0; (iter.hasNext()) && (i < result.length); ++i)
    	{
    		result[i] = iter.next();
    	}
    	
    	return i;
*/
    }
    
    /** same as put(key,val) for each item */
    @Override
    public void putAll(Map<? extends Integer, ? extends Integer> map)
    {
    	for (Integer key : map.keySet())
    	{
    		kiwi.put(key, map.get(key));
    	}
    }
    
    /** Same as get(key) != null **/
    @Override
    public boolean containsKey(Object o)
    {
    	return get(o) != null;
    }

    /** Clear is not really an option (can be implemented non-safe inside KiWi) - we just create new kiwi **/
    @Override
    public void clear()
    {
    	//this.kiwi.debugPrint();
    	ChunkInt.initPool();
    	this.kiwi = new KiWi<>(new ChunkInt(), SupportScan);
    }

    /** Not implemented - can scan all & return keys **/
    @Override
    public Set<Integer> keySet()
    {
        throw new NotImplementedException();
    }

    /** Not implemented - can scan all & return values **/
    @Override
    public Collection<Integer> values()
    {
        throw new NotImplementedException();
    }

    /** Not implemented - can scan all & create entries **/
    @Override
    public Set<Entry<Integer,Integer>> entrySet()
    {
        throw new NotImplementedException();
    }    

    /** Not implemented - can scan all & search **/
    @Override
    public boolean containsValue(Object o)
    {
    	throw new NotImplementedException();
    }

    public void compactAllSerial()
    {
        kiwi.compactAllSerial();
    }
    public int debugCountDups()
    {
    	return kiwi.debugCountDups();
    }
    public int debugCountKeys()
    {
    	return kiwi.debugCountKeys();
    }
    public void debugPrint()
    {
    	kiwi.debugPrint();
    }

    public int debugCountDuplicates() { return kiwi.debugCountDuplicates();}
    public int debugCountChunks() {return 0; }

    public void calcChunkStatistics()
    {
        kiwi.calcChunkStatistics();
    }
}
