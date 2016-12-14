package kiwi;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * Created by msulamy on 7/27/15.
 */
public class KiWiMapVLK implements CompositionalMap<Integer,Integer>
{
	/***************	Constants			***************/
	
	/***************	Members				***************/
	private KiWi<Cell,Cell> kiwi;
    
    /***************	Constructors		***************/
    public KiWiMapVLK()
    {
    	this.kiwi = new KiWi<>(new ChunkCell());
    }
    
    /***************	Methods				***************/

    @Override
    public Integer putIfAbsent(Integer k, Integer v)
    {
    	
    	kiwi.put(cellFromInt(k), cellFromInt(v));
        return null;
    }
    
    @Override
    public int size()
    {
        return -1;
    }
    @Override
    public boolean isEmpty()
    {
        return false;
    }

    @Override
    public Integer get(Object o)
    {
    	return cellToInt(kiwi.get(cellFromInt((Integer)o)));
    }

    @Override
    public Integer put(Integer k, Integer v)
    {
    	kiwi.put(cellFromInt(k), cellFromInt(v));
        return null;
    }

    @Override
    public Integer remove(Object o)
    {
    	kiwi.put(cellFromInt((Integer)o), null);
        return null;
    }

    @Override
    public void putAll(Map<? extends Integer, ? extends Integer> map)
    {
    	throw new NotImplementedException();
    }

    @Override
    public int getRange(Integer[] result, Integer min, Integer max)
    {
        throw new NotImplementedException();
    }
    
    /** Same as get(key) != null **/
    @Override
    public boolean containsKey(Object o)
    {
    	return get(cellFromInt((Integer)o)) != null;
    }

    /** Not supported - can be implemented in a non-safe manner **/
    @Override
    public void clear()
    {
    	//this.kiwi.debugPrint();
    	this.kiwi = new KiWi<>(new ChunkCell());
    }

    /** Scan all & return keys **/
    @Override
    public Set<Integer> keySet()
    {
        throw new NotImplementedException();
    }

    /** Scan all & return values **/
    @Override
    public Collection<Integer> values()
    {
        throw new NotImplementedException();
    }

    /** Scan all & create entries **/
    @Override
    public Set<Entry<Integer,Integer>> entrySet()
    {
        throw new NotImplementedException();
    }    

    /** Scan all & search **/
    @Override
    public boolean containsValue(Object o)
    {
    	return (get(cellFromInt((Integer)o)) != null);
    }
    
    private Cell cellFromInt(Integer n)
    {
    	byte[] b = new byte[4];
    	
    	// write int data as bytes into data-array
		b[0] = (byte) (n >>> 24);
		b[1] = (byte) (n >>> 16);
		b[2] = (byte) (n >>> 8);
		b[3] = (byte) (n.intValue());
		
		return new Cell(b, 0, 4);
    }
    private Integer cellToInt(Cell c)
    {
    	if (c == null)
    		return null;

    	byte[] b = c.getBytes();
    	int off = c.getOffset();
    	
    	int n = b[off+0] << 24 | (b[off+1] & 0xFF) << 16 |
    		(b[off+2] & 0xFF) << 8 | (b[off+3] & 0xFF);
    	
    	return n;
    }
    
    public void debugPrint()
    {
    	kiwi.debugPrint();
    }
}
