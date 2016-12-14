package kiwi;

import java.nio.ByteBuffer;

public class Cell implements Comparable<Cell>
{
	public static final	Cell Empty = new Cell(new byte[0], 0, 0);
	
	private final byte[]	bytes;
	private final int		offset;
	private final int		length;
	
	public Cell(byte[] bytes, int off, int len)
	{
		this.bytes = bytes;
		this.offset = off;
		this.length = len;
	}
	
	public byte[] getBytes()
	{
		return bytes;
	}
	public int getOffset()
	{
		return offset;
	}
	public int getLength()
	{
		return length;
	}
	
	@Override
	public String toString()
	{
		if (length == 0) return "Empty";
		
		// TODO remove this method! works only for INTEGER!
		int n = bytes[offset] << 24 | (bytes[offset+1] & 0xFF) << 16 |
				(bytes[offset+2] & 0xFF) << 8 | (bytes[offset+3] & 0xFF);
		return n+"";
	}
	
	@Override
	protected Cell clone()
	{
		// allocate new byte array and copy data into it
		byte[] b = new byte[this.length];
		System.arraycopy(bytes, offset, b, 0, length);

		// return new Cell wrapping the cloned byte array
		return new Cell(b, 0, length);
	}
	
	@Override
	public boolean equals(Object obj)
	{
		return this.compareTo((Cell) obj) == 0;
	}
	
	@Override
	public int hashCode()
	{
		return super.hashCode();
	}
	

	public int compareTo(Cell c)
	{
		return ByteBuffer.wrap(bytes, offset, length).compareTo(ByteBuffer.wrap(c.getBytes(), c.getOffset(), c.getLength()));
	}
}
