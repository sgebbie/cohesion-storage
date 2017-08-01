/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage.utils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Iterator;

import net.gethos.cohesion.storage.BTree;
import net.gethos.cohesion.storage.BTree.Key;
import net.gethos.cohesion.storage.BTreeTransaction;
import net.gethos.cohesion.storage.StorageConstants;
import net.gethos.cohesion.storage.ReadOnlyBTree;
import net.gethos.cohesion.storage.ReadOnlyBTreeTransaction;

/**
 * Dumps the BTree as pure key:value pairs.
 * 
 * @author {@literal Stewart Gebbie <sgebbie@gethos.net>}
 *
 */
public class BTreeDump {

	static final byte[] MAGIC = {(byte)0x43,(byte)0x44};
	static final int MAGIC_SIZE = MAGIC.length * StorageConstants.SIZEOF_BYTE;
	static final byte[] RESERVED = {(byte)0x0,(byte)0x0};
	static final int RESERVED_SIZE = RESERVED.length * StorageConstants.SIZEOF_BYTE;
	static final int TIMESTAMP_LENGTH = StorageConstants.SIZEOF_LONG;
	
	static final int HEADER_OFFSET = 0;
	static final int MAGIC_OFFSET = HEADER_OFFSET;
	static final int RESERVED_OFFSET = MAGIC_OFFSET + MAGIC_SIZE;
	static final int TIMESTAMP_OFFSET = RESERVED_OFFSET + MAGIC_SIZE;
	static final int DATA_OFFSET = TIMESTAMP_OFFSET + TIMESTAMP_LENGTH;
	
	static final int DATA_HEADER_SIZE = BTree.Key.SIZE + StorageConstants.SIZEOF_LONG;
	
	static final int HEADER_SIZE = DATA_OFFSET;

	/**
	 * Writes the contents of the tree to a flat file format.
	 * 
	 * @param btree - a transaction to use for the operation
	 * @param c - channel to write the data to
	 * @param batchSize - the number of keys to traverse within a transaction
	 * @throws IOException 
	 */
	public static void dump(ReadOnlyBTree btree, WritableByteChannel c, int batchSize) throws IOException {
		dumpHeader(c);
		
		// create a buffer to reuse for dumping the data
		ByteBuffer data = ByteBuffer.allocate(32*1024);
		data.order(StorageConstants.NETWORK_ORDER);
		
		BTree.Key current = BTree.Key.MIN_DATA_KEY;
		dump:
		while(true) {
			ReadOnlyBTreeTransaction t = btree.openReadOnly();
			try {
				int batch = 0;
				Iterable<BTree.Key> range = t.range(current, BTree.Key.MAX_KEY);
				Iterator<BTree.Key> i = range.iterator();
				while(i.hasNext()) {
					current = i.next();
					if (++batch >= batchSize) continue dump;
					dump(t,c,current, data);
				}
				break dump;
			} finally {
				t.close();
			}
		}
	}

	private static void dumpHeader(WritableByteChannel c) throws IOException {
		ByteBuffer header = ByteBuffer.allocate(HEADER_SIZE);
		header.order(StorageConstants.NETWORK_ORDER);
		header.put(MAGIC);
		header.put(RESERVED);
		
		long timestamp = System.currentTimeMillis();
		
		header.putLong(timestamp);
		header.flip();
		while(header.hasRemaining()) {
			c.write(header);
		}
	}
	
	private static void dump(ReadOnlyBTreeTransaction t, WritableByteChannel c, Key k, ByteBuffer buf) throws IOException {
		BTree.Reference ref = t.search(k);
		
		dumpDataHeader(c, k, buf, ref.size);
		
		// read all the data and write it out
		int offset = 0;
		long remaining = 0;
		int span = 0;
		
		while(offset < ref.size) {
			remaining = ref.size - offset;
			span = (int)Math.min(buf.capacity(), remaining);
			
			// fetch data from tree
			buf.position(0);
			buf.limit(span);
			t.fetch(ref, offset, buf);
			
			buf.flip();
			offset += buf.remaining();
			
			// write data to dump
			while(buf.hasRemaining()) c.write(buf);
		}
	}

	private static void dumpDataHeader(WritableByteChannel c, Key k, ByteBuffer buf, long size) throws IOException {
		// write out a data header
		buf.clear();
		k.write(buf);
		buf.putLong(size);
		buf.flip();
		while(buf.hasRemaining()) c.write(buf);
	}

	/**
	 * Reads the contents from a previous dump and populates the tree.
	 * 
	 * @param btree - a transaction to use for the operation
	 * @param c - channel from which to read the data
	 * @param batchSize - the number of keys to traverse within a transaction
	 * @throws IOException 
	 */
	public static void load(BTree btree, ReadableByteChannel c, int batchSize) throws IOException {
	
//		int count = 0;
		
		DumpHeader header = loadHeader(c);
		assert(header.timestamp > 0);
//		System.out.printf("timestamp=%s [%d]%n", new Date(header.timestamp), header.timestamp);
		
		// create a buffer to reuse while loading the data
		ByteBuffer data = ByteBuffer.allocate(32*1024);
		data.order(StorageConstants.NETWORK_ORDER);
		
		DataHeader h = null;
		load:
		while(true) {
			BTreeTransaction t = btree.open();
			try {
				try {
					int batch = 0;
					if (h == null) h = loadDataHeader(t,c,data);
					while(h != null) {
						if (++batch > batchSize) continue load;
						
//						System.out.printf("[%d] loading %s%n",count++,h);
						
						load(t,c,h,data);
						
						h = loadDataHeader(t,c,data);
					}
					
					break load;
				} finally {
					t.commit();
				}
			} finally {
				t.close();
			}
		}
	}
	
	private static void load(BTreeTransaction t, ReadableByteChannel c, DataHeader h, ByteBuffer buf) throws IOException {
		BTree.Reference ref = t.truncate(h.key, h.size);
		
		// read all the data and write it out
		long offset = 0;
		long remaining = 0;
		int span = 0;
		
		while(offset < ref.size) {
			remaining = h.size - offset;
			span = (int)Math.min(buf.capacity(), remaining);
			
			// fetch data from tree
			buf.position(0);
			buf.limit(span);
			
			while (buf.hasRemaining()) {
				c.read(buf);
			}
			
			buf.flip();
			
			while(buf.hasRemaining()) {
				offset += t.store(ref, offset, buf);
			}
		}
	}

	private static DataHeader loadDataHeader(BTreeTransaction t, ReadableByteChannel c, ByteBuffer data) throws IOException {
		data.clear();
		data.limit(DATA_HEADER_SIZE);
		while(data.hasRemaining()) {
			int r = c.read(data);
			if (r < 0) return null; // eof
		}
		data.flip();
		DataHeader h = new DataHeader();
		h.key = BTree.Key.loadKey(data);
		h.size = data.getLong();
		return h;
	}

	private static DumpHeader loadHeader(ReadableByteChannel c) throws IOException {
		ByteBuffer header = ByteBuffer.allocate(HEADER_SIZE);
		header.order(StorageConstants.NETWORK_ORDER);
		
		while(header.hasRemaining()) c.read(header);
		
		header.flip();
		
		byte[] reserved = new byte[RESERVED_SIZE];
		
		DumpHeader h = new DumpHeader();
		h.magic = new byte[MAGIC_SIZE];
		header.get(h.magic);
		header.get(reserved);
		h.timestamp = header.getLong();
		
		return h;
	}

	private static class DumpHeader {
		public byte[] magic;
		public long timestamp;
	}
	
	private static class DataHeader {
		public BTree.Key key;
		public long size;
		
		@Override
		public String toString() {
			return String.format("%s [%d]",key,size);
		}
	}
}
