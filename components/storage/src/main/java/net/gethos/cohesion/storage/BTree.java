/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage;

import java.nio.ByteBuffer;

import net.gethos.cohesion.common.UnsignedUtils;
import net.gethos.cohesion.storage.backing.BTreeLeafNode;

/**
 * Implements B-Tree based storage. The tree maintains
 * its own allocation information.
 * <p>
 * The keys are structured to support multiple indexes
 * within one tree. So, the key is comprised of:
 * (index id, unique key, object type, object parameterisation)
 * The 'parameterisation' is not used in the search, so there
 * could be multiple references to an object in the tree,
 * but they could be parameterised differently.
 * <p>
 * See: The Art of Computer Programming, Sorting and Searching,
 *      Multiway Trees, Refinements on B-Trees,
 *      T.H.Martin noted that rather than relying on the number
 *      of keys for deciding the split, we could just use the
 *      amount of data and then have variable length keys.
 * <p>
 * Invariants:
 * <ol>
 * <li> Every node must have a data usage of at most B, where B is
 *      the chosen block size, i.e. <code>&le; B</code>.
 * <li> Every node, except the root has a data usage of at least B/2,
 *      i.e. <code>&ge; &lceil;B/2&rceil;</code>.
 * <li> The root has at least 2 children, unless it is a leaf.
 * <li> Non-leaf nodes contain only keys, while leaf nodes contain
 *      keys and data.
 * <li> A non-leaf nodes with k children contains k-1 keys.
 * <li> A child node contains keys less than or equal to the the key pointing to it.
 * </ol>
 *
 * Note, in the above the key size together with the block size would imply
 * the order, <code>m</code>, of the tree in usual terms. However, we allow for variable
 * entry sizes in the leaves.
 * <p>
 * The key provides the data needed to find an interpret the object it references.
 *
 * @author {@literal Stewart Gebbie <sgebbie@gethos.net>}
 *
 */
public interface BTree extends ReadOnlyBTree {

	/**
	 * 32-byte key, with 24-bytes being used for comparison
	 * and 8 bytes containing extra data
	 */
	public static final class Key implements Comparable<Key>, Cloneable {

		public static final Key MAX_KEY;
		public static final Key MIN_KEY;

		/**
		 * The minimum key that is valid for standard data storage.
		 */
		public static final Key MIN_DATA_KEY;

		/**
		 * Number of bytes needed to represent the key.
		 */
		public static final int SIZE = 32;

		static {
			MAX_KEY = new Key();
			MAX_KEY.idx       = Short.MAX_VALUE; //0x7fff; +32767
			MAX_KEY.idHigh    = 0xffffffffffffffffL;
			MAX_KEY.idMiddle  = 0xffffffffffffffffL;
			MAX_KEY.idLow     = 0xffffffff;
			MAX_KEY.type      = Short.MAX_VALUE; //0x7fff;
			MAX_KEY.parameter = 0;

			MIN_KEY = new Key();
			MIN_KEY.idx       = Short.MIN_VALUE; //0x8000; -32768
			MIN_KEY.idHigh    = 0x0L;
			MIN_KEY.idMiddle  = 0x0L;
			MIN_KEY.idLow     = 0x0;
			MIN_KEY.type      = Short.MIN_VALUE; //0x8000;
			MIN_KEY.parameter = 0;

			MIN_DATA_KEY = new Key();
			MIN_DATA_KEY.idx       = (short)(Short.MIN_VALUE+1); //0x8001; -32767
			MIN_DATA_KEY.idHigh    = 0x0L;
			MIN_DATA_KEY.idMiddle  = 0x0L;
			MIN_DATA_KEY.idLow     = 0x0;
			MIN_DATA_KEY.type      = Short.MIN_VALUE; //0x8000;
			MIN_DATA_KEY.parameter = 0;
		}

		/**
		 * Which index to consult.
		 * <p>
		 * This is the most significant 16 bit (2 byte) part of the 32 byte key.
		 * So entries with different <code>idx</code> values will tend to be stored apart from each other.
		 */
		public short idx;

		// 20 byte unique key e.g. SHA1
		// (since we actually want unsigned data, it would be better to store this as a byte array.
		//  But, in Java, the array would then end up in the heap separately and not within this
		//  structure so we opt for using larger primitives)
		/**
		 * High 64 bits of the 160 bit (20 byte) arbitrary id part of the 32 byte key.
		 * <p>
		 * Along with the other <code>id</code> parts of the key, this is treated as unsigned data.
		 */
		public long idHigh;
		/**
		 * Middle 64 bits of the 160 bit (20 byte) arbitrary id part of the 32 byte key.
		 * <p>
		 * Along with the other <code>id</code> parts of the key, this is treated as unsigned data.
		 */
		public long idMiddle;
		/**
		 * Low 32 bits of the 160 bit (20 byte) arbitrary id part of the 32 byte key.
		 * <p>
		 * Along with the other <code>id</code> parts of the key, this is treated as unsigned data.
		 */
		public int  idLow;

		/**
		 * Object type - application specific.
		 * <p>
		 * This is the least significant 16 bit (2 byte) part of the 32 byte key.
		 * <p>
		 * This is enables the application layer to group various objects
		 * together that share the same key. Since they are then grouped,
		 * the will be stored in similar parts of the tree, thereby
		 * generally improving access performance.
		 */
		public short type;

		/**
		 * Extra parameterisation (not used for key lookup or comparison).
		 * <p>
		 * While part of the 32 byte key, these 8 bytes are not used for key comparison,
		 * i.e. they have no significance with respect to storage within the tree.
		 */
		public long parameter;

		public Key() {
			this.idx = 0;
			this.idHigh = 0;
			this.idMiddle = 0;
			this.idLow = 0;
			this.type = 0;
			this.parameter = 0;
		}

		public Key(Key k) {
			this.idx = k.idx;
			this.idHigh = k.idHigh;
			this.idMiddle = k.idMiddle;
			this.idLow = k.idLow;
			this.type = k.type;
			this.parameter = k.parameter;
		}

		public Key(short idx, byte[] id, short type, long parameter) {
			ByteBuffer idbuf = ByteBuffer.wrap(id);
			idbuf.order(StorageConstants.NETWORK_ORDER);
			this.idx = idx;
			this.idHigh = idbuf.getLong();
			this.idMiddle = idbuf.getLong();
			this.idLow = idbuf.getInt();
			this.type = type;
			this.parameter = parameter;
		}

		public Key(short idx, byte[] id, short type) {
			this(idx,id,type,0L);
		}

		public Key(ByteBuffer data) {
			readKey(data);
		}

		public byte[] id() {
			byte[] b = new byte[StorageConstants.SIZEOF_SHA1];
			ByteBuffer d = ByteBuffer.wrap(b);
			d.order(StorageConstants.NETWORK_ORDER);
			d.putLong(idHigh);
			d.putLong(idMiddle);
			d.putLong(idLow);
			return b;
		}

		public void readKey(ByteBuffer data) {
			this.idx = data.getShort();
			this.idHigh = data.getLong();
			this.idMiddle = data.getLong();
			this.idLow = data.getInt();
			this.type = data.getShort();
			this.parameter = data.getLong();
		}

		public void write(ByteBuffer data) {
			data.putShort(this.idx);
			data.putLong(this.idHigh);
			data.putLong(this.idMiddle);
			data.putInt(this.idLow);
			data.putShort(this.type);
			data.putLong(this.parameter);
		}

		public void write(ByteBuffer data, int offset) {
			data.putShort(offset, this.idx);      offset += StorageConstants.SIZEOF_SHORT;
			data.putLong (offset, this.idHigh);   offset += StorageConstants.SIZEOF_LONG;
			data.putLong (offset, this.idMiddle); offset += StorageConstants.SIZEOF_LONG;
			data.putInt  (offset, this.idLow);    offset += StorageConstants.SIZEOF_INT;
			data.putShort(offset, this.type);     offset += StorageConstants.SIZEOF_SHORT;
			data.putLong (offset, this.parameter);
		}

		@Override
		public Key clone() {
			try {
				return (Key) super.clone();
			} catch (CloneNotSupportedException e) {
				throw new RuntimeException("Clone is supported",e);
			}
			//			return new Key(this);
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + idx;
			result = prime * result + (int) (idHigh ^ (idHigh >>> 32));
			result = prime * result + (int) (idMiddle ^ (idMiddle >>> 32));
			result = prime * result + idLow;
			result = prime * result + type;
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj) return true;
			if (obj == null) return false;
			if (!(obj instanceof Key)) return false;
			Key other = (Key) obj;
			if (idx != other.idx) return false;
			if (idHigh != other.idHigh)	return false;
			if (idMiddle != other.idMiddle) return false;
			if (idLow != other.idLow) return false;
			if (type != other.type)	return false;
			return true;
		}

		@Override
		public int compareTo(Key o) {
			if (o == null) throw new NullPointerException("Can't compare with null");
			if (this == o) return 0;
			int c = this.idx - o.idx;
			if (c == 0) {
				c = UnsignedUtils.compareUnsigned(this.idHigh, o.idHigh);
				if (c == 0) {
					c = UnsignedUtils.compareUnsigned(this.idMiddle, o.idMiddle);
					if (c == 0) {
						c = UnsignedUtils.compareUnsigned(this.idLow, o.idLow);
						return c == 0 ? this.type - o.type : c;
					} else {
						return c;
					}
				} else {
					return c;
				}
			} else {
				return c;
			}
		}

		@Override
		public String toString() {
			return
					UnsignedUtils.digits(idx,4)
					+ ":"
					+ UnsignedUtils.digits(idHigh>>>32,8)+UnsignedUtils.digits(idHigh,8)
					+ UnsignedUtils.digits(idMiddle>>>32,8)+UnsignedUtils.digits(idMiddle,8)
					+ UnsignedUtils.digits(idLow,8)
					+ ":" + UnsignedUtils.digits(type,4)
					+ ":" + UnsignedUtils.digits(parameter>>>32,8)+UnsignedUtils.digits(parameter,8)
					;
		}

		public static BTree.Key parseKey(String key) {
			String[] parts = key.split(":");
			BTree.Key k = new Key();

			k.idx = (short)Long.parseLong(parts[0],16);

			k.idHigh = Long.parseLong(parts[1].substring(0,8),16)<<32|Long.parseLong(parts[1].substring(8,16),16);
			k.idMiddle = Long.parseLong(parts[1].substring(16,24),16)<<32|Long.parseLong(parts[1].substring(24,32),16);
			k.idLow = (int)Long.parseLong(parts[1].substring(32,40),16);

			k.type = (short)Long.parseLong(parts[2],16);
			k.parameter = Long.parseLong(parts[3].substring(0,8),16)<<32|Long.parseLong(parts[3].substring(8,16),16);

			return k;
		}

		public static BTree.Key loadKey(ByteBuffer buf) {
			return new BTree.Key(buf);
		}

		private Key(short idx, long idHigh, long idMiddle, int idLow, short type, long parameter) {
			this.idx = idx;
			this.idHigh = idHigh;
			this.idMiddle = idMiddle;
			this.idLow = idLow;
			this.type = type;
			this.parameter = parameter;
		}

		public static BTree.Key key(int idx, int id) {
			assert(idx >= Short.MIN_VALUE && idx <= Short.MAX_VALUE);
			assert(idx >= MIN_DATA_KEY.idx);
			return new Key((short)idx,id,0,0,(short)0,0);
		}

		public static BTree.Key key(short idx, long idHigh) {
			assert(idx >= MIN_DATA_KEY.idx);
			return new Key(idx,idHigh,0L,0,(short)0,0L);
		}

		public static BTree.Key key(short idx, long idHigh, short type) {
			assert(idx >= MIN_DATA_KEY.idx);
			return new Key(idx,idHigh,0L,0,type,0L);
		}

		public static BTree.Key key(short idx, byte[] id, short type) {
			assert (idx >= MIN_DATA_KEY.idx);
			return new Key(idx, id, type);
		}

		public static BTree.Key key(short idx, byte[] id, short type, long parameter) {
			assert (idx >= MIN_DATA_KEY.idx);
			return new Key(idx, id, type, parameter);
		}

		public static BTree.Key min(short idx, short type) {
			assert (idx >= MIN_DATA_KEY.idx);
			BTree.Key k = new BTree.Key(MIN_KEY);
			k.idx = idx;
			k.type = type;
			return k;
		}

		public static BTree.Key max(short idx, short type) {
			assert (idx >= MIN_DATA_KEY.idx);
			BTree.Key k = new BTree.Key(MAX_KEY);
			k.idx = idx;
			k.type = type;
			return k;
		}

		public static BTree.Key min(short idx) {
			assert (idx >= MIN_DATA_KEY.idx);
			BTree.Key k = new BTree.Key(MIN_KEY);
			k.idx = idx;
			k.type = Short.MIN_VALUE;
			return k;
		}

		public static BTree.Key max(short idx) {
			assert (idx >= MIN_DATA_KEY.idx);
			BTree.Key k = new BTree.Key(MAX_KEY);
			k.idx = idx;
			k.type = Short.MAX_VALUE;
			return k;
		}
	}

	/**
	 * References a unique item.
	 * <p>
	 * That is, this references a node in the tree, and an item in the node.
	 * <p>
	 * Note, objects may move as a result of inserts or deletes. As such,
	 * references are only valid if the tree has not been modified.
	 */
	public static class Reference {

		/**
		 * absolute block/node offset within the trees backing store.
		 */
		public long offset;

		/**
		 * index of the item within the node, or -1 if this is referencing a dedicated extent rather than a node.
		 */
		public int index;

		/**
		 * The amount of data, in bytes, associated with this reference.
		 */
		public long size;

		public Reference() {
			this(0L,0,0L);
		}

		public Reference(Reference l) {
			this.copy(l);
		}

		public Reference(long offset, int index, long size) {
			this.offset = offset;
			this.index = index;
			this.size = size;
		}

		public void copy(Reference l) {
			this.index = l.index;
			this.offset = l.offset;
			this.size = l.size;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + index;
			result = prime * result + (int) (offset ^ (offset >>> 32));
			result = prime * result + (int) (size ^ (size >>> 32));
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj) return true;
			if (obj == null) return false;
			if (!(obj instanceof Reference)) return false;
			Reference other = (Reference) obj;
			if (index != other.index) return false;
			if (offset != other.offset) return false;
			if (size != other.size) return false;
			return true;
		}

		@Override
		public String toString() {
			return "@" + Long.toString(offset) + ":" + Integer.toString(index) + "[" + Long.toString(size) + "]";
		}

	}

	public class Stat {

		public long itemNodeOffset;

		public int itemIndex;

		public int itemSize;

		public byte itemFlags;

		public long externalOffset;

		public long externalSize;

		@Override
		public String toString() {
			String base = String.format("@%d[%d]:%02x:%d",itemNodeOffset,itemIndex,itemFlags,itemSize);
			String extra = "";
			if ((itemFlags & BTreeLeafNode.Flags.EXTERNAL.mask) != 0) {
				extra = String.format("{@%d:%d}", externalOffset, externalSize);
			}
			return base + extra;
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public ReadOnlyBTreeTransaction openReadOnly();

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void close();

	/**
	 * Obtain a new transaction for accessing and updating the B-Tree
	 * 
	 * @return btree transaction
	 */
	public BTreeTransaction open();

	/**
	 * The depth of the tree.
	 * 
	 * @return the depth of the tree.
	 */
	public int depth(); // Consider rather exposing this via a summary object, possible as part of a tree-check.

}
