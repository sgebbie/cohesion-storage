/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage.utils;

import java.nio.ByteBuffer;
import java.util.Iterator;

import net.gethos.cohesion.storage.BTree;
import net.gethos.cohesion.storage.ReadOnlyBTreeTransaction;


/**
 * Utilities for working with B-Trees.
 *
 * @author {@literal Stewart Gebbie <sgebbie@gethos.net>}
 *
 */
public class BTreeUtils {
	
	/**
	 * Compare and test that two trees are equivalent.
	 * 
	 * @param a
	 * @param b
	 */
	public static boolean equals(BTree a, BTree b) {
		
		ByteBuffer bufa = ByteBuffer.allocate(4096);
		ByteBuffer bufb = ByteBuffer.allocate(4096);
		
//		int count = 0;
		
		ReadOnlyBTreeTransaction at = a.openReadOnly();
		ReadOnlyBTreeTransaction bt = b.openReadOnly();
		try {
			try {
				Iterable<BTree.Key> ai = at.range(BTree.Key.MIN_DATA_KEY, BTree.Key.MAX_KEY);
				Iterable<BTree.Key> bi = bt.range(BTree.Key.MIN_DATA_KEY, BTree.Key.MAX_KEY);
				Iterator<BTree.Key> ax = ai.iterator();
				Iterator<BTree.Key> bx = bi.iterator();
				
				while(ax.hasNext()) {
					if (!bx.hasNext()) return false;
					BTree.Key ak = ax.next();
					BTree.Key bk = bx.next();
					
//					System.out.printf("[%d] checking data for %s%n",count++,ak);
					
					if (!ak.equals(bk)) return false;
					
					BTree.Reference ar = at.search(ak);
					BTree.Reference br = bt.search(ak);
					
					if (ar.size != br.size) return false;
					
					long size = ar.size;
					long offset = 0;
					
					while(offset < size) {
					
						long remaining = size - offset;
						int span = (int)Math.min(bufa.capacity(), remaining);
						bufa.clear(); bufb.clear();
						bufa.limit(span);
						bufb.limit(span);
						
						while(bufa.hasRemaining()) at.fetch(ar, offset, bufa);
						while(bufb.hasRemaining()) bt.fetch(br, offset, bufb);
						
						byte[] adata = bufa.array();
						byte[] bdata = bufb.array();
						
						for(int i = 0; i < span; i++) if (adata[i] != bdata[i]) return false;
					
						offset += span;
					}
					
				}
			} finally {
				bt.close();
			}	
		} finally {
			at.close();
		}
		
		return true;
	}
	
}
