/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage.buffer;

import net.gethos.cohesion.storage.BTree;
import net.gethos.cohesion.storage.StorageConstants;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

/**
 * @author {@literal Stewart Gebbie <sgebbie@gethos.net>}
 *
 */
public class BufferLeafNodeTest {
/*
 * After balancing we get the following. However, this does not actually
 * leave 1829 bytes free in the correct node.
 * 
  
  sibling={@4ba33d48[t=LEAF|c=4096|f=3155|n= 6]
	(@  32:0001:e9c5e5c772394522aff438bbee3a94284b84f69f:0000:0000000000000000,  16,@4080)
	(@  74:0001:e9f4ab521041fed11f91bd2bac938b5fe8d82ca7:0000:0000000000000000,  16,@4064)
	(@ 116:0001:ea09f7e711962b63b6b1e10a4c799a4e8a6f50e1:0000:0000000000000000,  16,@4048)
	(@ 158:0001:ea4f941b31f1be7473d82be8087321f06b0756c2:0000:0000000000000000, 287,@3761)
	(@ 200:0001:eaa9ea623fe81a1085abc6cafe23d2e51feecc1b:0000:0000000000000000,  16,@3745)
	(@ 242:0001:ecb1ef6c2fb6a44b15ef083ba89cf38f7ec6bba4:0000:0000000000000000, 306,@3439)}
  siblingOffset=2952811
  
  key=     0001:ecc7ef42e5ce0378d44e25b0ee8dc4bf90bf8668:0000:0000000000000000
  nrequired=1829
  
  nl={@4cbfea1d[t=LEAF|c=4096|f=1363|n=17]
  	(@  32:0001:ecec1690ad594f685e1707138c4187f079ec941e:0000:0000000000000000,  16,@4080)
	(@  74:0001:edd3dcb1755ac9fb4b6705e675b9335a25cce394:0000:0000000000000000,  16,@4064)
	(@ 116:0001:edf95ec8821972a3fe6a10995cbc50215ce02c1a:0000:0000000000000000,  16,@4048)
	(@ 158:0001:edfd6ae69df6ad701a2e6ebe800da2d714ffec91:0000:0000000000000000,  16,@4032)
	(@ 200:0001:eeb6cdc1646d1775ac1f4506c87140ae6c0baf15:0000:0000000000000000,  16,@4016)
	(@ 242:0001:eebfbb871251d978a96be372706906b85ab0cd43:0000:0000000000000000,  16,@4000)
	(@ 284:0001:eedc08bd0490b2d1d51a7c707f86830a5b4d448c:0000:0000000000000000,  16,@3984)
	(@ 326:0001:efdff4c45730e94d50ed34c0ff075b9b5465ae51:0000:0000000000000000,  16,@3968)
	(@ 368:0001:f02d254efa46fe606e277e65c1144564fb56f4bd:0000:0000000000000000,  16,@3952)
	(@ 410:0001:f038e6910823f8c43d70ffb67dc9e8caada6eba0:0000:0000000000000000,  16,@3936)
	(@ 452:0001:f0b9734a3ca9cb6a9f88e55cf8cb4ccf1ba8925a:0000:0000000000000000,  16,@3920)
	(@ 494:0001:f0cb483db0b5eab8a2fe180aa4a1e5fdff2025e9:0000:0000000000000000, 758,@3162)
	(@ 536:0001:f188bf26160506b42a3e1f633c14fc66a8221aaf:0000:0000000000000000,  16,@3146)
	(@ 578:0001:f1f2a323db9f4e38193f2f1e94601dfed6a3d19f:0000:0000000000000000,  16,@3130)
	(@ 620:0001:f281b791b14f901ee31f818cb1ade76937a99db9:0000:0000000000000000,  16,@3114)
	(@ 662:0001:f281ceb0c92d9c51aff027177366e63f9c95cf4d:0000:0000000000000000, 989,@2125)
	(@ 704:0001:f2a8b917cf143f1ab0ed02a5fad83c6c9d953c12:0000:0000000000000000,  16,@2109)}
  nOffset=1421980

  sibling.rightHandKey= 0001:ecb1ef6c2fb6a44b15ef083ba89cf38f7ec6bba4:0000:0000000000000000
  key.compareTo(sibling.rightHandKey())=1
 */
	private static final int TEST_DATA_SIZE = 1829;
	
	private BTree.Key nKey;
	
	private BufferLeafNode corpusCopy;
	
	@Before
	public void setUp() {
		nKey = BTree.Key.parseKey("0001:ecc7ef42e5ce0378d44e25b0ee8dc4bf90bf8668:0000:0000000000000000");

		corpusCopy = BufferNode.allocateLeaf(StorageConstants.DEFAULT_NODE_CAPACITY);
		addItem(corpusCopy,"0001:e9c5e5c772394522aff438bbee3a94284b84f69f:0000:0000000000000000",  16);
		addItem(corpusCopy,"0001:e9f4ab521041fed11f91bd2bac938b5fe8d82ca7:0000:0000000000000000",  16);
		addItem(corpusCopy,"0001:ea09f7e711962b63b6b1e10a4c799a4e8a6f50e1:0000:0000000000000000",  16);
		addItem(corpusCopy,"0001:ea4f941b31f1be7473d82be8087321f06b0756c2:0000:0000000000000000", 287);
		addItem(corpusCopy,"0001:eaa9ea623fe81a1085abc6cafe23d2e51feecc1b:0000:0000000000000000",  16);
		addItem(corpusCopy,"0001:ecb1ef6c2fb6a44b15ef083ba89cf38f7ec6bba4:0000:0000000000000000", 306);

		addItem(corpusCopy,"0001:ecec1690ad594f685e1707138c4187f079ec941e:0000:0000000000000000",  16);
		addItem(corpusCopy,"0001:edd3dcb1755ac9fb4b6705e675b9335a25cce394:0000:0000000000000000",  16);
		addItem(corpusCopy,"0001:edf95ec8821972a3fe6a10995cbc50215ce02c1a:0000:0000000000000000",  16);
		addItem(corpusCopy,"0001:edfd6ae69df6ad701a2e6ebe800da2d714ffec91:0000:0000000000000000",  16);
		addItem(corpusCopy,"0001:eeb6cdc1646d1775ac1f4506c87140ae6c0baf15:0000:0000000000000000",  16);
		addItem(corpusCopy,"0001:eebfbb871251d978a96be372706906b85ab0cd43:0000:0000000000000000",  16);
		addItem(corpusCopy,"0001:eedc08bd0490b2d1d51a7c707f86830a5b4d448c:0000:0000000000000000",  16);
		addItem(corpusCopy,"0001:efdff4c45730e94d50ed34c0ff075b9b5465ae51:0000:0000000000000000",  16);
		addItem(corpusCopy,"0001:f02d254efa46fe606e277e65c1144564fb56f4bd:0000:0000000000000000",  16);
		addItem(corpusCopy,"0001:f038e6910823f8c43d70ffb67dc9e8caada6eba0:0000:0000000000000000",  16);
		addItem(corpusCopy,"0001:f0b9734a3ca9cb6a9f88e55cf8cb4ccf1ba8925a:0000:0000000000000000",  16);
		addItem(corpusCopy,"0001:f0cb483db0b5eab8a2fe180aa4a1e5fdff2025e9:0000:0000000000000000", 758);
		addItem(corpusCopy,"0001:f188bf26160506b42a3e1f633c14fc66a8221aaf:0000:0000000000000000",  16);
		addItem(corpusCopy,"0001:f1f2a323db9f4e38193f2f1e94601dfed6a3d19f:0000:0000000000000000",  16);
		addItem(corpusCopy,"0001:f281b791b14f901ee31f818cb1ade76937a99db9:0000:0000000000000000",  16);
		addItem(corpusCopy,"0001:f281ceb0c92d9c51aff027177366e63f9c95cf4d:0000:0000000000000000", 989);
		addItem(corpusCopy,"0001:f2a8b917cf143f1ab0ed02a5fad83c6c9d953c12:0000:0000000000000000",  16);
	}
	
	private BufferLeafNode emptyNode() {
		BufferLeafNode emptyNode = BufferNode.allocateLeaf(StorageConstants.DEFAULT_NODE_CAPACITY);
		return emptyNode;
	}
	
	private BufferLeafNode populatedNode() {
		BufferLeafNode populatedNode = BufferNode.allocateLeaf(corpusCopy.capacity());
		
		for (int i = 0; i < corpusCopy.items(); i++) {
			populatedNode.realloc(corpusCopy.key(i), corpusCopy.size(i));
		}
		
		return populatedNode;
	}
	
	private BufferLeafNode halfFullNode() {
		BufferLeafNode populatedNode = BufferNode.allocateLeaf(corpusCopy.capacity());
		
		for (int i = 0; i < corpusCopy.items(); i++) {
			populatedNode.realloc(corpusCopy.key(i), corpusCopy.size(i));
			if (!populatedNode.isHalfEmpty()) break;
		}
		
		return populatedNode;
	}
	
	private void addItem(BufferLeafNode node, String ktext, int itemDataSize) {
		BTree.Key k = BTree.Key.parseKey(ktext);
//		System.out.printf("adding k=%s l=%d%n", k,itemDataSize);
		int r = node.realloc(k, itemDataSize);
		assertTrue(r >= 0);
	}
	
	
	@Test
	public void balance() {
		BufferLeafNode emptyNode = emptyNode();
		BufferLeafNode populatedNode = populatedNode();
		balance(emptyNode, populatedNode, TEST_DATA_SIZE, 0, true);
	}
	
	@Test
	public void senarios() {
		balance(populatedNode(), emptyNode(), TEST_DATA_SIZE, 0, true);
		
		balance(emptyNode(), populatedNode(), 500, 0, true);
		
		balance(emptyNode(), populatedNode(), 500, 4064, false);
		balance(populatedNode(), emptyNode(), 500, 4064, false);
		
		balance(emptyNode(), populatedNode(), 400, 4064, true);
		balance(populatedNode(), emptyNode(), 400, 4064, true);
	}
	
	@Test
	public void isCompressible() {
		BufferLeafNode a = emptyNode();
		BufferLeafNode b = halfFullNode();
		boolean ok = false;
		ok = a.isCompressibleWith(b);
		assertTrue(ok);
	}
	
	@Test
	public void pull() {

		BufferLeafNode s = emptyNode();
		addItem(s,"0001:0000000000000002000000000000000000000000:0000:0000000000000000",  100);
		addItem(s,"0001:0000000000000005000000000000000000000000:0000:0000000000000000",  100);
		addItem(s,"0001:0000000000000006000000000000000000000000:0000:0000000000000000",  100);
		addItem(s,"0001:0000000000000007000000000000000000000000:0000:0000000000000000",  100);
		addItem(s,"0001:0000000000000008000000000000000000000000:0000:0000000000000000",  100);

		BufferLeafNode n = emptyNode();
		addItem(n,"0001:000000000000000d000000000000000000000000:0000:0000000000000000",  100);
		addItem(n,"0001:0000000000000011000000000000000000000000:0000:0000000000000000",  100);
		
//		System.out.printf("n = %s%n", n);
//		System.out.printf("s = %s%n", s);
		
		boolean ok = n.balance(s, false);
		assertTrue(ok);
	}
	
	public void balance(BufferLeafNode a, BufferLeafNode b, int nItemDataSize, int aRequired, boolean expectBalance) {
		
//		System.out.printf("Before...%n");
//		System.out.printf("nKey=%s  nItemDataSize=%d%n",nKey,nItemDataSize);
//		System.out.printf("aRequired=%d%n", aRequired);
//		System.out.printf("a=%s%n",a);
//		System.out.printf("b=%s%n",b);
		
		boolean ok = a.balance(b, nKey, nItemDataSize, aRequired);
		
//		System.out.printf("After...%n");
//		System.out.printf("a=%s%n",a);
//		System.out.printf("b=%s%n",b);
		
		verifyExistence(a,b,expectBalance,nItemDataSize);
		
		assertEquals(expectBalance,ok); // check after verify existence, so that if balance fails we make sure we didn't loose anything
		
		if (expectBalance) {
			assertTrue(String.format("a.free()=%d but aRequired=%d, b.free()=%d",a.free(),aRequired,b.free()),a.free() >= aRequired);
		}
	}

	private void verifyExistence(BufferLeafNode a, BufferLeafNode b,boolean expectNewKey,int nItemDataSize) {
		if (expectNewKey) verifyExistence(a, b, nKey, nItemDataSize);
		
		for (int i = 0; i < corpusCopy.items(); i++) {
			verifyExistence(a,b,corpusCopy.key(i), corpusCopy.size(i));
		}
	}
	
	private void verifyExistence(BufferLeafNode a, BufferLeafNode b, BTree.Key k, int expectSize) {
		int idx = -1;
		
		idx = a.find(k);
		boolean aFound = idx >= 0;
		if (aFound) assertEquals(expectSize,a.size(idx));
		
		idx = b.find(k);
		boolean bFound = idx >= 0;
		if (bFound) assertEquals(expectSize,b.size(idx));
		
		assertTrue(aFound || bFound);
		assertFalse(aFound && bFound);
	}
}
