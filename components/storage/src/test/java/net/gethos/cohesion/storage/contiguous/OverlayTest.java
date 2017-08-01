/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage.contiguous;

import java.util.List;

import net.gethos.cohesion.storage.contiguous.AllocationRange;
import net.gethos.cohesion.storage.contiguous.Overlay;

import static org.junit.Assert.*;

import org.junit.Test;

/**
 * @author {@literal Stewart Gebbie <sgebbie@gethos.net>}
 *
 */
public class OverlayTest {
	
	@Test
	public void overlays() {
		// 1.
		// record: [5,                      30,A)
		// record:          [15,   20,F)
		// check:  [5, 15,A)[15,   20,F)[20,30,A)
		// 2.
		// record:                               [30,35,A)
		// check:  [5, 15,A)[15,   20,F)[20,         35,A)
		// 3.
		// release:[5,                      30,A) 
		// check:           [15,   20,F)         [30,35,A)
		// 4.
		// release:         [15,   20,F)
		// check:                                [30,35,A)
		// 5.
		// release:                              [30,35,A)
		// check: empty
		
		List<AllocationRange> check;
		Overlay overlay = new Overlay();
		// 1.
		overlay.record(new AllocationRange(5, 25, true ));
		overlay.record(new AllocationRange(15, 5, false));
//		System.out.printf("overlay=%s%n",overlay);
		check = overlay.intervals();
		assertNotNull(check);
		assertEquals(3,check.size());
		assertEquals(new AllocationRange(5, 10,true ),check.get(0));
		assertEquals(new AllocationRange(15, 5,false),check.get(1));
		assertEquals(new AllocationRange(20,10,true ),check.get(2));
		
		// 2.
		AllocationRange x = new AllocationRange(30,5,true);
//		System.out.printf("record=%s%n",x);
		overlay.record(x);
//		System.out.printf("overlay=%s%n",overlay);
		check = overlay.intervals();
		assertNotNull(check);
		assertEquals(3,check.size());
		assertEquals(new AllocationRange(5, 10,true ),check.get(0));
		assertEquals(new AllocationRange(15, 5,false),check.get(1));
		assertEquals(new AllocationRange(20,15,true ),check.get(2));		
		
		// 3.
		x = new AllocationRange(5,25,true);
//		System.out.printf("release=%s%n",x);
		overlay.release(x);
//		System.out.printf("overlay=%s%n",overlay);
		check = overlay.intervals();
		assertNotNull(check);
		assertEquals(2,check.size());
		assertEquals(new AllocationRange(15, 5, false),check.get(0));
		assertEquals(new AllocationRange(30, 5, true),check.get(1));
		
		// 4.
		x = new AllocationRange(15,5,false);
//		System.out.printf("release=%s%n",x);
		overlay.release(x);
//		System.out.printf("overlay=%s%n",overlay);
		check = overlay.intervals();
		assertNotNull(check);
		assertEquals(1,check.size());
		assertEquals(new AllocationRange(30, 5, true),check.get(0));
		
		// 5.
		x = new AllocationRange(30,5,true);
//		System.out.printf("release=%s%n",x);
		overlay.release(x);
//		System.out.printf("overlay=%s%n",overlay);
		check = overlay.intervals();
		assertNotNull(check);
		assertEquals(0,check.size());
		assertTrue(overlay.isEmpty());
		assertNull(overlay.peek());
	}
	
	@Test
	public void touching() {
		Overlay overlay = new Overlay();
		overlay.record(new AllocationRange(5,  5, true ));
		overlay.record(new AllocationRange(10, 5, true));
		List<AllocationRange> check = overlay.intervals();
		assertNotNull(check);
		assertEquals(1,check.size());
		assertEquals(new AllocationRange(5, 10, true ),check.get(0));
		assertEquals(new AllocationRange(5, 10, true ),overlay.peek());
	}
}
