/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage.contiguous;

import java.util.ArrayDeque;
import java.util.Collection;

import net.gethos.cohesion.storage.contiguous.Range;

import static org.junit.Assert.*;

import org.junit.Test;

/**
 * @author {@literal Stewart Gebbie <sgebbie@gethos.net>}
 *
 */
public class RangeTest {
	
	@Test
	public void unionDisjoint() {
		Range a = new Range(9,5);
		Range b = new Range(16,3);
		Collection<Range> r = new ArrayDeque<Range>();
		boolean ok = Range.union(a,b,r);
		assertFalse(ok);
		assertEquals(2,r.size());
		assertTrue(r.contains(new Range(9,5)));
		assertTrue(r.contains(new Range(16,3)));
	}
	
	@Test
	public void unionDisjointTouching() {
		Range a = new Range(9,7);
		Range b = new Range(16,3);
		Collection<Range> r = new ArrayDeque<Range>();
		boolean ok = Range.union(a,b,r);
		assertTrue(ok);
		assertEquals(1,r.size());
		assertTrue(r.contains(new Range(9,10)));
	}
	
	@Test
	public void unionOverlap() {
		Range a = new Range(9,9);
		Range b = new Range(16,4);
		Collection<Range> r = new ArrayDeque<Range>();
		boolean ok = Range.union(a,b,r);
		assertTrue(ok);
		assertEquals(1,r.size());
		assertTrue(r.contains(new Range(9,11)));
	}
	
	@Test
	public void intersection() {
		Range a = new Range(9,5);
		Range b = new Range(16,3);
		Range c = new Range(11,6);
		assertTrue(a.intersects(a));

		assertFalse(b.intersects(a));
		assertFalse(a.intersects(b));
		
		assertTrue(a.intersects(c));
		assertTrue(b.intersects(c));
		assertTrue(c.intersects(a));
		assertTrue(c.intersects(b));
	}

	@Test
	public void subtractionInner() {
		Range a = new Range(9,8); //  [9,17)
		Range r = new Range(11,4); // [11,15)
		Collection<Range> s = new ArrayDeque<Range>();
		Range.subtract(a, r, s);
		assertEquals(2,s.size());
//		System.out.println(s);
		assertTrue(s.contains(new Range(9,2)));
		assertTrue(s.contains(new Range(15,2)));
	}
	
	@Test
	public void subtractionEqual() {
		Range a = new Range(9,8); //  [9,17)
		Range r = new Range(9,8);
		Collection<Range> s = new ArrayDeque<Range>();
		Range.subtract(a, r, s);
		assertEquals(0,s.size());
	}
	
	@Test
	public void subtractionLeft() {
		Range a = new Range(9,8); //  [9,17)
		Range r = new Range(5,8); //  [5,13)
		Collection<Range> s = new ArrayDeque<Range>();
		Range.subtract(a, r, s);
		assertEquals(1,s.size());
		assertTrue(s.contains(new Range(13,4)));
	}
	
	@Test
	public void subtractionLeftEdge() {
		Range a = new Range(9,8); //  [9,17)
		Range r = new Range(9,4); //  [9,13)
		Collection<Range> s = new ArrayDeque<Range>();
		Range.subtract(a, r, s);
		assertEquals(1,s.size());
		assertTrue(s.contains(new Range(13,4)));
	}
	
	@Test
	public void subtractionRight() {
		Range a = new Range(9,8); //  [9,17)
		Range r = new Range(14,6); //  [14,20)
		Collection<Range> s = new ArrayDeque<Range>();
		Range.subtract(a, r, s);
		assertEquals(1,s.size());
//		System.out.println(s);
		assertTrue(s.contains(new Range(9,5)));
	}
	
	@Test
	public void subtractionRightEdge() {
		Range a = new Range(9,8); //  [9,17)
		Range r = new Range(14,3); //  [14,17)
		Collection<Range> s = new ArrayDeque<Range>();
		Range.subtract(a, r, s);
		assertEquals(1,s.size());
//		System.out.println(s);
		assertTrue(s.contains(new Range(9,5)));
	}
	
	@Test
	public void subtractionUnbounded() {
		Range a = Range.$(50,Long.MAX_VALUE); //  [9,\infinity)
		Range r = Range.$(60,70); //  [60,70)
		Collection<Range> s = new ArrayDeque<Range>();
		Range.subtract(a, r, s);
//		System.out.println(s);
		assertEquals(2,s.size());
		assertTrue(s.contains(Range.$(50,60)));
		assertTrue(s.contains(Range.$(70,Long.MAX_VALUE)));
	}
	
	@Test
	public void subtractionUnboundedBoundary() {
		Range a = Range.$(50,Long.MAX_VALUE); //  [50,\infinity)
		Range r = Range.$(50,70); //  [50,70)
		Collection<Range> m = new ArrayDeque<Range>();
		m.add(r);
		Collection<Range> s = Range.subtractAll(a, m);
//		System.out.println(s);
		assertEquals(1,s.size());
		assertTrue(s.contains(Range.$(70,Long.MAX_VALUE)));
	}
	
	@Test
	public void subtractMany() {
		Collection<Range> m = new ArrayDeque<Range>();
		m.add(Range.$(9,17));
		m.add(Range.$(17,25));
		m.add(Range.$(30,40));
		
		Collection<Range> s = new ArrayDeque<Range>();
		s.add(Range.$(7, 10));
		s.add(Range.$(24, 35));
		
		Collection<Range> r = Range.subtractAll(m, s);
		
//		System.out.println(r);
		
		assertTrue(r.contains(Range.$(10, 17)));
		assertTrue(r.contains(Range.$(17, 24)));
		assertTrue(r.contains(Range.$(35, 40)));
		assertEquals(3,r.size());
	}
	
}