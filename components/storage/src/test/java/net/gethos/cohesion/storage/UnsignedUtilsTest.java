/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage;

import net.gethos.cohesion.common.UnsignedUtils;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author {@literal Stewart Gebbie <sgebbie@gethos.net>}
 *
 */
public class UnsignedUtilsTest {

	@Test
	public void digits() {
		
		digits( "fffffffffffffff", 0xfffffffffffffffL,15);
		digits( "0ffffffffffffff",  0xffffffffffffffL,15);
		digits( "000000000000001",               0x1L,15);
		
		digits("ffffffffffffffff",0xffffffffffffffffL,16);
		digits("0fffffffffffffff", 0xfffffffffffffffL,16);
		digits("0000000000000001",               0x1L,16);
		
		digits(              "01",               0x1L,2);
	}
	
	private void digits(String expect, long value, int digits) {
		String ff = UnsignedUtils.digits(value, digits);
//		System.out.println(">>" + ff + "<<");
		assertEquals(expect,ff);
	}

	@Test
	public void unsignedLong() {
		assertTrue (UnsignedUtils.compareUnsigned(0xffffffffffffffffL, 0xffffffffffffffffL) == 0);
		assertTrue (UnsignedUtils.compareUnsigned(0x0000000000000000L, 0xffffffffffffffffL)  < 0);
		assertTrue (UnsignedUtils.compareUnsigned(0x0000000000000000L, 0x0000000000000000L) == 0);
		assertTrue (UnsignedUtils.compareUnsigned(0xffffffffffffffffL, 0x0000000000000000L)  > 0);
		assertTrue (UnsignedUtils.compareUnsigned(0xffffffffffffffffL, 0xfffffffffffffff1L)  > 0);
		assertTrue (UnsignedUtils.compareUnsigned(0xfffffffffffffff1L, 0xffffffffffffffffL)  < 0);
		assertTrue (UnsignedUtils.compareUnsigned(0xffffffffffffffffL, 0x0fffffffffffffffL)  > 0);
		assertTrue (UnsignedUtils.compareUnsigned(0xffffffffffffffffL, 0x7fffffffffffffffL)  > 0);
		assertTrue (UnsignedUtils.compareUnsigned(0x7fffffffffffffffL, 0x8000000000000000L)  < 0);
		assertTrue (UnsignedUtils.compareUnsigned(                 8L,                  7L)  > 0);
		assertTrue (UnsignedUtils.compareUnsigned(               256L,              13000L)  < 0);
	}
	
	@Test
	public void unsignedInt() {
		assertTrue (UnsignedUtils.compareUnsigned(0xffffffff, 0xffffffff) == 0);
		assertTrue (UnsignedUtils.compareUnsigned(0x00000000, 0xffffffff)  < 0);
		assertTrue (UnsignedUtils.compareUnsigned(0x00000000, 0x00000000) == 0);
		assertTrue (UnsignedUtils.compareUnsigned(0xffffffff, 0x00000000)  > 0);
		assertTrue (UnsignedUtils.compareUnsigned(0xffffffff, 0xfffffff1)  > 0);
		assertTrue (UnsignedUtils.compareUnsigned(0xfffffff1, 0xffffffff)  < 0);
		assertTrue (UnsignedUtils.compareUnsigned(0xffffffff, 0x0fffffff)  > 0);
		assertTrue (UnsignedUtils.compareUnsigned(0xffffffff, 0x7fffffff)  > 0);
		assertTrue (UnsignedUtils.compareUnsigned(0x7fffffff, 0x80000000)  < 0);
		assertTrue (UnsignedUtils.compareUnsigned(         8,          7)  > 0);
		assertTrue (UnsignedUtils.compareUnsigned(       256,      13000)  < 0);
	}

}
