/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.common;

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class HexBuilderTest {

	private static final byte[] DEADBEEF = new byte[] {(byte)0xde,(byte)0xad,(byte)0xbe,(byte)0xef};

	@Test
	public void toHex() {
		String hex = HexBuilder.toHex(DEADBEEF);
		assertEquals("deadbeef", hex);
	}

	@Test
	public void fromHex() {
		byte[] data = HexBuilder.fromHex("deadbeef");
		assertNotNull(data);
		assertEquals(4,data.length);
		assertArrayEquals(DEADBEEF, data);
	}
}
