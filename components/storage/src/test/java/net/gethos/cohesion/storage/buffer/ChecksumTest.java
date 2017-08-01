/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage.buffer;

import java.util.zip.CRC32;

import org.junit.Test;

import net.gethos.cohesion.storage.BTreeTestUtils;

import static org.junit.Assert.*;

public class ChecksumTest {

	@Test
	public void crc32() {
		CRC32 crc32 = new CRC32();
		byte[] data = BTreeTestUtils.random(100);
		crc32.reset();
		crc32.update(data);
		long crcA = crc32.getValue();
		data[0] ^= 1;
		crc32.reset();
		crc32.update(data);
		long crcB = crc32.getValue();
		assertFalse(crcA == crcB);
	}

}
