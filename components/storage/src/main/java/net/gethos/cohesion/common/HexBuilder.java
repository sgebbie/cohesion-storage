/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.common;


public class HexBuilder {

	private final StringBuilder hex;

	public HexBuilder() {
		this.hex = new StringBuilder(2 * 20);
	}

	private static final String HEXES = "0123456789abcdef";

	public String toHexString(byte[] raw) {
		if (raw == null) {
			return null;
		}
		hex.setLength(0);
		append(raw);
		return hex.toString();
	}

	public void append(byte[] raw) {
		for (final byte b : raw) {
			hex.append(HEXES.charAt((b & 0xF0) >> 4)).append(HEXES.charAt((b & 0x0F)));
		}
	}

	@Override
	public String toString() {
		return hex.toString();
	}

	public static String toHex(byte[] raw) {
		HexBuilder hex = new HexBuilder();
		return hex.toHexString(raw);
	}

	public static byte[] fromHex(String hex) {
		if (hex == null) return null;
		if (hex.length() % 2 != 0) throw new IllegalArgumentException(String.format("Hex string '%s' must be an even number of characters in length, but was %d characters long.", hex, hex.length()));
		byte[] data = new byte[hex.length() / 2];

		for (int i = 0; i < data.length; i++) {
			data[i] = (byte)( (0xF0 & (toNibble(hex.charAt(2*i + 0)) << 4)) | (0x0F & toNibble(hex.charAt(2*i + 1))) );
		}

		return data;
	}

	private static int toNibble(char hex) {
		switch(hex) {
			case '0': return 0;
			case '1': return 1;
			case '2': return 2;
			case '3': return 3;
			case '4': return 4;
			case '5': return 5;
			case '6': return 6;
			case '7': return 7;
			case '8': return 8;
			case '9': return 9;
			case 'a':
			case 'A': return 0xa;
			case 'b':
			case 'B': return 0xb;
			case 'c':
			case 'C': return 0xc;
			case 'd':
			case 'D': return 0xd;
			case 'e':
			case 'E': return 0xe;
			case 'f':
			case 'F': return 0xf;
		}
		throw new IllegalArgumentException("Invalid hex character: " + hex);
	}

}
