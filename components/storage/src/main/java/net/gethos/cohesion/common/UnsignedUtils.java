/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.common;

/**
 * Utilities for emulating unsigned integer operations.
 *
 * @author {@literal Stewart Gebbie <sgebbie@gethos.net>}
 *
 */
public class UnsignedUtils {

	/**
	 * Trick to format with zeros. This works by creating a mask e.g. 1000000 if 6 digits are needed.
	 * Then formating this as a hex value and dropping the leading '1'.
	 *
	 * @return formated as hex to 'digits' long.
	 */
	public static final String digits(long val, int digits) {
		if (digits > 16 || digits < 0) throw new IllegalArgumentException("A long can have at most 16 hex digits as it is only 8 bytes long, and at least 0 digits: " + digits);

		// special case, since the trick below requires the 16th digit to work
		if (digits == 16) return digits(val>>>32,8) + digits(val,8);

		long hi = 1L << (digits * 4);
		return Long.toHexString(hi | (val & (hi - 1))).substring(1);
	}

	public static final int compareUnsigned(long n1, long n2) {
		if (n1 == n2) return 0;
		return ((n1 < n2) ^ ((n1 < 0) != (n2 < 0))) ? -1 : 1;
	}

	public static final int compareUnsigned(int n1, int n2) {
		return (n1 == n2) ? 0 : ((n1 & 0xffffffffL) < (n2 & 0xffffffffL) ? -1 : 1);
	}

	public static final int compareUnsigned(short n1, short n2) {
		if (n1 == n2) return 0;
		return (n1 & 0xffff) < (n2 & 0xffff) ? -1 : 1;
	}

	public static final int compareUnsigned(byte n1, byte n2) {
		if (n1 == n2) return 0;
		return (n1 & 0xffff) < (n2 & 0xffff) ? -1 : 1;
	}

	private static final long LONG_SIGN = 0x8000000000000000L;

	public static int compareUnsignedVerbose(long a, long b) {
		if (a == b) return 0;
		if ((a & LONG_SIGN) == 0L) {
			if ((b & LONG_SIGN) == 0L) {
				return a < b ? -1 : 1;
			} else {
				return -1;
			}
		} else {
			if ((b & LONG_SIGN) == 0L) {
				return 1;
			} else {
				return (a & ~LONG_SIGN) < (b & ~LONG_SIGN) ? -1 : 1;
			}
		}
	}

	/* Notes,
	 *	The following describes some details re unsigned in Java:
	 *		http://www.javamex.com/java_equivalents/unsigned.shtml
	 *		http://www.javamex.com/java_equivalents/unsigned_arithmetic.shtml
	 */

}
