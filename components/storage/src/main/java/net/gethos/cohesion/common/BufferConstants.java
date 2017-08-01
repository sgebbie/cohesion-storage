/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.common;

import java.nio.ByteOrder;

public class BufferConstants {

	public static final ByteOrder NETWORK_ORDER = ByteOrder.BIG_ENDIAN;
	
	public static final int SIZEOF_BYTE  = 1;
	public static final int SIZEOF_SHORT = 2;
	public static final int SIZEOF_INT   = 4;
	public static final int SIZEOF_LONG  = 8;
	public static final int SIZEOF_SHA1  = 20;

	public static final int SIZEOF_CHAR = 2;
	public static final int SIZEOF_FLOAT = 4;
	public static final int SIZEOF_DOUBLE = 8;
}
