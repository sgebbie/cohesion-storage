/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage.store;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import net.gethos.cohesion.storage.RuntimeIOException;

/**
 * Contiguous storage via a random access file.
 *
 * @author {@literal Stewart Gebbie <sgebbie@gethos.net>}
 *
 */
public class RandomAccessContiguousStore implements ContiguousStore {

	private static final int ALLOC_ROUNDING = 4096;
	private static final int ALLOC_SCALING = 32; // when the backing is 1GiB this will cause allocation jumps of 32MiB

	private final File storeFile;
	private final FileChannel storeChannel;

	private final boolean force;

	/**
	 * Position of the first byte beyond the highest byte provisioned.
	 */
	private volatile long highAllocMark;

	/**
	 * Position of the first byte beyond the highest byte written.
	 */
	private volatile long highWriteMark;

	private volatile boolean closed;

	private final ByteBuffer zero;

	public RandomAccessContiguousStore(File storeFile, boolean sync, int initialCapacity) {
		this.zero = ByteBuffer.allocate(1);
		this.closed = false;
		this.storeFile = storeFile;
		this.force = sync;

		RandomAccessFile rf = null;
		try {
			if (!storeFile.exists() || storeFile.length() == 0) {
				// initialise empty store
				boolean ret = storeFile.createNewFile();
				if (!ret && !storeFile.exists()) throw new RuntimeIOException("Unable to create store file: " + storeFile);
				rf = new RandomAccessFile(this.storeFile, "rw");
				this.storeChannel = rf.getChannel();
				ByteBuffer zeros = ByteBuffer.allocate(initialCapacity);
				storeChannel.position(0);
				while(zeros.hasRemaining()) storeChannel.write(zeros);
				storeChannel.force(true);
			} else {
				this.storeChannel = new RandomAccessFile(this.storeFile, "rw").getChannel();
			}

			highAllocMark = storeChannel.size();
			highWriteMark = highAllocMark;

		} catch (IOException e) {
			if (rf != null) try {
				rf.close();
			} catch (IOException io) {
				throw new RuntimeIOException("Failed to close during error state", io);
			}
			throw new RuntimeIOException("Failed to open store", e);
		}
	}

	@Override
	public void close() {
		if (closed) return;
		closed = true;
		try {
			storeChannel.close();
		} catch (IOException e) {
			throw new RuntimeIOException("Failed to close store", e);
		}
		//		System.out.println("fcount="+fcount);
	}

	@Override
	protected void finalize() {
		close();
	}

	@Override
	public long write(long offset, ByteBuffer... buffers) {
		try {
			assert(highWriteMark <= highAllocMark);

			storeChannel.position(offset);
			long l = storeChannel.write(buffers);

			long writeMark = offset + l;
			// if we write beyond the end of the file
			// then grow the file to the next allocation boundary
			if (writeMark > highWriteMark) {
				highWriteMark = writeMark;
				truncate(highWriteMark);
			}

			assert(writeMark <= highWriteMark);
			assert(highWriteMark <= highAllocMark);

			return l;
		} catch (IOException e) {
			throw new RuntimeIOException("Failed to write to store", e);
		}
	}

	@Override
	public long read(long position, ByteBuffer... buffers) {
		try {
			storeChannel.position(position);
			return storeChannel.read(buffers);
		} catch (IOException e) {
			throw new RuntimeIOException("Failed to write to store", e);
		}
	}

	@Override
	public long truncate(long length) {

		final long boundary = boundary(length);

		truncate:
			try {

				if (highAllocMark == boundary) break truncate; // no change

				// Unfortunately, FileChannel.truncate(len) this does not behave like Unix truncate, so we need to perform a write to grow the file

				if (boundary > highAllocMark) {
					// grow
					// write at the new EOF position to force the file to grow
					if (boundary > highWriteMark) {
						zero.rewind();
						while(zero.hasRemaining()) storeChannel.write(zero, boundary-1);
					}
				} else {
					// shrink
					storeChannel.truncate(boundary);
				}

				// trim the high markers
				highAllocMark = boundary;
				if (highWriteMark > highAllocMark) highWriteMark = highAllocMark;


			} catch (IOException e) {
				throw new RuntimeIOException("Failed to resize store", e);
			}

			return boundary;
	}

	@Override
	public long size() {
		try {
			return storeChannel.size();
		} catch (IOException e) {
			throw new RuntimeIOException("Failed to read store size.", e);
		}
	}

//	public static int fcount = 0;
	@Override
	public void force() {
		try {
//			fcount++;
			if (force) storeChannel.force(false);
		} catch (IOException e) {
			throw new RuntimeIOException("Failed flush data to store", e);
		}
	}

	private static long boundary(long min) {
		// calculate the rounded length
		long allocBlock = min/ALLOC_SCALING;
		if (allocBlock < ALLOC_ROUNDING) allocBlock = ALLOC_ROUNDING;
		allocBlock = round(allocBlock,ALLOC_ROUNDING);
		long boundary = round(min,allocBlock);
		return boundary;
	}

	private static long round(long v, long factor) {
		long r = v % factor;
		long c = r == 0 ? v : ((v/factor)+1)*factor;
		assert(c>=v);
		return c;
	}

	public static ContiguousStore createTemporaryStore(boolean sync) {
		File tmpFile;
		try {
			tmpFile = File.createTempFile("ContiguousBacking_", ".store");
			tmpFile.deleteOnExit();
		} catch (IOException e) {
			throw new RuntimeIOException("Failed to create tempory file backed store.",e);
		}

		ContiguousStore store = new RandomAccessContiguousStore(tmpFile, sync, 4096*4);
		return store;
	}
}
