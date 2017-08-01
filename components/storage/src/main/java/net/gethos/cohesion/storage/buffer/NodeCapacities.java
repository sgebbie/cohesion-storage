/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage.buffer;


/**
 * Provides access to data stored in a single
 * contiguous region, together with node capacities. 
 * 
 * @author {@literal Stewart Gebbie <sgebbie@gethos.net>}
 *
 */
public interface NodeCapacities {

	public int innerCapacity();

	public int leafCapacity();

	public int capacity(boolean isLeaf);

}
