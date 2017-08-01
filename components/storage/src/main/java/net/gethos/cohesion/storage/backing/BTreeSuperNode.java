/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage.backing;

/**
 * @author {@literal Stewart Gebbie <sgebbie@gethos.net>}
 *
 */
public interface BTreeSuperNode {
	
	/**
	 * This holds the depth of the leaf nodes, where the depth of the root == 0.
	 */
	public int depth();
	
	/**
	 * The offset of the current root node.
	 */
	public long root();
	
}
