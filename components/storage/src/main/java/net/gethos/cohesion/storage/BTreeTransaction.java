/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage;

import java.nio.ByteBuffer;


public interface BTreeTransaction extends ReadOnlyBTreeTransaction {

	/**
	 * Find an item stored in the tree and remove it, and the keys referring to it, from the tree.
	 *
	 * @param key
	 * @return the reference to the item that was deleted, or null if the key did not exist in the tree
	 */
	public BTree.Reference delete(BTree.Key key);

	/**
	 * Truncate the object data associated with the key, to the given length.
	 * <p>
	 * Note, this will allocate new storage if the key does not exist.
	 * Additionally, this could shrink or extend the existing storage associated with this key.
	 * As a result, an existing item may be relocated.
	 * <p>
	 * Note, this reference is only valid until the next operation that might affect the tree: delete, truncate.
	 * (The plain store will not affect the reference as the space must have already been provisioned).
	 * 
	 * @param key
	 * @param length
	 * @return reference to the, possibly new, location of the item that was truncated, or null of the key does not exit in the tree.
	 */
	public BTree.Reference truncate(BTree.Key key, long length);

	/**
	 * Update an existing data item in the tree. This allows for random writes to large items.
	 * <p>
	 * Note, space must have already been provisioned via a previous truncate (or store) call.
	 *
	 * @param ref
	 * @param objectOffset
	 * @param buffer
	 * @return the amount of data stored
	 */
	public int store(BTree.Reference ref, long objectOffset, ByteBuffer buffer);

	// -- convenience methods

	/**
	 * Truncate the object data associated with the reference, to the given length.
	 * <p>
	 * Equivalent to <code>truncate(key(ref),length)</code>.
	 * <p>
	 * Note, however, that the update may necessitate a relocation of the actual item
	 * (e.g. data moved from leaf-node storage to an explicit extent etc.). As such,
	 * a possibly different reference will be returned.
	 * 
	 * @param ref
	 * @param length
	 * @return reference to the location of the item following its truncation.
	 */
	public BTree.Reference truncate(BTree.Reference ref, long length);

	/**
	 * Store data in the tree using the key to position the data in the tree.
	 * <p>
	 * Equivalent to <code>store(search(key),objectOffset,buffer)</code>.
	 * 
	 * @param key
	 * @param objectOffset
	 * @param buffer
	 * @return store the data and return the reference to the item where this value was stored
	 */
	public BTree.Reference store(BTree.Key key, long objectOffset, ByteBuffer buffer); // XXX consider returning null if the key already existed.

	/**
	 * Modify the non-index parameter component of the key.
	 * <p>
	 * This updates the parameter in the leaf node item. Note, as an
	 * implementation detail, any parent nodes hold the key are not
	 * necessarily updated.
	 * 
	 * @param key
	 * @param parameter
	 */
	//public BTree.Reference update(BTree.Key key, long parameter); // TODO
}
