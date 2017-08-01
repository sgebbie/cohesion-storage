/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage.cache;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A simple "least-recently-used" cache map.
 * 
 * @author {@literal Stewart Gebbie <sgebbie@gethos.net>}
 *
 */
public class LRUCache<K,V> extends LinkedHashMap<K, V> {

    private static final long serialVersionUID = 1L;
    private static final float LOAD_FACTOR = (float)0.75;
    private static final boolean ACCESS_ORDER = true;

    private final int maxSize;

    public LRUCache(int initialSize, int maxSize) {
        super(initialSize, LOAD_FACTOR, ACCESS_ORDER);
        this.maxSize = maxSize;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<K,V> eldest) {
        return size() > maxSize;
    }
}