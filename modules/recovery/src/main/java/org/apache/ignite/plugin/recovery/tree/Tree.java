package org.apache.ignite.plugin.recovery.tree;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.lang.GridCursor;

public interface Tree<K, V> {

    public V find(K key) throws IgniteCheckedException;

    public GridCursor<V> find(K lower, K upper) throws IgniteCheckedException;

    public long size() throws IgniteCheckedException;
}
