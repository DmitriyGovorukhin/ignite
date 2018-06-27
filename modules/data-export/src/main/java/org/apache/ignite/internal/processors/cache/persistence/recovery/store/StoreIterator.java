package org.apache.ignite.internal.processors.cache.persistence.recovery.store;


import org.apache.ignite.IgniteCheckedException;

public interface StoreIterator<T> {

    boolean hasNext();

    void next(T callBack) throws IgniteCheckedException;
}
