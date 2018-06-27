package org.apache.ignite.internal.processors.cache.persistence.recovery.read;

public interface ReadHandler<T> {

    void onNextRead(T next);

    void onComplete();
}
