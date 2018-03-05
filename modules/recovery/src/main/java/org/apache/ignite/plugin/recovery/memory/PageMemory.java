package org.apache.ignite.plugin.recovery.memory;

import org.apache.ignite.IgniteCheckedException;

public interface PageMemory {

    public long acquirePage(int grpId, long pageId) throws IgniteCheckedException;

    public void releasePage(int grpId, long pageId, long pageAbsPtr);
}
