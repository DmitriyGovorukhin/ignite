package org.apache.ignite.plugin.recovery.utils;

import java.util.HashMap;
import java.util.Map;

import static org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO.*;

public class PageTypeMapping {

    private static final Map<Short, String> TYPE_TO_STRING = new HashMap<>();

    static {
        TYPE_TO_STRING.put(T_DATA, "data");
        TYPE_TO_STRING.put(T_BPLUS_META, "b_plus_tree_meta");
        TYPE_TO_STRING.put(T_H2_REF_LEAF, "data");
        TYPE_TO_STRING.put(T_H2_REF_INNER, "data");
        TYPE_TO_STRING.put(T_DATA_REF_INNER, "data");
        TYPE_TO_STRING.put(T_DATA_REF_LEAF, "data_ref_leaf");
        TYPE_TO_STRING.put(T_METASTORE_INNER, "data");
        TYPE_TO_STRING.put(T_METASTORE_LEAF, "data");
        TYPE_TO_STRING.put(T_PENDING_REF_INNER, "data");
        TYPE_TO_STRING.put(T_PENDING_REF_LEAF, "data");
        TYPE_TO_STRING.put(T_META, "data");
        TYPE_TO_STRING.put(T_PAGE_LIST_META, "page_list_meta");
        TYPE_TO_STRING.put(T_PAGE_LIST_NODE, "page_list_node");
        TYPE_TO_STRING.put(T_PART_META, "part_meta");
        TYPE_TO_STRING.put(T_PAGE_UPDATE_TRACKING, "page_update_tracing");
        TYPE_TO_STRING.put(T_CACHE_ID_AWARE_DATA_REF_INNER, "data");
    }

    public static String strType(int type) {
        return TYPE_TO_STRING.get((short)type);
    }

    public static int intType(String type) {
        return 0;
    }
}
