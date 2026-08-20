/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.model.shard;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.enums.ShardSupportEnum;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 单个切片规格：调度只认 itemId；payload 由连接器自解释。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-08-20
 */
public final class ShardSpec {

    public static final String KEY_FROM = "from";
    public static final String KEY_TO = "to";
    public static final String KEY_PK = "pk";
    public static final String KEY_MOD = "mod";
    public static final String KEY_INDEX = "index";
    public static final String KEY_OFFSET_START = "offsetStart";
    public static final String KEY_OFFSET_END = "offsetEnd";
    public static final String KEY_PARTITION_ID = "partitionId";

    private final String itemId;
    private final ShardSupportEnum capability;
    private final Map<String, String> payload;

    public ShardSpec(String itemId, ShardSupportEnum capability, Map<String, String> payload) {
        this.itemId = itemId;
        this.capability = capability == null ? ShardSupportEnum.NONE : capability;
        this.payload = payload == null
                ? Collections.emptyMap()
                : Collections.unmodifiableMap(new LinkedHashMap<>(payload));
    }

    public static ShardSpec whole(String tableGroupId) {
        return new ShardSpec(tableGroupId, ShardSupportEnum.NONE, Collections.emptyMap());
    }

    public static ShardSpec range(String itemId, String pk, String from, String to) {
        Map<String, String> payload = new LinkedHashMap<>(4);
        if (StringUtil.isNotBlank(pk)) {
            payload.put(KEY_PK, pk);
        }
        payload.put(KEY_FROM, from);
        payload.put(KEY_TO, to);
        return new ShardSpec(itemId, ShardSupportEnum.RANGE, payload);
    }

    public static ShardSpec hashMod(String itemId, String pk, int mod, int index) {
        Map<String, String> payload = new LinkedHashMap<>(4);
        if (StringUtil.isNotBlank(pk)) {
            payload.put(KEY_PK, pk);
        }
        payload.put(KEY_MOD, String.valueOf(mod));
        payload.put(KEY_INDEX, String.valueOf(index));
        return new ShardSpec(itemId, ShardSupportEnum.HASH_MOD, payload);
    }

    public static ShardSpec offset(String itemId, long start, long end) {
        Map<String, String> payload = new LinkedHashMap<>(2);
        payload.put(KEY_OFFSET_START, String.valueOf(start));
        payload.put(KEY_OFFSET_END, String.valueOf(end));
        return new ShardSpec(itemId, ShardSupportEnum.OFFSET, payload);
    }

    public String getItemId() {
        return itemId;
    }

    public ShardSupportEnum getCapability() {
        return capability;
    }

    public Map<String, String> getPayload() {
        return payload;
    }

    public String payload(String key) {
        return payload.get(key);
    }

    public boolean isWhole() {
        return capability == ShardSupportEnum.NONE;
    }
}
