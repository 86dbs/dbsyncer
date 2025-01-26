/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.parser.model;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2025-01-26 21:36
 */
public final class Group {
    private final List<String> index = new LinkedList<>();

    public synchronized void remove(String e) {
        index.remove(e);
    }

    public int size() {
        return index.size();
    }

    public List<String> getIndex() {
        return Collections.unmodifiableList(index);
    }

    public boolean contains(String id) {
        return index.contains(id);
    }

    public void add(String id) {
        index.add(id);
    }

    public boolean isEmpty() {
        return index.isEmpty();
    }
}