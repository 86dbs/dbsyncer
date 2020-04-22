package org.dbsyncer.manager.template.impl;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/12/2 23:18
 */
public class Group {

    private List<String> index;

    public Group() {
        this.index = new LinkedList<>();
    }

    public synchronized void addIfAbsent(String e) {
        if (!index.contains(e)) {
            index.add(e);
        }
    }

    public synchronized void remove(String e) {
        index.remove(e);
    }

    public List<String> subList(int fromIndex, int toIndex) {
        return index.subList(fromIndex, toIndex);
    }

    public int size(){
        return index.size();
    }

    public List<String> getAll() {
        return Collections.unmodifiableList(index);
    }

}