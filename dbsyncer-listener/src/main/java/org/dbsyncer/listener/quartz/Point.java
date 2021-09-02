package org.dbsyncer.listener.quartz;

import org.dbsyncer.common.util.StringUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Point {

    private Map<String, String> position;
    private Map<String, String> command;
    private List<Object> args;
    private String beginKey;
    private String beginValue;
    private boolean refreshed;

    public Point() {
        this.position = new HashMap<>();
        this.command = new HashMap<>();
        this.args = new ArrayList<>();
    }

    public Point(Map<String, String> command, List<Object> args) {
        this.command = command;
        this.args = args;
    }

    public void setCommand(String key, String value) {
        command.put(key, value);
    }

    public void addArg(Object val) {
        args.add(val);
    }

    public void refresh() {
        if(StringUtil.isNotBlank(beginKey) && StringUtil.isNotBlank(beginValue)){
            position.put(beginKey, beginValue);
            refreshed = true;
        }
    }

    public boolean refreshed() {
        return refreshed;
    }

    public Map<String, String> getPosition() {
        return position;
    }

    public Map<String, String> getCommand() {
        return command;
    }

    public List<Object> getArgs() {
        return new ArrayList<>(args);
    }

    public void setBeginKey(String beginKey) {
        this.beginKey = beginKey;
    }

    public void setBeginValue(String beginValue) {
        this.beginValue = beginValue;
    }

}
