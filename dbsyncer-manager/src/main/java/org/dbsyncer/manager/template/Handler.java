package org.dbsyncer.manager.template;

public interface Handler<C extends Callback> {

    Object execute(C callback);
}