package org.dbsyncer.manager;

import org.dbsyncer.manager.command.Persistence;
import org.dbsyncer.manager.command.Preload;

public interface Command extends Persistence, Preload {
}