/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.postgresql.enums;

public enum MessageTypeEnum {

    BEGIN, COMMIT, TABLE, INSERT, UPDATE, DELETE, RELATION, TRUNCATE, TYPE, ORIGIN, NONE;

    public static MessageTypeEnum getType(char type) {
        switch (type) {
            case 'B':
                return BEGIN;
            case 'C':
                return COMMIT;
            case 't':
                return TABLE;
            case 'I':
                return INSERT;
            case 'U':
                return UPDATE;
            case 'D':
                return DELETE;
            case 'R':
                return RELATION;
            case 'Y':
                return TYPE;
            case 'O':
                return ORIGIN;
            case 'T':
                return TRUNCATE;
            default:
                return NONE;
        }
    }
}