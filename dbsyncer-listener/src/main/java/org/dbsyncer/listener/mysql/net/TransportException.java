package org.dbsyncer.listener.mysql.net;

import org.dbsyncer.listener.mysql.net.impl.packet.ErrorPacket;

import java.io.IOException;

public class TransportException extends IOException {
    private static final long serialVersionUID = 646149465892278906L;

    private int errorCode;
    private String sqlState;
    private String errorMessage;

    public TransportException() {
    }

    public TransportException(String message) {
        super(message);
        this.errorMessage = message;
    }

    public TransportException(ErrorPacket ep) {
        super(ep.getErrorMessage().toString());
        this.errorCode = ep.getErrorCode();
        this.sqlState = ep.getSqlState().toString();
        this.errorMessage = ep.getErrorMessage().toString();
    }

    public int getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(int errorCode) {
        this.errorCode = errorCode;
    }

    public String getSqlState() {
        return sqlState;
    }

    public void setSqlState(String sqlState) {
        this.sqlState = sqlState;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }
}
