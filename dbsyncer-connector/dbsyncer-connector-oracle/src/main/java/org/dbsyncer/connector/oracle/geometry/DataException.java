/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.oracle.geometry;

import java.io.PrintStream;
import java.io.PrintWriter;

public class DataException extends Exception {
    public String description = "";
    public Throwable detail = null;

    public DataException(String var1) {
        super(var1);
    }

    public String toString() {
        String var1 = "Message:" + this.getMessage() + "\n";
        var1 = var1 + "Description: " + this.description;
        if (this.detail != null) {
            var1 = var1 + "Nested exception is:\n" + this.detail.toString();
        }

        return var1;
    }

    public void printStackTrace(PrintStream var1) {
        if (this.detail == null) {
            super.printStackTrace(var1);
        } else {
            synchronized (var1) {
                var1.println(this);
                this.detail.printStackTrace(var1);
            }
        }

    }

    public void printStackTrace() {
        this.printStackTrace(System.err);
    }

    public void printStackTrace(PrintWriter var1) {
        if (this.detail == null) {
            super.printStackTrace(var1);
        } else {
            synchronized (var1) {
                var1.println(this);
                this.detail.printStackTrace(var1);
            }
        }

    }
}
