package org.dbsyncer.connector.oracle.geometry;

import java.io.Serializable;

public class JPoint2DD implements Serializable {
    public double x;
    public double y;

    public JPoint2DD() {
        this.x = this.y = 0.0D;
    }

    public JPoint2DD(double var1, double var3) {
        this.x = var1;
        this.y = var3;
    }

    public JPoint2DD(JPoint2DD var1) {
        this.x = var1.x;
        this.y = var1.y;
    }

    public double[] getArray() {
        return new double[]{this.x, this.y};
    }

    public double getX() {
        return this.x;
    }

    public double getY() {
        return this.y;
    }

    public void setX(double var1) {
        this.x = var1;
    }

    public void setY(double var1) {
        this.y = var1;
    }

    public String toString() {
        return new String(this.x + " " + this.y);
    }

    public boolean equals(JPoint2DD var1) {
        return var1.x == this.x && var1.y == this.y;
    }
}
