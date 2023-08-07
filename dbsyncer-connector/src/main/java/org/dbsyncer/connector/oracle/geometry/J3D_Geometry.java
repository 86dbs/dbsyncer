package org.dbsyncer.connector.oracle.geometry;

public class J3D_Geometry extends JGeometry {

    public J3D_Geometry(int var1, int var2, int[] var3, double[] var4) {
        super(var1 - 3000 >= 0 ? var1 : 3000 + var1, var2, var3, var4);
    }

    protected static double[][] getMBH(J3D_Geometry var0) {
        int var3 = var0.ordinates.length / 3;
        double[][] var5 = new double[3][2];
        double[][] var4 = new double[var3][3];

        int var1;
        for (var1 = 0; var1 < var3; ++var1) {
            var4[var1][0] = var0.ordinates[3 * var1];
            var4[var1][1] = var0.ordinates[3 * var1 + 1];
            var4[var1][2] = var0.ordinates[3 * var1 + 2];
        }

        double var6 = var4[0][0];
        double var8 = var4[0][1];
        double var10 = var4[0][2];
        double var12 = var4[0][0];
        double var14 = var4[0][1];
        double var16 = var4[0][2];

        for (var1 = 1; var1 < var3; ++var1) {
            if (var4[var1][0] > var6) {
                var6 = var4[var1][0];
            } else if (var4[var1][0] < var12) {
                var12 = var4[var1][0];
            }

            if (var4[var1][1] > var8) {
                var8 = var4[var1][1];
            } else if (var4[var1][1] < var14) {
                var14 = var4[var1][1];
            }

            if (var4[var1][2] > var10) {
                var10 = var4[var1][2];
            } else if (var4[var1][2] < var16) {
                var16 = var4[var1][2];
            }
        }

        var5[0][0] = var12;
        var5[0][1] = var6;
        var5[1][0] = var14;
        var5[1][1] = var8;
        var5[2][0] = var16;
        var5[2][1] = var10;
        return var5;
    }

}