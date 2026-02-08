/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.oracle.geometry;

import oracle.sql.ARRAY;
import oracle.sql.ArrayDescriptor;
import oracle.sql.Datum;
import oracle.sql.NUMBER;
import oracle.sql.STRUCT;
import oracle.sql.StructDescriptor;

import oracle.sql.ARRAY;
import oracle.sql.ArrayDescriptor;
import oracle.sql.Datum;
import oracle.sql.NUMBER;
import oracle.sql.STRUCT;
import oracle.sql.StructDescriptor;

import java.awt.Shape;
import java.awt.geom.AffineTransform;
import java.awt.geom.GeneralPath;
import java.awt.geom.Point2D;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;

public class JGeometry implements Cloneable, Serializable {

    static final long serialVersionUID = -4792272186565640701L;
    protected static StructDescriptor geomDesc = null;
    protected static StructDescriptor pointDesc = null;
    protected static ArrayDescriptor elemInfoDesc = null;
    protected static ArrayDescriptor ordinatesDesc = null;
    protected int gtype;
    protected int linfo;
    protected int srid;
    protected double x;
    protected double y;
    protected double z;
    protected int[] elemInfo;
    protected double[] ordinates;
    protected double[] mbr;
    protected int dim;
    protected JGeometry.LT_transform lttpH;
    protected JGeometry.Gc_trans gtransH;
    private static final double MERCATOR_e3785 = Math.sqrt(0.0D);
    private static final double MERCATOR_e54004 = Math.sqrt(0.0066943799901413165D);
    private static final double MERCATOR_B = Math.exp(1.0D);

    private boolean lltogXYZ(double[] var1, JGeometry.Gc_trans var2) {
        var1[0] *= var2.unitfactor;
        var1[1] *= var2.unitfactor;
        double var3 = Math.cos(var1[1]);
        double var5 = Math.sin(var1[1]);
        double var7 = Math.cos(var1[0]);
        double var9 = Math.sin(var1[0]);
        double var11 = var2.smax / Math.sqrt(1.0D - var2.esq * var5 * var5);
        var1[0] = var3 * var7 * (var11 + var1[2]);
        var1[1] = var3 * var9 * (var11 + var1[2]);
        var1[2] = var5 * ((1.0D - var2.esq) * var11 + var1[2]);
        return true;
    }

    private boolean gXYZtoll(double[] var1, JGeometry.Gc_trans var2) {
        double var11 = var1[0] * var1[0] + var1[1] * var1[1];
        double var9 = (1.0D + var2.esq) * var1[2];
        double var7 = var9 / Math.sqrt(var11 + var9 * var9);

        double var3;
        double var5;
        do {
            var5 = var7;
            var3 = var2.smax / Math.sqrt(1.0D - var2.esq * var7 * var7);
            var9 = var1[2] + var3 * var2.esq * var7;
            var7 = var9 / Math.sqrt(var11 + var9 * var9);
        } while (Math.abs(var7 - var5) > 1.0E-15D);

        var3 = var2.smax / Math.sqrt(1.0D - var2.esq * var7 * var7);
        var9 = var1[2] + var3 * var2.esq * var7;
        var1[0] = Math.atan2(var1[1], var1[0]);
        var1[1] = Math.asin(var7);
        var1[2] = Math.sqrt(var11 + var9 * var9) - var3;
        return true;
    }

    private boolean gxyzgmcenter(double[] var1, double[] var2, JGeometry.Gc_trans var3) {
        for (int var4 = 0; var4 < 3; ++var4) {
            var1[var4] = (var2[var4] + var2[var4 + 3]) / 2.0D;
        }

        double var7 = var3.radius / Math.sqrt(var1[0] * var1[0] + var1[1] * var1[1] + var1[2] * var1[2]);

        for (int var6 = 0; var6 < 3; ++var6) {
            var1[var6] *= var7;
        }

        return this.gXYZtoll(var1, var3);
    }

    private boolean ltxform(double[] var1, JGeometry.LT_transform var2) {
        double var3 = var1[0] - var2.xc;
        double var5 = var1[1] - var2.yc;
        double var7 = var1[2] - var2.zc;
        var1[0] = var2.xrow[0] * var3 + var2.xrow[1] * var5 + var2.xrow[2] * var7;
        var1[1] = var2.yrow[0] * var3 + var2.yrow[1] * var5 + var2.yrow[2] * var7;
        var1[2] = 0.0D;
        return true;
    }

    private boolean iltxform(double[] var1, JGeometry.LT_transform var2, JGeometry.Gc_trans var3) {
        double var4 = var1[0];
        double var6 = var1[1];
        double var8 = 0.0D;
        var8 = Math.sqrt(var3.radius * var3.radius - var4 * var4 - var6 * var6) - var3.radius;
        var1[0] = var2.xrow[0] * var4 + var2.yrow[0] * var6 + var2.zrow[0] * var8 + var2.xc;
        var1[1] = var2.xrow[1] * var4 + var2.yrow[1] * var6 + var2.zrow[1] * var8 + var2.yc;
        var1[2] = var2.xrow[2] * var4 + var2.yrow[2] * var6 + var2.zrow[2] * var8 + var2.zc;
        return true;
    }

    protected JGeometry(int var1, int var2) {
        this.gtype = 0;
        this.linfo = 0;
        this.srid = 0;
        this.x = 0.0D / 0.0;
        this.y = 0.0D / 0.0;
        this.z = 0.0D / 0.0;
        this.elemInfo = null;
        this.ordinates = null;
        this.mbr = null;
        this.dim = 2;
        this.gtype = var1 % 100;
        this.linfo = var1 % 1000 / 100;
        this.dim = var1 / 1000 > 0 ? var1 / 1000 : 2;
        this.srid = var2 <= 0 ? 0 : var2;
    }

    public JGeometry(int var1, int var2, double var3, double var5, double var7, int[] var9, double[] var10) {
        this(var1, var2);
        this.x = var3;
        this.y = var5;
        this.z = var7;
        if (var9 != null && var10 != null) {
            if (etype0_exists(var9) && !ordOffset0_exists(var9)) {
                ArrayList var11 = new ArrayList();
                ArrayList var12 = new ArrayList();
                remove_etype0(var9, var10, var11, var12);
                int[] var13 = new int[var11.size()];
                double[] var14 = new double[var12.size()];

                int var15;
                for (var15 = 0; var15 < var11.size(); ++var15) {
                    var13[var15] = (Integer) var11.get(var15);
                }

                for (var15 = 0; var15 < var12.size(); ++var15) {
                    var14[var15] = (Double) var12.get(var15);
                }

                this.elemInfo = var13;
                this.ordinates = var14;
            } else {
                this.elemInfo = var9;
                this.ordinates = var10;
            }
        } else {
            this.elemInfo = var9;
            this.ordinates = var10;
        }

    }

    public JGeometry(int var1, int var2, int[] var3, double[] var4) {
        this(var1, var2);
        if (var3 != null && var4 != null) {
            if (etype0_exists(var3) && !ordOffset0_exists(var3)) {
                ArrayList var5 = new ArrayList();
                ArrayList var6 = new ArrayList();
                remove_etype0(var3, var4, var5, var6);
                int[] var7 = new int[var5.size()];
                double[] var8 = new double[var6.size()];

                int var9;
                for (var9 = 0; var9 < var5.size(); ++var9) {
                    var7[var9] = (Integer) var5.get(var9);
                }

                for (var9 = 0; var9 < var6.size(); ++var9) {
                    var8[var9] = (Double) var6.get(var9);
                }

                this.elemInfo = var7;
                this.ordinates = var8;
            } else {
                this.elemInfo = var3;
                this.ordinates = var4;
            }
        } else {
            this.elemInfo = var3;
            this.ordinates = var4;
        }

    }

    public JGeometry(double var1, double var3, int var5) {
        this(1, var5);
        this.x = var1;
        this.y = var3;
    }

    public JGeometry(double var1, double var3, double var5, int var7) {
        this(3001, var7);
        this.x = var1;
        this.y = var3;
        this.z = var5;
    }

    public JGeometry(double var1, double var3, double var5, double var7, int var9) {
        this(3, var9);
        this.elemInfo = new int[3];
        this.elemInfo[0] = 1;
        this.elemInfo[1] = 1003;
        this.elemInfo[2] = 3;
        this.ordinates = new double[4];
        this.ordinates[0] = var1;
        this.ordinates[1] = var3;
        this.ordinates[2] = var5;
        this.ordinates[3] = var7;
    }

    public Object clone() {
        try {
            JGeometry var1 = (JGeometry) super.clone();
            if (this.elemInfo != null) {
                var1.elemInfo = new int[this.elemInfo.length];
                System.arraycopy(this.elemInfo, 0, var1.elemInfo, 0, this.elemInfo.length);
            }

            if (this.ordinates != null) {
                var1.ordinates = new double[this.ordinates.length];
                System.arraycopy(this.ordinates, 0, var1.ordinates, 0, this.ordinates.length);
            }

            if (this.mbr != null) {
                var1.mbr = new double[this.mbr.length];
                System.arraycopy(this.mbr, 0, var1.mbr, 0, this.mbr.length);
            }

            return var1;
        } catch (CloneNotSupportedException var2) {
            throw new InternalError(var2.toString());
        }
    }

    public static JGeometry createPoint(double[] var0, int var1, int var2) {
        JGeometry var3 = new JGeometry(1, var2);
        var3.dim = var0.length;
        var3.x = var0[0];
        var3.y = var0[1];
        if (var1 == 3) {
            var3.z = var0[2];
        }

        return var3;
    }

    public static JGeometry createLRSPoint(double[] var0, int var1, int var2) {
        try {
            int[] var3;
            JGeometry var4;
            if (var1 == 2 && var0.length == 3) {
                var3 = new int[]{1, 1, 1};
                var4 = new JGeometry(3301, var2, var3, var0);
                return var4;
            } else if (var1 == 3 && var0.length == 4) {
                var3 = new int[]{1, 1, 1};
                var4 = new JGeometry(4401, var2, var3, var0);
                return var4;
            } else {
                throw new RuntimeException("Unsupported LRS Point type");
            }
        } catch (Exception var5) {
            System.out.println("Error: " + var5);
            return null;
        }
    }

    public static JGeometry createCircle(double var0, double var2, double var4, int var6) {
        return createCircle(var0 + var4 * Math.cos(0.7853981633974483D), var2 + var4 * Math.sin(0.7853981633974483D), var0 + var4 * Math.cos(2.356194490192345D), var2
                + var4 * Math.sin(2.356194490192345D), var0 + var4 * Math.cos(4.71238898038469D), var2 + var4 * Math.sin(4.71238898038469D), var6);
    }

    public static JGeometry createCircle(double var0, double var2, double var4, double var6, double var8, double var10, int var12) {
        if (orientation(var0, var2, var4, var6, var8, var10) == 0.0D) {
            return null;
        } else {
            JGeometry var13 = new JGeometry(3, var12);
            var13.elemInfo = new int[3];
            var13.elemInfo[0] = 1;
            var13.elemInfo[1] = 1003;
            var13.elemInfo[2] = 4;
            var13.ordinates = new double[6];
            var13.ordinates[0] = var0;
            var13.ordinates[1] = var2;
            var13.ordinates[2] = var4;
            var13.ordinates[3] = var6;
            var13.ordinates[4] = var8;
            var13.ordinates[5] = var10;
            return var13;
        }
    }

    public static JGeometry createArc2d(double[] var0, int var1, int var2) {
        JGeometry var3 = new JGeometry(2, var2);
        var3.dim = var1;
        var3.elemInfo = new int[3];
        var3.elemInfo[0] = 1;
        var3.elemInfo[1] = 2;
        var3.elemInfo[2] = 2;
        var3.ordinates = var0;
        return var3;
    }

    public static JGeometry createLinearLineString(double[] var0, int var1, int var2) {
        JGeometry var3 = new JGeometry(2, var2);
        var3.dim = var1;
        var3.elemInfo = new int[3];
        var3.elemInfo[0] = 1;
        var3.elemInfo[1] = 2;
        var3.elemInfo[2] = 1;
        var3.ordinates = var0;
        return var3;
    }

    public static JGeometry createLRSLinearLineString(double[] var0, int var1, int var2) {
        try {
            JGeometry var3;
            if (var1 == 2) {
                var3 = createLinearLineString(var0, 3, var2);
                var3.setType(3302);
                if (monoMeasure(var3.ordinates, var1 + 1) != 0) {
                    return var3;
                } else {
                    System.out.println("Inconsistent LRS Measure Values");
                    return null;
                }
            } else if (var1 == 3) {
                var3 = createLinearLineString(var0, 4, var2);
                var3.setType(4402);
                if (monoMeasure(var3.ordinates, var1 + 1) != 0) {
                    return var3;
                } else {
                    System.out.println("Inconsistent LRS Measure Values");
                    return null;
                }
            } else {
                throw new RuntimeException("Unsupported LRS Line type");
            }
        } catch (Exception var4) {
            System.out.println("Error: " + var4);
            return null;
        }
    }

    public static JGeometry createLinearMultiLineString(Object[] var0, int var1, int var2) {
        if (var0.length == 1) {
            return createLinearLineString((double[]) ((double[]) var0[0]), var1, var2);
        } else {
            JGeometry var3 = new JGeometry(6, var2);
            var3.dim = var1;
            int var4 = var0.length;
            int var5 = 1;
            var3.elemInfo = new int[var4 * 3];

            for (int var6 = 0; var6 < var4; ++var6) {
                var3.elemInfo[var6 * 3 + 0] = var5;
                var3.elemInfo[var6 * 3 + 1] = 2;
                var3.elemInfo[var6 * 3 + 2] = 1;
                var5 += ((double[]) ((double[]) var0[var6])).length;
            }

            double[] var10 = new double[var5 - 1];
            int var7 = 0;

            for (int var8 = 0; var8 < var4; ++var8) {
                double[] var9 = (double[]) ((double[]) var0[var8]);
                System.arraycopy(var9, 0, var10, var7, var9.length);
                var7 += var9.length;
            }

            var3.ordinates = var10;
            return var3;
        }
    }

    public static JGeometry createLRSLinearMultiLineString(Object[] var0, int var1, int var2) {
        try {
            JGeometry var3;
            if (var1 == 2) {
                var3 = createLinearMultiLineString(var0, 3, var2);
                var3.setType(3306);
                if (monoMeasure(var3.ordinates, var1 + 1) != 0) {
                    return var3;
                } else {
                    System.out.println("Inconsistent LRS Measure Values");
                    return null;
                }
            } else if (var1 == 3) {
                var3 = createLinearMultiLineString(var0, 4, var2);
                var3.setType(4406);
                if (monoMeasure(var3.ordinates, var1 + 1) != 0) {
                    return var3;
                } else {
                    System.out.println("Inconsistent LRS Measure Values");
                    return null;
                }
            } else {
                throw new RuntimeException("Unsupported LRS MultiLine type");
            }
        } catch (Exception var4) {
            System.out.println("Error: " + var4);
            return null;
        }
    }

    public static JGeometry createMultiPoint(Object[] var0, int var1, int var2) {
        if (var0.length == 1) {
            return createPoint((double[]) ((double[]) var0[0]), var1, var2);
        } else {
            JGeometry var3 = new JGeometry(5, var2);
            var3.dim = var1;
            int var4 = var0.length;
            var3.elemInfo = new int[3];
            var3.elemInfo[0] = 1;
            var3.elemInfo[1] = 1;
            var3.elemInfo[2] = var4;
            double[] var5 = new double[var4 * var1];
            int var6 = 0;

            for (int var7 = 0; var7 < var4; ++var7) {
                double[] var8 = (double[]) ((double[]) var0[var7]);
                System.arraycopy(var8, 0, var5, var6, var1);
                var6 += var1;
            }

            var3.ordinates = var5;
            return var3;
        }
    }

    public static JGeometry createLinearPolygon(double[] var0, int var1, int var2) {
        JGeometry var3 = new JGeometry(3, var2);
        var3.dim = var1;
        var3.elemInfo = new int[3];
        var3.elemInfo[0] = 1;
        var3.elemInfo[1] = 1003;
        var3.elemInfo[2] = 1;
        var3.ordinates = closeCoords(var0, var1);
        return var3;
    }

    public static JGeometry createLinearPolygon(Object[] var0, int var1, int var2) {
        if (var0.length == 1) {
            return createLinearPolygon((double[]) ((double[]) var0[0]), var1, var2);
        } else {
            JGeometry var3 = new JGeometry(3, var2);
            var3.dim = var1;
            int var4 = var0.length;
            Object[] var5 = new Object[var4];

            int var6;
            for (var6 = 0; var6 < var4; ++var6) {
                var5[var6] = closeCoords((double[]) ((double[]) var0[var6]), var1);
            }

            var6 = 1;
            var3.elemInfo = new int[var4 * 3];

            for (int var7 = 0; var7 < var4; ++var7) {
                var3.elemInfo[var7 * 3 + 0] = var6;
                var3.elemInfo[var7 * 3 + 1] = var7 == 0 ? 1003 : 2003;
                var3.elemInfo[var7 * 3 + 2] = 1;
                var6 += ((double[]) ((double[]) var5[var7])).length;
            }

            double[] var11 = new double[var6 - 1];
            int var8 = 0;

            for (int var9 = 0; var9 < var4; ++var9) {
                double[] var10 = (double[]) ((double[]) var5[var9]);
                System.arraycopy(var10, 0, var11, var8, var10.length);
                var8 += var10.length;
            }

            var3.ordinates = var11;
            return var3;
        }
    }

    public static JGeometry createLRSLinearPolygon(double[] var0, int var1, int var2) {
        try {
            JGeometry var3;
            if (var1 == 2) {
                var3 = createLinearPolygon((double[]) var0, 3, var2);
                var3.setType(3303);
                if (monoMeasure(var3.ordinates, var1 + 1) != 0) {
                    return var3;
                } else {
                    System.out.println("Inconsistent LRS Measure Values");
                    return null;
                }
            } else if (var1 == 3) {
                var3 = createLinearPolygon((double[]) var0, 4, var2);
                var3.setType(4403);
                if (monoMeasure(var3.ordinates, var1 + 1) != 0) {
                    return var3;
                } else {
                    System.out.println("Inconsistent LRS Measure Values");
                    return null;
                }
            } else {
                throw new RuntimeException("Unsupported LRS Polygon type");
            }
        } catch (Exception var4) {
            System.out.println("Error: " + var4);
            return null;
        }
    }

    public static JGeometry createLRSLinearPolygon(Object[] var0, int var1, int var2) {
        try {
            if (var0 instanceof double[][]) {
                for (int var3 = 0; var3 < var0.length; ++var3) {
                    double[] var4 = ((double[][]) ((double[][]) var0))[var3];
                    if (monoMeasure(var4, var1 + 1) == 0) {
                        System.out.println("Inconsistent LRS Measure Values");
                        return null;
                    }
                }
            }

            JGeometry var6;
            if (var1 == 2) {
                var6 = createLinearPolygon((Object[]) var0, 3, var2);
                var6.setType(3303);
                return var6;
            } else if (var1 == 3) {
                var6 = createLinearPolygon((Object[]) var0, 4, var2);
                var6.setType(4403);
                return var6;
            } else {
                throw new RuntimeException("Unsupported LRS Polygon type");
            }
        } catch (Exception var5) {
            System.out.println("Error: " + var5);
            return null;
        }
    }

    public static int monoMeasure(double[] var0, int var1) {
        double var2 = 0.0D / 0.0;
        int var4 = 0;
        boolean var5 = false;
        double var6 = 0.0D / 0.0;

        int var13;
        for (var13 = var1 - 1; var13 < var0.length; var13 += var1) {
            var2 = var0[var13];
            if (!Double.isNaN(var2)) {
                var4 = var13;
                break;
            }
        }

        int var14;
        if (var0[var4] == var0[var0.length - 1] && var0[var4 - (var1 - 2)] == var0[var0.length - (var1 - 1)] && var0[var4 - (var1 - 1)] == var0[var0.length - var1]) {
            var14 = var0.length - var1;
        } else {
            var14 = var0.length;
        }

        if (Double.isNaN(var2)) {
            return 1;
        } else {
            byte var8 = 0;
            double var11 = var2;

            for (var13 = var4 + var1; var13 < var14; var13 += var1) {
                var6 = var0[var13];
                if (!Double.isNaN(var6)) {
                    double var9 = var6 - var11;
                    if (var8 == 0) {
                        if (var9 > 0.0D) {
                            var8 = 1;
                        } else if (var9 < 0.0D) {
                            var8 = -1;
                        } else {
                            var8 = 0;
                        }
                    } else if (var9 * (double) var8 < 0.0D) {
                        return 0;
                    }

                    var11 = var6;
                }
            }

            if (var8 == 0) {
                return 1;
            } else {
                return var8;
            }
        }
    }

    public int getType() {
        return this.gtype;
    }

    public void setType(int var1) {
        this.gtype = var1 % 100;
        this.linfo = var1 % 1000 / 100;
        if (var1 / 1000 > 0) {
            this.dim = var1 / 1000;
        }

    }

    public int getLRMDimension() {
        return this.linfo;
    }

    public void setLRMDimension(int var1) {
        this.linfo = var1;
    }

    public int getSRID() {
        return this.srid;
    }

    public void setSRID(int var1) {
        this.srid = var1;
    }

    public Point2D getLabelPoint() {
        return !Double.isNaN(this.x) && !Double.isNaN(this.y) ? new java.awt.geom.Point2D.Double(this.x, this.y) : null;
    }

    public double[] getLabelPointXYZ() {
        return new double[]{this.x, this.y, this.z};
    }

    public double[] getPoint() {
        if (this.gtype != 1) {
            return null;
        } else {
            double[] var1 = new double[this.dim];
            if ((Double.isNaN(this.x) || Double.isNaN(this.y)) && (this.elemInfo == null || this.elemInfo.length == 0)) {
                return null;
            } else if (this.elemInfo == null && !Double.isNaN(this.x) && !Double.isNaN(this.y)) {
                var1[0] = this.x;
                var1[1] = this.y;
                if (this.dim > 2) {
                    var1[2] = this.z;
                }

                return var1;
            } else if (this.elemInfo.length != 3) {
                if (this.elemInfo.length == 6 && this.elemInfo[4] == 1 && this.elemInfo[5] == 0) {
                    var1[0] = this.ordinates[0];
                    var1[1] = this.ordinates[1];
                    if (this.dim > 2) {
                        var1[2] = this.ordinates[2];
                    }

                    return var1;
                } else {
                    double[] var3 = this.getMBR();
                    if (var3 == null) {
                        return null;
                    } else {
                        var1[0] = var3[0];
                        var1[1] = var3[1];
                        return var1;
                    }
                }
            } else {
                for (int var2 = 0; var2 < this.dim; ++var2) {
                    var1[var2] = this.ordinates[var2];
                }

                return var1;
            }
        }
    }

    public Point2D getJavaPoint() {
        if (this.gtype != 1) {
            return null;
        } else if (!Double.isNaN(this.x) && !Double.isNaN(this.y)) {
            return new java.awt.geom.Point2D.Double(this.x, this.y);
        } else if (this.elemInfo.length == 3) {
            return new java.awt.geom.Point2D.Double(this.ordinates[0], this.ordinates[1]);
        } else if (this.elemInfo.length == 6 && this.elemInfo[4] == 1 && this.elemInfo[5] == 0) {
            return new java.awt.geom.Point2D.Double(this.ordinates[0], this.ordinates[1]);
        } else {
            double[] var1 = this.getMBR();
            return var1 != null ? new java.awt.geom.Point2D.Double(var1[0], var1[1]) : null;
        }
    }

    public Point2D[] getJavaPoints() {
        int var1 = this.elemInfo[1] % 10;
        int var2 = this.elemInfo[2];
        int var3;
        if (this.isOrientedMultiPoint()) {
            var2 = this.elemInfo.length / 6;
        } else if (this.isMultiPoint()) {
            var2 = 0;

            for (var3 = 0; var3 < this.elemInfo.length; var3 += 3) {
                var2 += this.elemInfo[var3 + 2];
            }
        }

        var3 = var2;
        if (this.gtype == 5 && var1 == 1) {
            Point2D[] var4 = new Point2D[var2];
            int var5 = 0;
            if (this.isOrientedMultiPoint()) {
                var5 = this.getOrientMultiPointOffset();
            }

            for (int var6 = 0; var6 < var3; ++var6) {
                var4[var6] = new java.awt.geom.Point2D.Double(this.ordinates[var6 * (this.dim + var5)], this.ordinates[var6 * (this.dim + var5) + 1]);
            }

            return var4;
        } else {
            return null;
        }
    }

    public final boolean isPoint() {
        return this.gtype == 1;
    }

    public final boolean isOrientedPoint() {
        if (this.elemInfo != null && this.ordinates != null) {
            return this.isPoint() && this.elemInfo.length == 6 && this.elemInfo[4] == 1 && this.elemInfo[5] == 0;
        } else {
            return false;
        }
    }

    public final boolean isMultiPoint() {
        return this.gtype == 5;
    }

    public final boolean isOrientedMultiPoint() {
        if (this.elemInfo != null && this.ordinates != null) {
            return this.isMultiPoint() && this.elemInfo.length > 3 && this.elemInfo[4] == 1 && this.elemInfo[5] == 0;
        } else {
            return false;
        }
    }

    public final boolean isRectangle() {
        if (this.elemInfo == null) {
            return false;
        } else {
            return this.gtype == 3 && this.elemInfo[1] % 100 == 3 && this.elemInfo[2] == 3 && this.elemInfo.length == 3;
        }
    }

    public final boolean isCircle() {
        if (this.elemInfo == null) {
            return false;
        } else {
            return this.gtype == 3 && this.elemInfo[1] % 100 == 3 && this.elemInfo[2] == 4 && this.elemInfo.length == 3;
        }
    }

    public final boolean isGeodeticMBR() {
        if (this.elemInfo == null) {
            return false;
        } else {
            return this.gtype == 3 && this.elemInfo[1] % 100 == 3 && this.elemInfo[2] == 5 && this.elemInfo.length == 3;
        }
    }

    public final boolean isLRSGeometry() {
        return this.dim > 2 && this.linfo != 0 ? (this.dim == 3 ? this.linfo == 3 : (this.dim == 4 ? this.linfo == 3 || this.linfo == 4 : false)) : false;
    }

    public final boolean hasCircularArcs() {
        if (this.elemInfo != null) {
            for (int var1 = 1; var1 < this.elemInfo.length; var1 += 3) {
                int var2 = this.elemInfo[var1];
                int var3 = this.elemInfo[var1 + 1];
                if (var3 == 2 && (var2 == 2 || var2 == 2003 || var2 == 3 || var2 == 1003) || var2 == 4 || var2 == 5 || var2 == 2005 || var2 == 1005) {
                    return true;
                }
            }
        }

        return false;
    }

    public int getDimensions() {
        return this.dim;
    }

    public double[] getOrdinatesArray() {
        return this.ordinates;
    }

    public int[] getElemInfo() {
        return this.elemInfo;
    }

    public final int getNumPoints() {
        if (this.gtype == 1) {
            return 1;
        } else if (this.isOrientedMultiPoint()) {
            int var1 = this.getOrientMultiPointOffset();
            return this.ordinates.length / (this.dim + var1);
        } else {
            return this.ordinates.length / this.dim;
        }
    }

    public double[] getFirstPoint() {
        double[] var1 = new double[this.dim];
        if (this.gtype == 1 && !Double.isNaN(this.x) && !Double.isNaN(this.y)) {
            var1[0] = this.x;
            var1[1] = this.y;
            if (this.dim > 2) {
                var1[2] = this.z;
            }

            return var1;
        } else {
            for (int var2 = 0; var2 < this.dim; ++var2) {
                var1[var2] = this.ordinates[var2];
            }

            return var1;
        }
    }

    public double[] getLastPoint() {
        double[] var1 = new double[this.dim];
        if (this.gtype == 1 && !Double.isNaN(this.x) && !Double.isNaN(this.y)) {
            var1[0] = this.x;
            var1[1] = this.y;
            if (this.dim > 2) {
                var1[2] = this.z;
            }

            return var1;
        } else {
            int var2;
            if (this.isOrientedPoint()) {
                for (var2 = 0; var2 < this.dim; ++var2) {
                    var1[var2] = this.ordinates[var2];
                }

                return var1;
            } else {
                var2 = 0;
                if (this.isOrientedMultiPoint()) {
                    var2 = this.getOrientMultiPointOffset();
                }

                int var3 = this.ordinates.length - this.dim - var2;

                for (int var4 = 0; var4 < this.dim; ++var4) {
                    var1[var4] = this.ordinates[var3 + var4];
                }

                return var1;
            }
        }
    }

    public double[] getMBR() {
        if (this.mbr != null) {
            return this.mbr;
        } else if (this.getDimensions() == 3 && this.linfo == 0) {
            this.mbr = new double[6];
            if (this.isOptimizedPoint()) {
                this.mbr[0] = this.x;
                this.mbr[1] = this.y;
                this.mbr[2] = this.z;
                this.mbr[3] = this.x;
                this.mbr[4] = this.y;
                this.mbr[5] = this.z;
                return this.mbr;
            } else if ((!this.isPoint() || this.elemInfo.length != 3) && !this.isOrientedPoint()) {
                this.mbr[0] = 1.0D / 0.0;
                this.mbr[1] = 1.0D / 0.0;
                this.mbr[2] = 1.0D / 0.0;
                this.mbr[3] = -1.0D / 0.0;
                this.mbr[4] = -1.0D / 0.0;
                this.mbr[5] = -1.0D / 0.0;
                int var10;
                int var12;
                if (this.isOrientedMultiPoint()) {
                    var10 = this.ordinates.length / (2 * this.dim);
                    var12 = this.getOrientMultiPointOffset();

                    for (int var6 = 0; var6 < var10; ++var6) {
                        expandMBR(this.mbr, this.ordinates[var6 * (this.dim + var12)], this.ordinates[var6 * (this.dim + var12) + 1], this.ordinates[var6 * (this.dim + var12) + 2]);
                    }

                    return this.mbr;
                } else {
                    int[] var9 = new int[this.elemInfo.length];
                    double[] var11 = new double[this.ordinates.length];

                    for (var10 = 0; var10 < this.ordinates.length; ++var10) {
                        var11[var10] = this.ordinates[var10];
                    }

                    for (var10 = 0; var10 < this.elemInfo.length; ++var10) {
                        var9[var10] = this.elemInfo[var10];
                    }

                    J3D_Geometry var8 = new J3D_Geometry(this.gtype, this.srid, var9, var11);
                    double[][] var13 = new double[3][2];
                    var13 = J3D_Geometry.getMBH(var8);

                    for (var12 = 0; var12 < 3; ++var12) {
                        this.mbr[var12] = var13[var12][0];
                        this.mbr[var12 + 3] = var13[var12][1];
                    }

                    return this.mbr;
                }
            } else {
                this.mbr[0] = this.ordinates[0];
                this.mbr[1] = this.ordinates[1];
                this.mbr[2] = this.ordinates[2];
                this.mbr[3] = this.ordinates[0];
                this.mbr[4] = this.ordinates[1];
                this.mbr[5] = this.ordinates[2];
                return this.mbr;
            }
        } else {
            this.mbr = new double[4];
            if (this.isOptimizedPoint()) {
                this.mbr[0] = this.x;
                this.mbr[1] = this.y;
                this.mbr[2] = this.x;
                this.mbr[3] = this.y;
                return this.mbr;
            } else if ((!this.isPoint() || this.elemInfo.length != 3) && !this.isOrientedPoint()) {
                if (!this.isRectangle() && !this.isGeodeticMBR()) {
                    this.mbr[0] = 1.0D / 0.0;
                    this.mbr[1] = 1.0D / 0.0;
                    this.mbr[2] = -1.0D / 0.0;
                    this.mbr[3] = -1.0D / 0.0;
                    int var2;
                    int var7;
                    if (this.isMultiPoint() && this.elemInfo.length == 3) {
                        var7 = this.elemInfo[2];

                        for (var2 = 0; var2 < var7; ++var2) {
                            expandMBR(this.mbr, this.ordinates[var2 * this.dim], this.ordinates[var2 * this.dim + 1]);
                        }

                        return this.mbr;
                    } else {
                        int var3;
                        if (this.isOrientedMultiPoint()) {
                            var7 = this.ordinates.length / (2 * this.dim);
                            var2 = this.getOrientMultiPointOffset();

                            for (var3 = 0; var3 < var7; ++var3) {
                                expandMBR(this.mbr, this.ordinates[var3 * (this.dim + var2)], this.ordinates[var3 * (this.dim + var2) + 1]);
                            }

                            return this.mbr;
                        } else {
                            JGeometry.ElementIterator var1 = new JGeometry.ElementIterator(this);

                            while (var1.next()) {
                                var2 = var1.ord_offset;

                                for (var3 = 0; var3 < var1.nCoord; ++var3) {
                                    double[] var4;
                                    if (var1.eitpr == 2) {
                                        if (var2 + var3 * this.dim >= var1.next_ord_offset || var1.lastElem && var2 + var3 * this.dim >= var1.next_ord_offset - this.dim || var3 >= var1.nCoord - 1) {
                                            break;
                                        }

                                        var4 = computeArcMBR(this.ordinates[var2 + var3 * this.dim], this.ordinates[var2 + var3 * this.dim + 1], this.ordinates[var2
                                                + (var3 + 1) * this.dim], this.ordinates[var2 + (var3 + 1) * this.dim
                                                        + 1], this.ordinates[var2 + (var3 + 2) * this.dim], this.ordinates[var2 + (var3 + 2) * this.dim + 1]);
                                        this.expandMBR(this.mbr, var4, 2);
                                        ++var3;
                                    } else if (var1.eitpr == 4) {
                                        if (var2 + var3 * this.dim >= var1.next_ord_offset || var1.lastElem && var2 + var3 * this.dim >= var1.next_ord_offset - this.dim) {
                                            break;
                                        }

                                        var4 = computeArc(this.ordinates[var2 + var3 * this.dim], this.ordinates[var2 + var3 * this.dim + 1], this.ordinates[var2
                                                + (var3 + 1) * this.dim], this.ordinates[var2 + (var3 + 1) * this.dim
                                                        + 1], this.ordinates[var2 + (var3 + 2) * this.dim], this.ordinates[var2 + (var3 + 2) * this.dim + 1]);
                                        double[] var5 = new double[]{var4[0] - var4[2], var4[1] - var4[2], var4[0] + var4[2], var4[1] + var4[2]};
                                        this.expandMBR(this.mbr, var5, 2);
                                        var3 += 2;
                                    } else {
                                        expandMBR(this.mbr, this.ordinates[var2 + var3 * this.dim], this.ordinates[var2 + var3 * this.dim + 1]);
                                    }
                                }
                            }

                            return this.mbr;
                        }
                    }
                } else {
                    this.mbr[0] = Math.min(this.ordinates[0], this.ordinates[2]);
                    this.mbr[1] = Math.min(this.ordinates[1], this.ordinates[3]);
                    this.mbr[2] = Math.max(this.ordinates[0], this.ordinates[2]);
                    this.mbr[3] = Math.max(this.ordinates[1], this.ordinates[3]);
                    return this.mbr;
                }
            } else {
                this.mbr[0] = this.ordinates[0];
                this.mbr[1] = this.ordinates[1];
                this.mbr[2] = this.ordinates[0];
                this.mbr[3] = this.ordinates[1];
                return this.mbr;
            }
        }
    }

    public Object[] getOrdinatesOfElements() {
        ArrayList var1 = new ArrayList();
        int var2 = 0;
        if (this.isOrientedMultiPoint()) {
            var2 = this.getOrientMultiPointOffset();
        }

        JGeometry.ElementIterator var3 = new JGeometry.ElementIterator(this);

        while (var3.next()) {
            int var4 = var3.ord_offset;
            double[] var5 = new double[var3.nCoord * this.dim];

            for (int var6 = 0; var6 < var3.nCoord; ++var6) {
                for (int var7 = 0; var7 < this.dim; ++var7) {
                    var5[var6 * this.dim + var7] = this.ordinates[var4 + var6 * (this.dim + var2) + var7];
                }
            }

            var1.add(var5);
        }

        return var1.toArray();
    }

    protected double[] getOrdinatesOfElement(int var1, int var2) {
        double[] var3 = new double[var2 - var1];
        int var4 = 0;

        for (int var5 = var1; var5 < var2; ++var5) {
            var3[var4] = this.ordinates[var5];
            ++var4;
        }

        return var3;
    }

    protected int[] getElemInfoOfElement(int var1, int var2) {
        int[] var3 = new int[var2 - var1];
        int var4 = 0;
        int var5 = this.elemInfo[var1] - 1;

        for (int var6 = var1; var6 < var2; var6 += 3) {
            var3[var4] = this.elemInfo[var6] - var5;
            var3[var4 + 1] = this.elemInfo[var6 + 1];
            var3[var4 + 2] = this.elemInfo[var6 + 2];
            var4 += 3;
        }

        return var3;
    }

    protected JGeometry makeElementGeometry(int var1, int var2, int var3, int var4, int var5) {
        int[] var6 = new int[var3 - var2];
        var6 = this.getElemInfoOfElement(var2, var3);
        double[] var7 = new double[var5 - var4];
        var7 = this.getOrdinatesOfElement(var4, var5);
        JGeometry var8 = new JGeometry(var1, this.srid, var6, var7);
        return var8;
    }

    public JGeometry getElementAt(int var1) {
        if (var1 >= 1) {
            JGeometry[] var2 = this.getElements(var1);
            return var2 != null ? var2[0] : null;
        } else {
            return null;
        }
    }

    public JGeometry[] getElements() {
        JGeometry[] var1 = this.getElements(-1);
        return var1;
    }

    protected JGeometry[] getElements(int var1) {
        ArrayList var2 = new ArrayList();
        JGeometry var3 = null;
        int var4 = 0;
        if (this.gtype != 1 && this.gtype != 2 && this.gtype != 3) {
            if (this.gtype == 6 || this.gtype == 7 || this.gtype == 4 || this.gtype == 5) {
                if (this.elemInfo == null || this.ordinates == null) {
                    return null;
                }

                boolean var5 = false;
                boolean var6 = false;
                boolean var7 = false;
                int var8 = 0;

                for (int var9 = 0; var9 < this.elemInfo.length; var9 += 3) {
                    byte var10;
                    int var17;
                    if (this.elemInfo[var9 + 1] == 2) {
                        ++var4;
                        if (var4 == var1 || var1 == -1) {
                            var10 = 2;
                            var17 = this.elemInfo[var9] - 1;
                            if (var9 + 3 < this.elemInfo.length) {
                                var8 = this.elemInfo[var9 + 3] - 1;
                            } else {
                                var8 = this.ordinates.length;
                            }

                            var3 = this.makeElementGeometry(var10, var9, var9 + 3, var17, var8);
                            var3.dim = this.dim;
                            var3.linfo = this.linfo;
                            var2.add(var3);
                        }

                        if (var4 == var1) {
                            var9 = this.elemInfo.length;
                        }
                    } else {
                        int var18;
                        int var19;
                        byte var20;
                        if (this.elemInfo[var9 + 1] == 4) {
                            ++var4;
                            var19 = this.elemInfo[var9 + 2];
                            if (var4 == var1 || var1 == -1) {
                                var20 = 2;
                                var17 = this.elemInfo[var9] - 1;
                                if (var9 + 3 + 3 * var19 < this.elemInfo.length) {
                                    var8 = this.elemInfo[var9 + 3 + 3 * var19] - 1;
                                    var18 = var9 + 3 + 3 * var19;
                                } else {
                                    var8 = this.ordinates.length;
                                    var18 = this.elemInfo.length;
                                }

                                var3 = this.makeElementGeometry(var20, var9, var18, var17, var8);
                                var3.dim = this.dim;
                                var3.linfo = this.linfo;
                                var2.add(var3);
                            }

                            if (var4 == var1) {
                                var9 = this.elemInfo.length;
                            } else {
                                var9 += 3 * var19;
                            }
                        } else {
                            boolean var12;
                            int var13;
                            if (this.elemInfo[var9 + 1] != 1003 && this.elemInfo[var9 + 1] != 1005 && this.elemInfo[var9 + 1] != 3 && this.elemInfo[var9 + 1] != 5) {
                                if (this.elemInfo[var9 + 1] == 1) {
                                    var19 = this.elemInfo[var9 + 2];
                                    var20 = 1;
                                    var17 = this.elemInfo[var9] - 1;
                                    var12 = false;
                                    if (var19 < 1) {
                                        return null;
                                    }

                                    if (var9 + 3 < this.elemInfo.length) {
                                        if (var19 == 1 && this.elemInfo[var9 + 4] == 1 && this.elemInfo[var9 + 5] == 0) {
                                            var12 = true;
                                        }

                                        if (var12 && var9 + 6 < this.elemInfo.length) {
                                            var8 = this.elemInfo[var9 + 6] - 1;
                                        } else if (var12) {
                                            var8 = this.ordinates.length;
                                        }
                                    }

                                    for (var13 = 0; var13 < var19; ++var13) {
                                        ++var4;
                                        if (this.dim == 2 && !var12) {
                                            if (var4 == var1 || var1 == -1) {
                                                var3 = new JGeometry(this.ordinates[var17], this.ordinates[var17 + 1], this.srid);
                                                var2.add(var3);
                                            }

                                            if (var4 == var1) {
                                                var13 = var19;
                                                var9 = this.elemInfo.length;
                                            } else {
                                                var17 += 2;
                                            }
                                        } else if (this.dim == 3 && !var12) {
                                            if (var4 == var1 || var1 == -1) {
                                                var3 = new JGeometry(this.ordinates[var17], this.ordinates[var17 + 1], this.ordinates[var17 + 2], this.srid);
                                                var2.add(var3);
                                            }

                                            if (var4 == var1) {
                                                var13 = var19;
                                                var9 = this.elemInfo.length;
                                            } else {
                                                var17 += 3;
                                            }
                                        } else if (!var12) {
                                            if (var4 == var1 || var1 == -1) {
                                                int[] var22 = new int[]{1, 1, 1};
                                                double[] var23 = new double[this.dim];
                                                var23 = this.getOrdinatesOfElement(var17, var17 + this.dim);
                                                var3 = new JGeometry(var20, this.srid, var22, var23);
                                                var3.dim = this.dim;
                                                var2.add(var3);
                                            }

                                            if (var4 == var1) {
                                                var13 = var19;
                                                var9 = this.elemInfo.length;
                                            } else {
                                                var17 += this.dim;
                                            }
                                        } else if (var12) {
                                            if (var4 == var1 || var1 == -1) {
                                                var3 = this.makeElementGeometry(var20, var9, var9 + 6, var17, var8);
                                                var3.dim = this.dim;
                                                var2.add(var3);
                                            }

                                            if (var4 == var1) {
                                                var13 = var19;
                                                var9 = this.elemInfo.length;
                                            } else {
                                                var9 += 3;
                                            }
                                        }
                                    }
                                } else {
                                    var9 += 3;
                                }
                            } else {
                                ++var4;
                                var10 = 3;
                                int var11 = this.elemInfo[var9 + 1];
                                var17 = this.elemInfo[var9] - 1;
                                var12 = false;
                                if ((var11 == 1003 || var11 == 3) && var9 + 3 < this.elemInfo.length) {
                                    var8 = this.elemInfo[var9 + 3] - 1;
                                    var18 = var9 + 3;
                                    var12 = true;
                                } else if ((var11 == 1005 || var11 == 5) && var9 + 3 + 3 * this.elemInfo[var9 + 2] < this.elemInfo.length) {
                                    var13 = this.elemInfo[var9 + 2];
                                    var8 = this.elemInfo[var9 + 3 + 3 * var13] - 1;
                                    var18 = var9 + 3 + 3 * var13;
                                    var9 += 3 * var13;
                                    var12 = true;
                                } else {
                                    var8 = this.ordinates.length;
                                    var18 = this.elemInfo.length;
                                }

                                boolean var21 = true;

                                while (var12 && var21) {
                                    if (var9 + 3 + 1 < this.elemInfo.length) {
                                        int var14 = this.elemInfo[var9 + 3 + 1];
                                        if (var14 == 2003) {
                                            var9 += 3;
                                            if (var9 + 3 + 1 > this.elemInfo.length) {
                                                var8 = this.ordinates.length;
                                                var18 = this.elemInfo.length;
                                                var21 = false;
                                            } else {
                                                var8 = this.elemInfo[var9 + 3] - 1;
                                                var18 = var9 + 3;
                                                var21 = true;
                                            }
                                        } else if (var14 == 2005) {
                                            var9 += 3;
                                            int var15 = this.elemInfo[var9 + 2];
                                            if (var9 + 3 + 3 * var15 < this.elemInfo.length) {
                                                var8 = this.elemInfo[var9 + 3 + 3 * var15] - 1;
                                                var18 = var9 + 3 + 3 * var15;
                                                var21 = true;
                                            } else {
                                                var8 = this.ordinates.length;
                                                var18 = this.elemInfo.length;
                                                var21 = false;
                                            }

                                            var9 += 3 * var15;
                                        } else {
                                            var21 = false;
                                        }
                                    } else {
                                        var12 = false;
                                    }
                                }

                                if (var4 == var1 || var1 == -1) {
                                    var3 = this.makeElementGeometry(var10, var9, var18, var17, var8);
                                    var3.dim = this.dim;
                                    var2.add(var3);
                                }

                                if (var4 == var1) {
                                    var9 = this.elemInfo.length;
                                }
                            }
                        }
                    }
                }
            }
        } else {
            var2.add(this);
            ++var4;
        }

        if (var1 <= var4 && var4 != 0) {
            JGeometry[] var16 = new JGeometry[var2.size()];
            return (JGeometry[]) ((JGeometry[]) var2.toArray(var16));
        } else {
            return null;
        }
    }

    public int getOrientMultiPointOffset() {
        byte var1 = 0;
        if (this.isOrientedMultiPoint()) {
            if (this.dim != 2 && (this.dim != 3 || this.linfo != 3)) {
                var1 = 3;
            } else {
                var1 = 2;
            }
        }

        return var1;
    }

    public final Shape createShape() {
        GeneralPath var1 = null;
        if (!this.isRectangle() && !this.isGeodeticMBR()) {
            if (this.isCircle()) {
                double[] var12 = computeArc(this.ordinates[0], this.ordinates[1], this.ordinates[this.dim], this.ordinates[this.dim + 1], this.ordinates[2 * this.dim], this.ordinates[2 * this.dim
                        + 1]);
                java.awt.geom.Ellipse2D.Double var13 = new java.awt.geom.Ellipse2D.Double(var12[0] - var12[2], var12[1] - var12[2], 2.0D * var12[2], 2.0D * var12[2]);
                return var13;
            } else {
                int var10 = 0;
                if (this.isOrientedMultiPoint()) {
                    var10 = this.getOrientMultiPointOffset();
                }

                JGeometry.ElementIterator var11 = new JGeometry.ElementIterator(this);

                while (var11.next()) {
                    if (var1 == null) {
                        var1 = new GeneralPath();
                    }

                    int var14 = var11.ord_offset;

                    for (int var15 = 0; var15 < var11.nCoord; ++var15) {
                        if (var11.etype != 1 && var11.eitpr == 1) {
                            if (var15 != 0 || var11.isCompound && !var11.isFirstElemOfCompound) {
                                var1.lineTo((float) this.ordinates[var14 + var15 * (this.dim + var10)], (float) this.ordinates[var14 + var15 * (this.dim + var10) + 1]);
                            } else {
                                var1.moveTo((float) this.ordinates[var14 + var15 * (this.dim + var10)], (float) this.ordinates[var14 + var15 * (this.dim + var10) + 1]);
                            }

                            if (var15 >= var11.nCoord - 1 && var11.top_etype == 3) {
                                var1.closePath();
                            }
                        } else {
                            double[] var16;
                            if (var11.eitpr == 2) {
                                if (var11.etype == 1) {
                                    return null;
                                }

                                if (var14 + var15 * this.dim >= var11.next_ord_offset || var11.lastElem && var14 + var15 * this.dim >= var11.next_ord_offset - this.dim
                                        || (var11.next_ord_offset - (var14 + var15 * this.dim)) / this.dim < 2) {
                                    break;
                                }

                                var16 = new double[]{this.ordinates[var14 + var15 * this.dim], this.ordinates[var14 + var15 * this.dim + 1], this.ordinates[var14 + (var15 + 1) * this.dim],
                                        this.ordinates[var14 + (var15 + 1) * this.dim + 1], this.ordinates[var14 + (var15 + 2) * this.dim], this.ordinates[var14 + (var15 + 2) * this.dim + 1]};
                                double[] var18 = reFormulateArc(var16);
                                if (var15 != 0 || var11.isCompound && !var11.isFirstElemOfCompound) {
                                    var1.lineTo((float) this.ordinates[var14 + var15 * this.dim], (float) this.ordinates[var14 + var15 * this.dim + 1]);
                                } else {
                                    var1.moveTo((float) this.ordinates[var14 + var15 * this.dim], (float) this.ordinates[var14 + var15 * this.dim + 1]);
                                }

                                java.awt.geom.Arc2D.Double var19 = new java.awt.geom.Arc2D.Double(var18[0] - var18[2], var18[1] - var18[2], var18[2] * 2.0D, var18[2] * 2.0D,
                                        var18[3] / 3.141592653589793D * 180.0D, (var18[5] - var18[3]) / 3.141592653589793D * 180.0D, 0);
                                var1.append(var19, true);
                                ++var15;
                                if (var15 >= var11.nCoord - 1 && (var11.top_etype == 3 || var11.top_etype == 5)) {
                                    var1.closePath();
                                }
                            } else if (var11.eitpr == 3) {
                                float var6 = (float) this.ordinates[var14 + 0];
                                float var7 = (float) this.ordinates[var14 + 1];
                                float var8 = (float) this.ordinates[var14 + this.dim];
                                float var9 = (float) this.ordinates[var14 + this.dim + 1];
                                if (var11.original_etype < 2000) {
                                    var1.moveTo(var6, var7);
                                    var1.lineTo(var8, var7);
                                    var1.lineTo(var8, var9);
                                    var1.lineTo(var6, var9);
                                    var1.closePath();
                                } else {
                                    var1.moveTo(var6, var7);
                                    var1.lineTo(var6, var9);
                                    var1.lineTo(var8, var9);
                                    var1.lineTo(var8, var7);
                                    var1.closePath();
                                }

                                ++var15;
                            } else if (var11.eitpr == 4) {
                                if (var14 + var15 * this.dim >= var11.next_ord_offset || var11.lastElem && var14 + var15 * this.dim >= var11.next_ord_offset - this.dim) {
                                    break;
                                }

                                var16 = computeArc(this.ordinates[var14 + var15 * this.dim], this.ordinates[var14 + var15 * this.dim + 1], this.ordinates[var14
                                        + (var15 + 1) * this.dim], this.ordinates[var14 + (var15 + 1) * this.dim
                                                + 1], this.ordinates[var14 + (var15 + 2) * this.dim], this.ordinates[var14 + (var15 + 2) * this.dim + 1]);
                                java.awt.geom.Ellipse2D.Double var17 = new java.awt.geom.Ellipse2D.Double(var16[0] - var16[2], var16[1] - var16[2], 2.0D * var16[2], 2.0D * var16[2]);
                                var1.append(var17, false);
                                if (var11.original_etype > 2000) {
                                    var1.setWindingRule(0);
                                }

                                var15 += 2;
                            }
                        }
                    }
                }

                return var1;
            }
        } else {
            float var2 = (float) this.ordinates[0];
            float var3 = (float) this.ordinates[1];
            float var4 = (float) this.ordinates[2];
            float var5 = (float) this.ordinates[3];
            return new java.awt.geom.Rectangle2D.Double((double) var2, (double) Math.min(var3, var5), (double) (var4 - var2), (double) Math.abs(var5 - var3));
        }
    }

    public final Shape createShape(AffineTransform var1) {
        return this.createShape(var1, false);
    }

    public final Shape createShape(AffineTransform var1, boolean var2) {
        if (var1 == null) {
            return this.createShape();
        } else {
            java.awt.geom.Point2D.Double var3 = new java.awt.geom.Point2D.Double();
            java.awt.geom.Point2D.Double var4 = new java.awt.geom.Point2D.Double();
            GeneralPath var5 = null;
            if (!this.isRectangle() && !this.isGeodeticMBR()) {
                double[] var29;
                if (this.isCircle()) {
                    var3.setLocation(this.ordinates[0], this.ordinates[1]);
                    var1.transform(var3, var4);
                    double var24 = var4.getX();
                    double var26 = var4.getY();
                    var3.setLocation(this.ordinates[this.dim], this.ordinates[this.dim + 1]);
                    var1.transform(var3, var4);
                    double var10 = var4.getX();
                    double var28 = var4.getY();
                    var3.setLocation(this.ordinates[2 * this.dim], this.ordinates[2 * this.dim + 1]);
                    var1.transform(var3, var4);
                    double var14 = var4.getX();
                    double var16 = var4.getY();
                    var29 = computeArc(var24, var26, var10, var28, var14, var16);
                    java.awt.geom.Ellipse2D.Double var32 = new java.awt.geom.Ellipse2D.Double(var29[0] - var29[2], var29[1] - var29[2], 2.0D * var29[2], 2.0D * var29[2]);
                    return var32;
                } else {
                    int var23 = 0;
                    if (this.isOrientedMultiPoint()) {
                        var23 = this.getOrientMultiPointOffset();
                    }

                    double var25 = 0.0D;
                    double var27 = 0.0D;
                    JGeometry.ElementIterator var11 = new JGeometry.ElementIterator(this);

                    while (var11.next()) {
                        if (var5 == null) {
                            var5 = new GeneralPath();
                        }

                        int var12 = var11.ord_offset;
                        double var13 = 1.7976931348623157E308D;
                        double var15 = 1.7976931348623157E308D;

                        for (int var17 = 0; var17 < var11.nCoord; ++var17) {
                            if (var11.etype != 1 && var11.eitpr == 1) {
                                var25 = this.ordinates[var12 + var17 * (this.dim + var23)];
                                var27 = this.ordinates[var12 + var17 * (this.dim + var23) + 1];
                                var3.setLocation(var25, var27);
                                var1.transform(var3, var4);
                                var25 = var4.getX();
                                var27 = var4.getY();
                                if (var17 != 0 || var11.isCompound && !var11.isFirstElemOfCompound) {
                                    double var30 = var25 - var13;
                                    double var35 = var27 - var15;
                                    boolean var22 = var30 < -0.25D || var30 > 0.25D || var35 < -0.25D || var35 > 0.25D;
                                    if (!var2 || var22) {
                                        var13 = var25;
                                        var15 = var27;
                                        var5.lineTo((float) var25, (float) var27);
                                    }
                                } else {
                                    var13 = var25;
                                    var15 = var27;
                                    var5.moveTo((float) var25, (float) var27);
                                }

                                if (var17 >= var11.nCoord - 1 && var11.top_etype == 3) {
                                    var5.closePath();
                                }
                            } else {
                                double[] var31;
                                if (var11.eitpr == 2) {
                                    if (var11.etype == 1) {
                                        return null;
                                    }

                                    if (var12 + var17 * this.dim >= var11.next_ord_offset || var11.lastElem && var12 + var17 * this.dim >= var11.next_ord_offset - this.dim
                                            || (var11.next_ord_offset - (var12 + var17 * this.dim)) / this.dim < 2) {
                                        break;
                                    }

                                    var29 = new double[]{this.ordinates[var12 + var17 * this.dim], this.ordinates[var12 + var17 * this.dim + 1], this.ordinates[var12 + (var17 + 1) * this.dim],
                                            this.ordinates[var12 + (var17 + 1) * this.dim + 1], this.ordinates[var12 + (var17 + 2) * this.dim], this.ordinates[var12 + (var17 + 2) * this.dim + 1]};
                                    var1.transform(var29, 0, var29, 0, 3);
                                    var31 = reFormulateArc(var29);
                                    if (var17 == 0 && (!var11.isCompound || var11.isFirstElemOfCompound)) {
                                        var5.moveTo((float) var29[0], (float) var29[1]);
                                    } else {
                                        var5.lineTo((float) var29[0], (float) var29[1]);
                                    }

                                    java.awt.geom.Arc2D.Double var34 = new java.awt.geom.Arc2D.Double(var31[0] - var31[2], var31[1] - var31[2], var31[2] * 2.0D, var31[2] * 2.0D,
                                            var31[3] / 3.141592653589793D * 180.0D, (var31[5] - var31[3]) / 3.141592653589793D * 180.0D, 0);
                                    var5.append(var34, true);
                                    ++var17;
                                    if (var17 >= var11.nCoord - 1 && (var11.top_etype == 3 || var11.top_etype == 5)) {
                                        var5.closePath();
                                    }
                                } else if (var11.eitpr == 3) {
                                    var3.setLocation(this.ordinates[var12 + 0], this.ordinates[var12 + 1]);
                                    var1.transform(var3, var4);
                                    float var18 = (float) var4.getX();
                                    float var19 = (float) var4.getY();
                                    var3.setLocation(this.ordinates[var12 + this.dim], this.ordinates[var12 + this.dim + 1]);
                                    var1.transform(var3, var4);
                                    float var20 = (float) var4.getX();
                                    float var21 = (float) var4.getY();
                                    if (var11.original_etype < 2000) {
                                        var5.moveTo(var18, var19);
                                        var5.lineTo(var20, var19);
                                        var5.lineTo(var20, var21);
                                        var5.lineTo(var18, var21);
                                        var5.closePath();
                                    } else {
                                        var5.moveTo(var18, var19);
                                        var5.lineTo(var18, var21);
                                        var5.lineTo(var20, var21);
                                        var5.lineTo(var20, var19);
                                        var5.closePath();
                                    }

                                    ++var17;
                                } else if (var11.eitpr == 4) {
                                    if (var12 + var17 * this.dim >= var11.next_ord_offset || var11.lastElem && var12 + var17 * this.dim >= var11.next_ord_offset - this.dim) {
                                        break;
                                    }

                                    var29 = new double[]{this.ordinates[var12 + var17 * this.dim], this.ordinates[var12 + var17 * this.dim + 1], this.ordinates[var12 + (var17 + 1) * this.dim],
                                            this.ordinates[var12 + (var17 + 1) * this.dim + 1], this.ordinates[var12 + (var17 + 2) * this.dim], this.ordinates[var12 + (var17 + 2) * this.dim + 1]};
                                    var1.transform(var29, 0, var29, 0, 3);
                                    var31 = computeArc(var29[0], var29[1], var29[2], var29[3], var29[4], var29[5]);
                                    java.awt.geom.Ellipse2D.Double var33 = new java.awt.geom.Ellipse2D.Double(var31[0] - var31[2], var31[1] - var31[2], 2.0D * var31[2], 2.0D * var31[2]);
                                    var5.append(var33, false);
                                    if (var11.original_etype > 2000) {
                                        var5.setWindingRule(0);
                                    }

                                    var17 += 2;
                                }
                            }
                        }
                    }

                    return var5;
                }
            } else {
                var3.setLocation(this.ordinates[0], this.ordinates[1]);
                var1.transform(var3, var4);
                float var6 = (float) var4.getX();
                float var7 = (float) var4.getY();
                var3.setLocation(this.ordinates[2], this.ordinates[3]);
                var1.transform(var3, var4);
                float var8 = (float) var4.getX();
                float var9 = (float) var4.getY();
                return new java.awt.geom.Rectangle2D.Double((double) var6, (double) Math.min(var7, var9), (double) (var8 - var6), (double) Math.abs(var9 - var7));
            }
        }
    }

    public static final JGeometry load(STRUCT var0) throws SQLException {
        Datum[] var1 = var0.getOracleAttributes();
        int var2 = var1[0] != null ? var1[0].intValue() : 0;
        int var3 = var1[1] != null ? var1[1].intValue() : 0;
        STRUCT var4 = (STRUCT) var1[2];
        double var5 = 0.0D / 0.0;
        double var7 = 0.0D / 0.0;
        double var9 = 0.0D / 0.0;
        if (var4 != null) {
            Datum[] var11 = (Datum[]) var4.getOracleAttributes();
            if (var11[0] != null && var11[1] != null) {
                var5 = var11[0].doubleValue();
                var7 = var11[1].doubleValue();
            }

            if (var11[2] != null) {
                var9 = var11[2].doubleValue();
            }
        }

        int[] var18 = var1[3] != null ? ((ARRAY) var1[3]).getIntArray() : null;
        double[] var12 = null;
        if (var1[4] != null) {
            if (var2 % 1000 / 100 == 0) {
                var12 = ((ARRAY) var1[4]).getDoubleArray();
            } else {
                int var13 = var2 % 1000 / 100;
                int var14 = var2 / 1000 > 0 ? var2 / 1000 : 2;
                Datum[] var15 = ((ARRAY) var1[4]).getOracleArray();
                int var16 = var15.length;
                var12 = new double[var16];
                if (var14 == 2 || (var14 != 3 || var14 != var13) && (var14 != 4 || var13 != 3 && var13 != 4)) {
                    throw new SQLException("An invalid sdo_gtype is found");
                }

                for (int var17 = 0; var17 < var16; ++var17) {
                    if (var15[var17] == null && var17 % var14 != var13 - 1) {
                        throw new SQLException("An invalid null value is found in LRS sdo_ordinates");
                    }

                    if (var15[var17] != null) {
                        var12[var17] = var15[var17].doubleValue();
                    } else {
                        var12[var17] = 0.0D / 0.0;
                    }
                }
            }
        }

        if (var12 != null && var18 != null) {
            return new JGeometry(var2, var3, var5, var7, var9, var18, var12);
        } else if (!Double.isNaN(var5) && !Double.isNaN(var7)) {
            if (!Double.isNaN(var9)) {
                return new JGeometry(var5, var7, var9, var3);
            } else {
                return new JGeometry(var5, var7, var3);
            }
        } else {
            return null;
        }
    }

    private static boolean etype0_exists(int[] var0) {
        boolean var1 = false;

        for (int var2 = 0; var2 < var0.length / 3; ++var2) {
            if (var0[3 * var2 + 1] == 0) {
                var1 = true;
                break;
            }
        }

        return var1;
    }

    private static boolean ordOffset0_exists(int[] var0) {
        boolean var1 = false;

        for (int var2 = 0; var2 < var0.length / 3; ++var2) {
            if (var0[3 * var2] < 1) {
                var1 = true;
                break;
            }
        }

        return var1;
    }

    protected static void remove_etype0(int[] var0, double[] var1, ArrayList var2, ArrayList var3) {
        int[] var4 = new int[var0.length];
        double[] var5 = new double[var1.length];
        boolean var6 = false;
        boolean var7 = false;
        int var8 = 0;
        int var9 = 0;
        int var10 = 0;
        boolean var11 = false;
        boolean var12 = false;

        for (int var14 = 0; var14 < var0.length / 3; ++var14) {
            int var16;
            int var17;
            if (var0[3 * var14 + 1] == 0) {
                var16 = var0[3 * var14] - 1;
                if (3 * (var14 + 1) <= var0.length - 1) {
                    var17 = var0[3 * (var14 + 1)] - 2;
                } else {
                    var17 = var1.length - 1;
                }

                var10 += var17 - var16 + 1;
            } else {
                var16 = var0[3 * var14] - 1;
                if (3 * (var14 + 1) <= var0.length - 1) {
                    var17 = var0[3 * (var14 + 1)] - 2;
                } else {
                    var17 = var1.length - 1;
                }

                int var15;
                for (var15 = var16; var15 <= var17; ++var15) {
                    var5[var9] = var1[var15];
                    ++var9;
                }

                var4[var8] = var0[3 * var14] - var10;
                ++var8;

                for (var15 = 1; var15 < 3; ++var15) {
                    var4[var8] = var0[3 * var14 + var15];
                    ++var8;
                }
            }
        }

        int var13;
        for (var13 = 0; var13 < var8; ++var13) {
            var2.add(var4[var13]);
        }

        for (var13 = 0; var13 < var9; ++var13) {
            var3.add(var5[var13]);
        }

    }

    static final JGeometry loadAndReorient(STRUCT var0) throws SQLException {
        JGeometry var1 = load(var0);
        if (var1 == null) {
            return null;
        } else {
            if ((var1.gtype == 2 || var1.gtype == 6 || var1.gtype == 4) && !var1.hasCircularArcs()) {
                var1.reOrientCurves();
            }

            return var1;
        }
    }

    public static STRUCT store(JGeometry var0, Connection var1) throws SQLException {
        return store((JGeometry) var0, (Connection) var1, (Object[]) null);
    }

    public static STRUCT store(JGeometry var0, Connection var1, boolean var2) throws SQLException {
        return store(var0, var1, (Object[]) null, var2);
    }

    public static STRUCT store(JGeometry var0, Connection var1, Object[] var2) throws SQLException {
        return store(var0, var1, var2, false);
    }

    public static STRUCT store(JGeometry var0, Connection var1, Object[] var2, boolean var3) throws SQLException {
        StructDescriptor var4 = null;
        StructDescriptor var5 = null;
        ArrayDescriptor var6 = null;
        ArrayDescriptor var7 = null;
        if (var2 != null && var2.length >= 4) {
            var4 = (StructDescriptor) var2[0];
            var5 = (StructDescriptor) var2[1];
            var6 = (ArrayDescriptor) var2[2];
            var7 = (ArrayDescriptor) var2[3];
        } else {
            if (geomDesc == null) {
                createDBDescriptors(var1);
            }

            var4 = geomDesc;
            var5 = pointDesc;
            var6 = elemInfoDesc;
            var7 = ordinatesDesc;
        }

        if (var4 == null) {
            throw new SQLException("sdo_geometry descriptor is null.");
        } else if (var5 == null) {
            throw new SQLException("sdo_point descriptor is null.");
        } else if (var6 == null) {
            throw new SQLException("elem_info descriptor is null.");
        } else if (var7 == null) {
            throw new SQLException("ordinates descriptor is null.");
        } else {
            NUMBER var8 = new NUMBER(var0.gtype + var0.linfo * 100 + var0.dim * 1000);
            NUMBER var9 = var0.srid == 0 ? null : new NUMBER(var0.srid);
            STRUCT var10 = null;
            NUMBER[] var11;
            if (var3) {
                var11 = !Double.isNaN(var0.x) && !Double.isNaN(var0.y) ? new NUMBER[3] : null;
                if (var11 != null) {
                    var11[0] = new NUMBER(new BigDecimal(var0.x));
                    var11[1] = new NUMBER(new BigDecimal(var0.y));
                    var11[2] = Double.isNaN(var0.z) ? null : new NUMBER(new BigDecimal(var0.z));
                    var10 = new STRUCT(var5, var1, var11);
                }
            } else {
                var11 = !Double.isNaN(var0.x) && !Double.isNaN(var0.y) ? new NUMBER[3] : null;
                if (var11 != null) {
                    var11[0] = new NUMBER(var0.x);
                    var11[1] = new NUMBER(var0.y);
                    var11[2] = Double.isNaN(var0.z) ? null : new NUMBER(var0.z);
                    var10 = new STRUCT(var5, var1, var11);
                }
            }

            ARRAY var15 = var0.elemInfo == null ? null : new ARRAY(var6, var1, var0.elemInfo);
            ARRAY var12;
            int var14;
            if (var3) {
                BigDecimal[] var13 = null;
                if (var0.ordinates != null) {
                    var13 = new BigDecimal[var0.ordinates.length];
                    if (var0.linfo == 0) {
                        for (var14 = 0; var14 < var0.ordinates.length; ++var14) {
                            var13[var14] = new BigDecimal(var0.ordinates[var14]);
                        }
                    } else {
                        if (var0.dim == 2 || var0.dim == 3 && var0.linfo != 3 || var0.dim == 4 && var0.linfo != 3 && var0.linfo != 4) {
                            throw new SQLException("An invalid gtype value for LRS is found");
                        }

                        for (var14 = 0; var14 < var0.ordinates.length; ++var14) {
                            if (Double.isNaN(var0.ordinates[var14]) && var14 % var0.dim != var0.linfo - 1) {
                                throw new SQLException("An invalid Double.NaN is found in LRS ordinates");
                            }

                            var13[var14] = Double.isNaN(var0.ordinates[var14]) ? null : new BigDecimal(var0.ordinates[var14]);
                        }
                    }
                }

                var12 = var0.ordinates == null ? null : new ARRAY(var7, var1, var13);
            } else {
                Double[] var16 = null;
                if (var0.ordinates != null) {
                    var16 = new Double[var0.ordinates.length];
                    if (var0.linfo == 0) {
                        for (var14 = 0; var14 < var0.ordinates.length; ++var14) {
                            var16[var14] = new Double(var0.ordinates[var14]);
                        }
                    } else {
                        if (var0.dim == 2 || var0.dim == 3 && var0.linfo != 3 || var0.dim == 4 && var0.linfo != 3 && var0.linfo != 4) {
                            throw new SQLException("An invalid gtype value for LRS is found");
                        }

                        for (var14 = 0; var14 < var0.ordinates.length; ++var14) {
                            if (Double.isNaN(var0.ordinates[var14]) && var14 % var0.dim != var0.linfo - 1) {
                                throw new SQLException("An invalid Double.NaN is found in LRS ordinates");
                            }

                            var16[var14] = Double.isNaN(var0.ordinates[var14]) ? null : new Double(var0.ordinates[var14]);
                        }
                    }
                }

                var12 = var0.ordinates == null ? null : new ARRAY(var7, var1, var16);
            }

            Object[] var17 = new Object[]{var8, var9, var10, var15, var12};
            return new STRUCT(var4, var1, var17);
        }
    }

    public static Object[] getOracleDescriptors(Connection var0) throws SQLException {
        StructDescriptor var1 = StructDescriptor.createDescriptor("MDSYS.SDO_GEOMETRY", var0);
        StructDescriptor var2 = StructDescriptor.createDescriptor("MDSYS.SDO_POINT_TYPE", var0);
        ArrayDescriptor var3 = ArrayDescriptor.createDescriptor("MDSYS.SDO_ELEM_INFO_ARRAY", var0);
        ArrayDescriptor var4 = ArrayDescriptor.createDescriptor("MDSYS.SDO_ORDINATE_ARRAY", var0);
        return new Object[]{var1, var2, var3, var4};
    }

    protected final boolean isOptimizedPoint() {
        return this.gtype % 100 == 1 && this.ordinates == null && !Double.isNaN(this.x) && !Double.isNaN(this.y);
    }

    protected static final void createDBDescriptors(Connection var0) throws SQLException {
        geomDesc = StructDescriptor.createDescriptor("MDSYS.SDO_GEOMETRY", var0);
        pointDesc = StructDescriptor.createDescriptor("MDSYS.SDO_POINT_TYPE", var0);
        elemInfoDesc = ArrayDescriptor.createDescriptor("MDSYS.SDO_ELEM_INFO_ARRAY", var0);
        ordinatesDesc = ArrayDescriptor.createDescriptor("MDSYS.SDO_ORDINATE_ARRAY", var0);
    }

    protected final boolean isSimpleElement(int var1) {
        return var1 == 1 || var1 == 2 || var1 % 10 == 3;
    }

    protected final boolean isCompoundElement(int var1) {
        return var1 == 4 || var1 % 10 == 5;
    }

    protected static final void expandMBR(double[] var0, double var1, double var3) {
        if (var0[0] > var1) {
            var0[0] = var1;
        }

        if (var0[1] > var3) {
            var0[1] = var3;
        }

        if (var0[2] < var1) {
            var0[2] = var1;
        }

        if (var0[3] < var3) {
            var0[3] = var3;
        }

    }

    protected static final void expandMBR(double[] var0, double var1, double var3, double var5) {
        if (var0[0] > var1) {
            var0[0] = var1;
        }

        if (var0[1] > var3) {
            var0[1] = var3;
        }

        if (var0[2] > var5) {
            var0[2] = var5;
        }

        if (var0[3] < var1) {
            var0[3] = var1;
        }

        if (var0[4] < var3) {
            var0[4] = var3;
        }

        if (var0[5] < var5) {
            var0[5] = var5;
        }

    }

    protected final void expandMBR(double[] var1, double[] var2, int var3) {
        for (int var4 = 0; var4 < var2.length / var3; ++var4) {
            int var5 = var4 * var3;
            if (var3 == 2) {
                expandMBR(var1, var2[var5], var2[var5 + 1]);
            } else {
                expandMBR(var1, var2[var5], var2[var5 + 1], var2[var5 + 2]);
            }
        }

    }

    protected final void expandMBR(double[] var1, double[] var2, int var3, int var4, int var5) {
        for (int var6 = 0; var6 < var4 / var5; ++var6) {
            int var7 = var3 + var6 * var5;
            if (var5 == 2) {
                expandMBR(var1, var2[var7], var2[var7 + 1]);
            } else {
                expandMBR(var1, var2[var7], var2[var7 + 1], var2[var7 + 2]);
            }
        }

    }

    protected void reOrientCurves() {
    }

    public static final double[] computeArc(double var0, double var2, double var4, double var6, double var8, double var10) {
        double var25 = var0 - var4;
        double var27 = var4 - var8;
        double var29 = var6 - var10;
        double var31 = var2 - var6;
        double var33 = var0 + var4;
        double var35 = var4 + var8;
        double var37 = var2 + var6;
        double var39 = var6 + var10;
        double var41 = var25 * var29 - var27 * var31;
        if (var41 > -4.9E-323D && var41 < 4.9E-323D) {
            return null;
        } else {
            double[] var43 = new double[6];
            double var13 = (var29 * var33 * var25 - var31 * var35 * var27 + var31 * var29 * (var31 + var29)) / var41;
            double var15;
            if (Math.abs(var6 - var10) < 5.0E-8D) {
                var15 = (var37 * var31 + (var33 - var13) * var25) / var31;
            } else {
                var15 = (var39 * var29 + (var35 - var13) * var27) / var29;
            }

            var13 *= 0.5D;
            var15 *= 0.5D;
            double var17 = Math.sqrt((var13 - var0) * (var13 - var0) + (var15 - var2) * (var15 - var2));
            double var19 = Math.atan2(var2 - var15, var0 - var13);
            if (var19 < 0.0D) {
                var19 += 6.283185307179586D;
            }

            double var21 = Math.atan2(var6 - var15, var4 - var13);
            if (var21 < 0.0D) {
                var21 += 6.283185307179586D;
            }

            double var23 = Math.atan2(var10 - var15, var8 - var13);
            if (var23 < 0.0D) {
                var23 += 6.283185307179586D;
            }

            var43[0] = var13;
            var43[1] = var15;
            var43[2] = var17;
            var43[3] = var19;
            var43[4] = var21;
            var43[5] = var23;
            return var43;
        }
    }

    protected static final double[] computeArcMBR(double var0, double var2, double var4, double var6, double var8, double var10) {
        double[] var12 = computeArc(var0, var2, var4, var6, var8, var10);
        if (var12 == null) {
            var12 = new double[]{var0, var2, var4, var6, var8, var10};
            return var12;
        } else {
            double var13 = var12[0];
            double var15 = var12[1];
            double var17 = var12[2];
            double var19 = var12[3];
            double var21 = var12[4];
            double var23 = var12[5];
            double var25 = orientation(var0, var2, var4, var6, var8, var10);
            double[] var27 = new double[]{Math.min(var0, var8), Math.min(var2, var10), Math.max(var0, var8), Math.max(var2, var10)};
            if (thetaInArc(0.0D, var19, var23, var25) != 0) {
                expandMBR(var27, var13 + var17, var15);
            }

            if (thetaInArc(1.5707963267948966D, var19, var23, var25) != 0) {
                expandMBR(var27, var13, var15 + var17);
            }

            if (thetaInArc(3.141592653589793D, var19, var23, var25) != 0) {
                expandMBR(var27, var13 - var17, var15);
            }

            if (thetaInArc(4.71238898038469D, var19, var23, var25) != 0) {
                expandMBR(var27, var13, var15 - var17);
            }

            var12 = new double[]{var27[0], var27[1], var27[0], var27[3], var27[2], var27[3], var27[2], var27[1]};
            return var12;
        }
    }

    protected static final short thetaInArc(double var0, double var2, double var4, double var6) {
        if (var0 != var2 && var0 != var4 && var0 != var2 + 6.283185307179586D && var0 != var4 + 6.283185307179586D && var0 != var2 - 6.283185307179586D && var0 != var4 - 6.283185307179586D) {
            short var8 = 0;
            if (var6 > 0.0D) {
                ++var8;
            }

            if (var4 > var2) {
                ++var8;
            }

            if (var2 < var0 && var0 < var4 || var2 > var0 && var0 > var4) {
                ++var8;
            }

            return (short) (var8 & 1);
        } else {
            return -1;
        }
    }

    protected static final double orientation(double var0, double var2, double var4, double var6, double var8, double var10) {
        return var0 * var6 + var4 * var10 + var8 * var2 - (var8 * var6 + var4 * var2 + var0 * var10);
    }

    public static double[] expandCircle(double var0, double var2, double var4, double var6, double var8, double var10) {
        byte var12 = 4;
        double[] var13 = computeArc(var0, var2, var4, var6, var8, var10);
        if (var13 == null) {
            var13 = new double[]{var0, var2, var4, var6, var8, var10};
            return var13;
        } else {
            double var14 = var13[0];
            double var16 = var13[1];
            double var18 = var13[2];
            double var20 = var13[3];
            double var22 = var13[4];
            double var24 = var13[3];
            double var26 = orientation(var0, var2, var4, var6, var8, var10);
            double var28 = var20;
            double var30 = var24;
            if (var26 > 0.0D) {
                var30 = 1.0D;
            } else if (var26 < 0.0D) {
                var30 = -1.0D;
            }

            double[] var32 = new double[10];

            for (int var33 = 0; var33 <= var12; ++var33) {
                double var34 = var28 + (double) var33 * 1.5707963267948966D * var30;
                var32[var33 * 2] = var14 + var18 * Math.cos(var34);
                var32[var33 * 2 + 1] = var16 + var18 * Math.sin(var34);
            }

            var32[0] = var0;
            var32[1] = var2;
            var32[8] = var0;
            var32[9] = var2;
            return var32;
        }
    }

    public static double[] linearizeArc(double var0, double var2, double var4, double var6, double var8, double var10, int var12) {
        int var13 = var12 - 1;
        double[] var14;
        if (var13 < 1) {
            var14 = new double[]{var0, var2, var4, var6, var8, var10};
            return var14;
        } else {
            var14 = computeArc(var0, var2, var4, var6, var8, var10);
            if (var14 == null) {
                var14 = new double[]{var0, var2, var4, var6, var8, var10};
                return var14;
            } else {
                double var15 = var14[0];
                double var17 = var14[1];
                double var19 = var14[2];
                double var21 = var14[3];
                double var23 = var14[4];
                double var25 = var14[5];
                double var27 = orientation(var0, var2, var4, var6, var8, var10);
                double var31;
                if (var27 > 0.0D && var25 < var21) {
                    var31 = var25 + 6.283185307179586D;
                } else if (var27 < 0.0D && var25 > var21) {
                    var31 = var25 - 6.283185307179586D;
                }

                double var33 = var25 - var21;
                if (var27 > 0.0D && var33 < 0.0D) {
                    var33 += 6.283185307179586D;
                } else if (var27 < 0.0D && var33 > 0.0D) {
                    var33 -= 6.283185307179586D;
                }

                double var35 = Math.abs(var33) / (double) var13;
                double var37 = Math.sin(var35);
                double var39 = Math.cos(var35);
                if (var27 > 0.0D) {
                    var37 = -var37;
                }

                double var41 = var0 - var15;
                double var43 = var2 - var17;
                double[] var45 = new double[(var13 + 1) * 2];
                var45[0] = var0;
                var45[1] = var2;
                double var46 = 0.0D;

                for (int var48 = 1; var48 < var13; ++var48) {
                    var46 = var41 * var39 + var43 * var37;
                    var43 = -var41 * var37 + var43 * var39;
                    var41 = var46;
                    var45[2 * var48] = var15 + var46;
                    var45[2 * var48 + 1] = var17 + var43;
                }

                var45[(var13 + 1) * 2 - 2] = var8;
                var45[(var13 + 1) * 2 - 1] = var10;
                return var45;
            }
        }
    }

    private static double[] linearizeArc(double var0, double var2, double var4, double var6, double var8, double var10, double var12, boolean var14) {
        if (var12 <= 0.0D) {
            return linearizeArc(var0, var2, var4, var6, var8, var10, 0);
        } else {
            double[] var15 = computeArc(var0, var2, var4, var6, var8, var10);
            if (var15 == null) {
                var15 = new double[]{var0, var2, var4, var6, var8, var10};
                return var15;
            } else {
                double var16 = var15[0];
                double var18 = var15[1];
                double var20 = var15[2];
                double var22 = var15[3];
                double var24 = var15[4];
                double var26 = var15[5];
                double var28 = orientation(var0, var2, var4, var6, var8, var10);
                if (var14) {
                    var12 = var20 * (1.0D - Math.cos(Math.asin(var12 / (2.0D * var20))));
                }

                double var30 = var26 - var22;
                if (var28 > 0.0D && var30 < 0.0D) {
                    var30 += 6.283185307179586D;
                } else if (var28 < 0.0D && var30 > 0.0D) {
                    var30 -= 6.283185307179586D;
                }

                if (var30 < 0.0D) {
                    var30 = -var30;
                }

                double var32 = Math.acos(1.0D - var12 / var20) * 2.0D;
                int var34 = (int) Math.ceil(var30 / var32);
                if (var34 > 1000) {
                    var34 = 1000;
                }

                if (var34 < 2) {
                    var15 = new double[]{var0, var2, var4, var6, var8, var10};
                    return var15;
                } else {
                    double var35 = Math.abs(var30) / (double) var34;
                    double var37 = Math.sin(var35);
                    double var39 = Math.cos(var35);
                    if (var28 > 0.0D) {
                        var37 = -var37;
                    }

                    double var41 = var0 - var16;
                    double var43 = var2 - var18;
                    double[] var45 = new double[(var34 + 1) * 2];
                    var45[0] = var0;
                    var45[1] = var2;
                    double var46 = 0.0D;

                    for (int var48 = 1; var48 < var34; ++var48) {
                        var46 = var41 * var39 + var43 * var37;
                        var43 = -var41 * var37 + var43 * var39;
                        var41 = var46;
                        var45[2 * var48] = var16 + var46;
                        var45[2 * var48 + 1] = var18 + var43;
                    }

                    var45[(var34 + 1) * 2 - 2] = var8;
                    var45[(var34 + 1) * 2 - 1] = var10;
                    return var45;
                }
            }
        }
    }

    public static double[] linearizeArc(double var0, double var2, double var4, double var6, double var8, double var10) {
        double[] var12 = computeArc(var0, var2, var4, var6, var8, var10);
        double var13 = var12[2] / 250.0D;
        return linearizeArc(var0, var2, var4, var6, var8, var10, var13, false);
    }

    public static double[] reFormulateArc(double[] var0) {
        double[] var1 = computeArc(var0[0], var0[1], var0[2], var0[3], var0[4], var0[5]);
        if (var1 == null) {
            var1 = new double[]{var0[0], var0[1], var0[2], var0[3], var0[4], var0[5]};
            return var1;
        } else {
            double var2 = var1[0];
            double var4 = var1[1];
            double var6 = var1[2];
            double var8 = -var1[3];
            double var10 = var1[4];
            double var12 = -var1[5];
            double var14 = -orientation(var0[0], var0[1], var0[2], var0[3], var0[4], var0[5]);
            if (var14 > 0.0D && var12 < var8) {
                var12 += 6.283185307179586D;
                if (var12 > 6.283185307179586D) {
                    var12 -= 6.283185307179586D;
                    var8 -= 6.283185307179586D;
                }
            } else if (var14 < 0.0D && var12 > var8) {
                var12 -= 6.283185307179586D;
                if (var12 < -6.283185307179586D) {
                    var12 += 6.283185307179586D;
                    var8 += 6.283185307179586D;
                }
            }

            double[] var16 = new double[]{var2, var4, var6, var8, var10, var12};
            return var16;
        }
    }

    protected static final double[] closeCoords(double[] var0, int var1) {
        int var2 = var0.length / var1 - 1;
        boolean var3 = true;

        for (int var4 = 0; var4 < var1; ++var4) {
            if (var0[0 + var4] != var0[var2 * var1 + var4]) {
                var3 = false;
            }
        }

        if (var3) {
            return var0;
        } else {
            double[] var6 = new double[var0.length + var1];
            System.arraycopy(var0, 0, var6, 0, var0.length);
            ++var2;

            for (int var5 = 0; var5 < var1; ++var5) {
                var6[var2 * var1 + var5] = var6[var5];
            }

            return var6;
        }
    }

    private void writeObject(ObjectOutputStream var1) throws IOException {
        var1.writeInt(this.gtype);
        var1.writeInt(this.linfo);
        var1.writeInt(this.srid);
        var1.writeDouble(this.x);
        var1.writeDouble(this.y);
        var1.writeDouble(this.z);
        var1.writeObject(this.elemInfo);
        var1.writeObject(this.ordinates);
        var1.writeInt(this.dim);
    }

    private void readObject(ObjectInputStream var1) throws IOException, ClassNotFoundException {
        this.gtype = var1.readInt();
        this.linfo = var1.readInt();
        this.srid = var1.readInt();
        this.x = var1.readDouble();
        this.y = var1.readDouble();
        this.z = var1.readDouble();
        this.elemInfo = (int[]) ((int[]) var1.readObject());
        this.ordinates = (double[]) ((double[]) var1.readObject());
        this.dim = var1.readInt();
    }

    public long getSize() {
        return (long) (40 + (this.elemInfo == null ? 0 : 4 * this.elemInfo.length) + (this.ordinates == null ? 0 : 8 * this.ordinates.length));
    }

    public String toString() {
        String var1 = "JGeometry (gtype=" + this.gtype + ", dim=" + this.dim + ", srid=" + this.srid;
        return var1;
    }

    public String toStringFull() {
        if (this == null) {
            return "NULL JGeometry";
        } else {
            String var1 = "JGeometry (gtype=" + this.gtype + ", dim=" + this.dim + ", srid=" + this.srid;
            if (this.isOptimizedPoint()) {
                var1 = var1 + ", Point=(";
                var1 = var1 + this.x + "," + this.y;
                if (this.dim > 2) {
                    var1 = var1 + "," + this.z;
                }

                var1 = var1 + "))";
                return var1;
            } else {
                var1 = var1 + ",  \n ElemInfo(";

                int var2;
                for (var2 = 0; var2 < this.elemInfo.length - 1; ++var2) {
                    var1 = var1 + this.elemInfo[var2] + ",";
                }

                var1 = var1 + this.elemInfo[this.elemInfo.length - 1] + ")";
                var1 = var1 + ",  \n Ordinates(";

                for (var2 = 0; var2 < this.ordinates.length / this.dim; ++var2) {
                    for (int var3 = 0; var3 < this.dim; ++var3) {
                        var1 = var1 + this.ordinates[var3 + var2 * this.dim];
                        if (var3 < this.dim - 1) {
                            var1 = var1 + ",";
                        }
                    }

                    var1 = var1 + "\n";
                }

                var1 = var1 + "))";
                return var1;
            }
        }
    }

    public String toStringFull(int var1) {
        if (this == null) {
            return "NULL JGeometry";
        } else {
            DecimalFormat var2 = new DecimalFormat();
            var2.setMaximumFractionDigits(var1);
            var2.setMinimumFractionDigits(var1);
            var2.setGroupingUsed(false);
            String var3 = "JGeometry (gtype=" + this.gtype + ", dim=" + this.dim + ", srid=" + this.srid;
            if (this.isOptimizedPoint()) {
                String var9 = var2.format(this.x).toString();
                String var10 = var2.format(this.y).toString();
                var3 = var3 + ", Point=(";
                var3 = var3 + var9 + "," + var10;
                if (this.dim > 2) {
                    var3 = var3 + "," + var2.format(this.z).toString();
                }

                var3 = var3 + "))";
                return var3;
            } else {
                var3 = var3 + ",  \n ElemInfo(";

                int var4;
                for (var4 = 0; var4 < this.elemInfo.length - 1; ++var4) {
                    var3 = var3 + this.elemInfo[var4] + ",";
                }

                var3 = var3 + this.elemInfo[this.elemInfo.length - 1] + ")";
                var3 = var3 + ",  \n Ordinates(";

                for (var4 = 0; var4 < this.ordinates.length / this.dim; ++var4) {
                    for (int var5 = 0; var5 < this.dim; ++var5) {
                        double var6 = this.ordinates[var5 + var4 * this.dim];
                        String var8 = var2.format(var6).toString();
                        var3 = var3 + var8;
                        if (var5 < this.dim - 1) {
                            var3 = var3 + ",";
                        }
                    }

                    var3 = var3 + "\n";
                }

                var3 = var3 + "))";
                return var3;
            }
        }
    }

    /** @deprecated */
    public boolean equals(Object var1) {
        if (var1 == null) {
            return false;
        } else if (!(var1 instanceof JGeometry)) {
            return false;
        } else {
            JGeometry var2 = (JGeometry) var1;
            return this.getType() == var2.getType() && Arrays.equals(this.getPoint(), var2.getPoint()) && Arrays.equals(this.getElemInfo(), var2.getElemInfo())
                    && Arrays.equals(this.getOrdinatesArray(), var2.getOrdinatesArray());
        }
    }

    public static final JGeometry load(byte[] var0) throws Exception {
        return var0 != null && var0.length >= 8 ? SdoPickler.unpickle(var0) : null;
    }

    public static byte[] store(JGeometry var0) throws Exception {
        return SdoPickler.pickle(var0);
    }

    public static final STRUCT store(Connection var0, JGeometry var1) throws Exception {
        Object[] var2 = getOracleDescriptors(var0);
        return store(var1, var0, var2);
    }

    public static final STRUCT store(Connection var0, JGeometry var1, StructDescriptor var2) throws Exception {
        if (var2 == null) {
            geomDesc = getGeomDescriptor(var0);
            return new STRUCT(geomDesc, SdoPickler.pickle(var1), var0);
        } else {
            return new STRUCT(var2, SdoPickler.pickle(var1), var0);
        }
    }

    public static StructDescriptor getGeomDescriptor(Connection var0) throws SQLException {
        return StructDescriptor.createDescriptor("MDSYS.SDO_GEOMETRY", var0);
    }

    public static String byteArrayToHexString(byte[] var0) {
        boolean var1 = false;
        int var2 = 0;
        if (var0 != null && var0.length > 0) {
            String[] var3 = new String[]{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "A", "B", "C", "D", "E", "F"};

            StringBuffer var4;
            for (var4 = new StringBuffer(var0.length * 2); var2 < var0.length; ++var2) {
                byte var6 = (byte) (var0[var2] & 240);
                var6 = (byte) (var6 >>> 4);
                var6 = (byte) (var6 & 15);
                var4.append(var3[var6]);
                var6 = (byte) (var0[var2] & 15);
                var4.append(var3[var6]);
            }

            String var5 = new String(var4);
            return var5;
        } else {
            return null;
        }
    }

    public final JGeometry projectToLTP(double var1, double var3) throws DataException {
        if (this.srid == 0) {
            return null;
        } else {
            JGeometry.Gc_trans var6 = new JGeometry.Gc_trans(var1, var3);
            JGeometry var5;
            double[] var7;
            double[] var9;
            double[] var23;
            if (this.isOptimizedPoint()) {
                var5 = new JGeometry(this.x, this.y, 0);
                var7 = new double[]{this.x, this.y, 0.0D};
                this.lltogXYZ(var7, var6);
                var9 = new double[]{var7[0], var7[1], var7[2], var7[0], var7[1], var7[2]};
                var23 = new double[3];
                this.gxyzgmcenter(var23, var9, var6);
                JGeometry.LT_transform var25 = new JGeometry.LT_transform(var23[0], var23[1], var6);
                this.ltxform(var7, var25);
                var5.x = var7[0];
                var5.y = var7[1];
                var5.lttpH = var25;
                var5.gtransH = var6;
                return var5;
            } else {
                var7 = new double[3 * (this.ordinates.length / 2)];
                var9 = new double[3];

                for (int var10 = 0; var10 < var7.length / 3; ++var10) {
                    var9[0] = this.ordinates[2 * var10];
                    var9[1] = this.ordinates[2 * var10 + 1];
                    var9[2] = 0.0D;
                    this.lltogXYZ(var9, var6);
                    var7[var10 * 3] = var9[0];
                    var7[var10 * 3 + 1] = var9[1];
                    var7[var10 * 3 + 2] = var9[2];
                }

                var23 = new double[6];
                var23[0] = var23[3] = var7[0];
                var23[1] = var23[4] = var7[1];
                var23[2] = var23[5] = var7[2];

                for (int var11 = 0; var11 < var7.length / 3; ++var11) {
                    if (var23[0] > var7[var11 * 3]) {
                        var23[0] = var7[var11 * 3];
                    }

                    if (var23[1] > var7[var11 * 3 + 1]) {
                        var23[1] = var7[var11 * 3 + 1];
                    }

                    if (var23[2] > var7[var11 * 3 + 2]) {
                        var23[2] = var7[var11 * 3 + 2];
                    }

                    if (var23[3] < var7[var11 * 3]) {
                        var23[3] = var7[var11 * 3];
                    }

                    if (var23[4] < var7[var11 * 3 + 1]) {
                        var23[4] = var7[var11 * 3 + 1];
                    }

                    if (var23[5] < var7[var11 * 3 + 2]) {
                        var23[5] = var7[var11 * 3 + 2];
                    }
                }

                double[] var24 = new double[3];
                this.gxyzgmcenter(var24, var23, var6);
                JGeometry.LT_transform var12 = new JGeometry.LT_transform(var24[0], var24[1], var6);
                double[] var13 = new double[2 * (var7.length / 3)];
                double var20 = 0.0D;
                double var18 = 0.0D;
                double var16 = 0.0D;
                double var14 = 0.0D;

                for (int var22 = 0; var22 < var7.length / 3; ++var22) {
                    var9[0] = var7[var22 * 3];
                    var9[1] = var7[var22 * 3 + 1];
                    var9[2] = var7[var22 * 3 + 2];
                    this.ltxform(var9, var12);
                    var13[2 * var22] = var9[0];
                    var13[2 * var22 + 1] = var9[1];
                    if (var13[2 * var22] < var14) {
                        var14 = var13[2 * var22];
                    }

                    if (var13[2 * var22] > var18) {
                        var18 = var13[2 * var22];
                    }

                    if (var13[2 * var22 + 1] < var16) {
                        var16 = var13[2 * var22 + 1];
                    }

                    if (var13[2 * var22 + 1] > var20) {
                        var20 = var13[2 * var22 + 1];
                    }
                }

                if (var18 - var14 <= 1.274E7D && var20 - var16 <= 1.274E7D) {
                    var5 = new JGeometry(this.gtype, 0, this.elemInfo, var13);
                    var5.lttpH = var12;
                    var5.gtransH = var6;
                    return var5;
                } else {
                    throw new DataException("Buffer Distance Too large for LTP");
                }
            }
        }
    }

    public final JGeometry projectFromLTP() throws DataException {
        JGeometry var1;
        double[] var2;
        if (this.isOptimizedPoint()) {
            var1 = new JGeometry(this.x, this.y, this.gtransH.txsrid);
            var2 = new double[]{this.x, this.y, 0.0D};
            this.iltxform(var2, this.lttpH, this.gtransH);
            this.gXYZtoll(var2, this.gtransH);
            var1.x = var2[0] / this.gtransH.unitfactor;
            var1.y = var2[1] / this.gtransH.unitfactor;
            var1.setSRID(this.getSRID());
            return var1;
        } else {
            var2 = new double[this.ordinates.length];
            double[] var4 = new double[3];
            double var11 = 0.0D;
            double var9 = 0.0D;
            double var7 = 0.0D;
            double var5 = 0.0D;

            int var13;
            for (var13 = 0; var13 < this.ordinates.length / 2; ++var13) {
                if (this.ordinates[2 * var13] < var5) {
                    var5 = this.ordinates[2 * var13];
                }

                if (this.ordinates[2 * var13] > var9) {
                    var9 = this.ordinates[2 * var13];
                }

                if (this.ordinates[2 * var13 + 1] < var7) {
                    var7 = this.ordinates[2 * var13 + 1];
                }

                if (this.ordinates[2 * var13 + 1] > var11) {
                    var11 = this.ordinates[2 * var13 + 1];
                }
            }

            if (var9 - var5 <= 1.274E7D && var11 - var7 <= 1.274E7D) {
                for (var13 = 0; var13 < this.ordinates.length / 2; ++var13) {
                    var4[0] = this.ordinates[2 * var13];
                    var4[1] = this.ordinates[2 * var13 + 1];
                    var4[2] = 0.0D;
                    this.iltxform(var4, this.lttpH, this.gtransH);
                    this.gXYZtoll(var4, this.gtransH);
                    var2[2 * var13] = var4[0] / this.gtransH.unitfactor;
                    var2[2 * var13 + 1] = var4[1] / this.gtransH.unitfactor;
                }

                var1 = new JGeometry(this.gtype, this.getSRID(), this.elemInfo, var2);
                return var1;
            } else {
                throw new DataException("Buffer Distance Too large for LTP");
            }
        }
    }

    public final JGeometry projectToGNM_longLat() throws DataException {
        double var1 = 0.0D;
        double var3 = 0.0D;
        double[] var5 = this.getMBR();
        double[] var6 = new double[var5.length / 2];

        for (int var7 = 0; var7 < var5.length / 2; ++var7) {
            var6[var7] = (var5[var7 + var5.length / 2] + var5[var7]) / 2.0D;
        }

        var1 = 0.017453292519943295D * var6[0];
        var3 = 0.017453292519943295D * var6[1];
        if (this.getType() == 1) {
            var1 /= 2.0D;
            var3 /= 2.0D;
        }

        return this.projectToGNM_longLat(var1, var3);
    }

    public final JGeometry projectToGNM_longLat(double var1, double var3) throws DataException {
        JGeometry var9 = null;
        double var13 = 6378000.0D;
        int[] var10;
        if (this.getElemInfo() == null) {
            var10 = new int[]{1, 1, 1};
        } else {
            var10 = new int[this.getElemInfo().length];

            for (int var5 = 0; var5 < this.getElemInfo().length; ++var5) {
                var10[var5] = this.getElemInfo()[var5];
            }
        }

        double[] var11;
        double[] var12;
        if (this.getOrdinatesArray() == null) {
            if (this.dim == 2) {
                var12 = new double[2];
                var11 = new double[]{this.x, this.y};
            } else {
                var12 = new double[3];
                var11 = new double[]{this.x, this.y, this.z};
            }
        } else {
            var12 = new double[this.getOrdinatesArray().length];
            var11 = new double[this.getOrdinatesArray().length];

            for (int var8 = 0; var8 < this.getOrdinatesArray().length; ++var8) {
                var11[var8] = this.getOrdinatesArray()[var8];
            }
        }

        int var7 = 0;
        double[] var15 = this.getMBR();

        for (int var6 = 0; var6 < var11.length / (var15.length / 2); ++var6) {
            double var16 = 0.017453292519943295D * var11[var15.length / 2 * var6];
            double var18 = 0.017453292519943295D * var11[var15.length / 2 * var6 + 1];
            double var20 = 0.0D;
            if (var15.length == 6) {
                var20 = var11[var15.length / 2 * var6 + 2];
            }

            double var22 = Math.sin(var3) * Math.sin(var18) + Math.cos(var3) * Math.cos(var18) * Math.cos(var16 - var1);
            double var24 = 0.0D;
            double var26 = 0.0D;
            if (Math.abs(var3 - 1.5707963267948966D) <= 1.0E-16D) {
                var24 = 1.0D / Math.tan(var18) * Math.sin(var16 - var1);
                var26 = -1.0D / Math.tan(var18) * Math.cos(var16 - var1);
            } else if (Math.abs(var3 - -1.5707963267948966D) <= 1.0E-16D) {
                var24 = -1.0D / Math.tan(var18) * Math.sin(var16 - var1);
                var26 = 1.0D / Math.tan(var18) * Math.cos(var16 - var1);
            } else if (Math.abs(var3) <= 1.0E-16D) {
                var24 = Math.tan(var16 - var1);
                var26 = Math.tan(var18) / Math.cos(var16 - var1);
            } else {
                var24 = Math.cos(var18) * Math.sin(var16 - var1) / var22;
                var26 = (Math.cos(var3) * Math.sin(var18) - Math.sin(var3) * Math.cos(var18) * Math.cos(var16 - var1)) / var22;
            }

            var12[var7] = var13 * var24;
            ++var7;
            var12[var7] = var13 * var26;
            ++var7;
            if (var15.length == 6) {
                var12[var7] = var20;
                ++var7;
            }
        }

        var9 = new JGeometry(this.getType(), 0, var10, var12);
        int var28 = this.dim;
        int var17 = this.linfo;
        var9.dim = var28;
        var9.linfo = var17;
        return var9;
    }

    public final JGeometry projectFromGNM_longLat(double var1, double var3) throws DataException {
        JGeometry var9 = null;
        double var13 = 6378000.0D;
        int[] var10;
        if (this.getElemInfo() == null) {
            var10 = new int[]{1, 1, 1};
        } else {
            var10 = new int[this.getElemInfo().length];

            for (int var5 = 0; var5 < this.getElemInfo().length; ++var5) {
                var10[var5] = this.getElemInfo()[var5];
            }
        }

        double[] var11;
        double[] var12;
        if (this.getOrdinatesArray() == null) {
            if (this.dim == 2) {
                var12 = new double[2];
                var11 = new double[]{this.x, this.y};
            } else {
                var12 = new double[3];
                var11 = new double[]{this.x, this.y, this.z};
            }
        } else {
            var12 = new double[this.getOrdinatesArray().length];
            var11 = new double[this.getOrdinatesArray().length];

            for (int var8 = 0; var8 < this.getOrdinatesArray().length; ++var8) {
                var11[var8] = this.getOrdinatesArray()[var8];
            }
        }

        int var7 = 0;
        double[] var15 = this.getMBR();

        for (int var6 = 0; var6 < var11.length / (var15.length / 2); ++var6) {
            double var16 = var11[var15.length / 2 * var6] / var13;
            double var18 = var11[var15.length / 2 * var6 + 1] / var13;
            double var20 = 0.0D;
            if (var15.length == 6) {
                var20 = var11[var15.length / 2 * var6 + 2];
            }

            double var22 = Math.sqrt(var16 * var16 + var18 * var18);
            double var24 = Math.atan2(var22, 1.0D);
            double var26 = 0.0D;
            if (var22 <= 1.0E-16D) {
                var26 = var3;
            } else {
                var26 = Math.asin(Math.cos(var24) * Math.sin(var3) + var18 * Math.sin(var24) * Math.cos(var3) / var22);
            }

            double var28 = 0.0D;
            if (Math.abs(var3 - 1.5707963267948966D) <= 1.0E-16D) {
                var28 = var1 + Math.atan2(var16, -var18);
            } else if (Math.abs(var3 - -1.5707963267948966D) <= 1.0E-16D) {
                var28 = var1 + Math.atan2(var16, var18);
            } else {
                var28 = var1 + Math.atan2(var16 * Math.sin(var24), var22 * Math.cos(var3) * Math.cos(var24) - var18 * Math.sin(var3) * Math.sin(var24));
            }

            var12[var7] = 57.29577951308232D * var28;
            ++var7;
            var12[var7] = 57.29577951308232D * var26;
            ++var7;
            if (var15.length == 6) {
                var12[var7] = var20;
                ++var7;
            }
        }

        var9 = new JGeometry(this.getType(), this.getSRID(), var10, var12);
        int var30 = this.dim;
        int var17 = this.linfo;
        var9.dim = var30;
        var9.linfo = var17;
        return var9;
    }

    public final JGeometry densifyArcs(double var1) {
        return this.densifyArcs(var1, false);
    }

    public JGeometry densifyArcs(double var1, boolean var3) {
        ArrayList var4 = new ArrayList();
        ArrayList var5 = new ArrayList();
        int var6 = this.dim * 1000 + this.linfo * 100 + this.gtype;
        if (this.isOptimizedPoint()) {
            return new JGeometry(this.x, this.y, this.srid);
        } else if (!this.isPoint() && !this.isMultiPoint()) {
            double[] var10;
            int var11;
            int var22;
            if (this.isCircle()) {
                double[] var18 = expandCircle(this.ordinates[0], this.ordinates[1], this.ordinates[this.dim], this.ordinates[this.dim + 1], this.ordinates[2 * this.dim], this.ordinates[2 * this.dim
                        + 1]);
                int[] var20 = new int[]{1, 1003, 1};
                double[] var27 = linearizeArc(var18[0], var18[1], var18[2], var18[3], var18[4], var18[5], var1, var3);

                for (var22 = 0; var22 < var27.length; ++var22) {
                    var5.add(new Double(var27[var22]));
                }

                var10 = linearizeArc(var18[4], var18[5], var18[6], var18[7], var18[8], var18[9], var1, var3);

                for (var11 = 2; var11 < var10.length; ++var11) {
                    var5.add(new Double(var10[var11]));
                }

                double[] var23 = new double[var5.size()];

                for (int var25 = 0; var25 < var5.size(); ++var25) {
                    var23[var25] = (Double) var5.get(var25);
                }

                return new JGeometry(this.gtype, this.srid, var20, var23);
            } else {
                JGeometry.ElementIterator var7 = new JGeometry.ElementIterator(this);

                int var9;
                while (var7.next()) {
                    int var8 = var7.ord_offset;
                    if (var7.isCompound && var7.isFirstElemOfCompound || !var7.isCompound) {
                        var4.add(new Integer(var5.size() + 1));
                        var9 = var7.original_etype;
                        if (var9 == 4) {
                            var9 = 2;
                        } else if (var9 == 1005) {
                            var9 = 1003;
                        } else if (var9 == 2005) {
                            var9 = 2003;
                        }

                        var4.add(new Integer(var9));
                        if (var7.eitpr == 3) {
                            var4.add(new Integer(1));
                        } else {
                            var4.add(new Integer(1));
                        }
                    }

                    for (var9 = 0; var9 < var7.nCoord - 1; ++var9) {
                        if (var7.eitpr == 1) {
                            var5.add(new Double(this.ordinates[var8 + var9 * this.dim]));
                            var5.add(new Double(this.ordinates[var8 + var9 * this.dim + 1]));
                            if (var7.lastElem && var9 == var7.nCoord - 2 || !var7.isCompound && var9 == var7.nCoord - 2 || var7.isCompound && var9 == var7.nCoord - 2 && var7.nSubElement == 1) {
                                var5.add(new Double(this.ordinates[var8 + var9 * this.dim + 2]));
                                var5.add(new Double(this.ordinates[var8 + var9 * this.dim + 3]));
                            }
                        } else if (var7.eitpr == 2) {
                            if (var8 + var9 * this.dim >= var7.next_ord_offset || var7.lastElem && var8 + var9 * this.dim >= var7.next_ord_offset - this.dim
                                    || (var7.next_ord_offset - (var8 + var9 * this.dim)) / this.dim < 2) {
                                break;
                            }

                            var10 = linearizeArc(this.ordinates[var8 + var9 * this.dim], this.ordinates[var8 + var9 * this.dim + 1], this.ordinates[var8 + (var9 + 1) * this.dim], this.ordinates[var8
                                    + (var9 + 1) * this.dim + 1], this.ordinates[var8 + (var9 + 2) * this.dim], this.ordinates[var8 + (var9 + 2) * this.dim + 1], var1, var3);

                            for (var11 = 0; var11 < var10.length - 2; ++var11) {
                                var5.add(new Double(var10[var11]));
                            }

                            if (var7.lastElem && var9 == var7.nCoord - 3 || !var7.isCompound && var9 == var7.nCoord - 3 || var7.isCompound && var9 == var7.nCoord - 3 && var7.nSubElement == 1) {
                                var5.add(new Double(var10[var10.length - 2]));
                                var5.add(new Double(var10[var10.length - 1]));
                            }

                            ++var9;
                            if (var9 >= var7.nCoord - 1 && var7.top_etype != 3 && var7.top_etype == 5) {
                            }
                        } else if (var7.eitpr == 3) {
                            double var21 = this.ordinates[var8 + 0];
                            double var24 = this.ordinates[var8 + 1];
                            double var29 = this.ordinates[var8 + this.dim];
                            double var16 = this.ordinates[var8 + this.dim + 1];
                            var5.add(new Double(var21));
                            var5.add(new Double(var24));
                            var5.add(new Double(var29));
                            var5.add(new Double(var24));
                            var5.add(new Double(var29));
                            var5.add(new Double(var16));
                            var5.add(new Double(var21));
                            var5.add(new Double(var16));
                            var5.add(new Double(var21));
                            var5.add(new Double(var24));
                            ++var9;
                        } else if (var7.eitpr == 4) {
                            if (var8 + var9 * this.dim >= var7.next_ord_offset || var7.lastElem && var8 + var9 * this.dim >= var7.next_ord_offset - this.dim) {
                                break;
                            }

                            var10 = expandCircle(this.ordinates[var8 + var9 * this.dim], this.ordinates[var8 + var9 * this.dim + 1], this.ordinates[var8 + (var9 + 1) * this.dim], this.ordinates[var8
                                    + (var9 + 1) * this.dim + 1], this.ordinates[var8 + (var9 + 2) * this.dim], this.ordinates[var8 + (var9 + 2) * this.dim + 1]);
                            int[] var10000 = new int[]{1, 1003, 1};
                            double[] var12 = linearizeArc(var10[0], var10[1], var10[2], var10[3], var10[4], var10[5], var1, var3);

                            for (int var13 = 0; var13 < var12.length; ++var13) {
                                var5.add(new Double(var12[var13]));
                            }

                            double[] var28 = linearizeArc(var10[4], var10[5], var10[6], var10[7], var10[8], var10[9], var1, var3);

                            for (int var14 = 2; var14 < var28.length; ++var14) {
                                var5.add(new Double(var28[var14]));
                            }

                            var9 += 2;
                        }
                    }
                }

                double[] var19 = new double[var5.size()];

                for (var9 = 0; var9 < var5.size(); ++var9) {
                    var19[var9] = (Double) var5.get(var9);
                }

                int[] var26 = new int[var4.size()];

                for (var22 = 0; var22 < var4.size(); ++var22) {
                    var26[var22] = (Integer) var4.get(var22);
                }

                return new JGeometry(this.gtype, this.srid, var26, var19);
            }
        } else {
            return new JGeometry(this.gtype, this.srid, this.elemInfo, this.ordinates);
        }
    }

    private double[] simplify(double[] var1, double var2) {
        int var4 = var1.length / this.dim;
        int[] var6 = new int[var4];
        double[] var5;
        int var8;
        if (var2 > 0.0D && var1.length >= 4) {
            var8 = 0;
            byte var7 = 0;
            int var34 = var7 + 1;
            var6[var7] = 0;
            int var9 = var4 - 1;
            var6[var9] = var4 - 1;
            int var24 = 0;

            do {
                int var10 = var6[var9];
                int var11 = 2 * var8;
                int var12 = 2 * var10;
                double var13 = var1[var12] - var1[var11];
                double var15 = var1[var12 + 1] - var1[var11 + 1];
                double var17 = Math.sqrt(var13 * var13 + var15 * var15);
                double var19 = 0.0D;
                double var21;
                int var23;
                int var33;
                if (var17 <= var2) {
                    for (var33 = var8 + 1; var33 < var10; ++var33) {
                        var23 = 2 * var33;
                        var13 = var1[var23] - var1[var11];
                        var15 = var1[var23 + 1] - var1[var11 + 1];
                        var21 = Math.sqrt(var13 * var13 + var15 * var15);
                        if (var21 >= var19) {
                            var19 = var21;
                            var24 = var33;
                        }
                    }
                } else if (Math.abs(var13) <= Math.abs(var15)) {
                    double var25 = var13 / var15;
                    double var29 = var25 * var1[var11 + 1];

                    for (var33 = var8 + 1; var33 < var10; ++var33) {
                        var23 = 2 * var33;
                        var21 = Math.abs(var1[var23] - var1[var11] + var29 - var25 * var1[var23 + 1]);
                        if (var21 >= var19) {
                            var19 = var21;
                            var24 = var33;
                        }
                    }

                    var19 = Math.abs(var19 * var15 / var17);
                } else {
                    double var27 = var15 / var13;
                    double var31 = var27 * var1[var11];

                    for (var33 = var8 + 1; var33 < var10; ++var33) {
                        var23 = 2 * var33;
                        var21 = Math.abs(var1[var23 + 1] - var1[var11 + 1] + var31 - var27 * var1[var23]);
                        if (var21 >= var19) {
                            var19 = var21;
                            var24 = var33;
                        }
                    }

                    var19 = Math.abs(var19 * var13 / var17);
                }

                if (var19 > var2) {
                    --var9;
                    var6[var9] = var24;
                } else {
                    var6[var34++] = var10;
                    ++var9;
                    var8 = var10;
                }
            } while (var9 < var4);

            var5 = new double[var34 * 2];

            for (var9 = 0; var9 < var34; ++var9) {
                var8 = var6[var9];
                var5[var9 * 2] = var1[2 * var8];
                var5[var9 * 2 + 1] = var1[2 * var8 + 1];
            }

            return var5;
        } else {
            var5 = new double[var1.length];

            for (var8 = 0; var8 < var1.length; ++var8) {
                var5[var8] = var1[var8];
            }

            return var5;
        }
    }

    public JGeometry simplify(double var1, double var3, double var5) throws Exception, SQLException {
        if (var3 != 0.0D && var5 != 0.0D) {
            JGeometry var8;
            try {
                JGeometry var7 = this.projectToLTP(var3, 1.0D / var5);
                var8 = var7.simplify(var1);
                var8.lttpH = var7.lttpH;
                var8.gtransH = var7.gtransH;
                var8.setSRID(this.getSRID());
            } catch (Exception var10) {
                throw var10;
            }

            return var8.projectFromLTP();
        } else {
            return this.simplify(var1);
        }
    }

    public final JGeometry simplify(double var1) {
        int var3 = this.elemInfo.length / 3;
        double[][] var6 = new double[var3][];

        int var7;
        double[] var8;
        int var10;
        for (var7 = 0; var7 < var3; ++var7) {
            int var4 = var7 == var3 - 1 ? this.ordinates.length - 2 : this.elemInfo[3 * (var7 + 1)] - 3;
            int var5 = this.elemInfo[3 * var7] - 1;
            var8 = new double[var4 - var5 + 2];
            int var9 = 0;

            for (var10 = this.elemInfo[3 * var7] - 1; var10 <= var4; ++var9) {
                var8[2 * var9] = this.ordinates[var10];
                var8[2 * var9 + 1] = this.ordinates[var10 + 1];
                var10 += 2;
            }

            var6[var7] = this.simplify(var8, var1);
        }

        var7 = 0;

        for (int var16 = 0; var16 < var3; ++var16) {
            if (this.elemInfo[3 * var16 + 1] != 1003 && this.elemInfo[3 * var16 + 1] != 2003) {
                var7 += var6[var16].length;
            } else if (var6[var16].length == 4) {
                var7 += 4;
            } else if (var6[var16].length == 6) {
                var7 += 4;
            } else {
                var7 += var6[var16].length;
            }
        }

        var8 = new double[var7];
        int[] var17 = new int[3 * var3];
        var10 = 1;

        int var11;
        int var12;
        for (var11 = 0; var11 < var3; ++var11) {
            if (this.elemInfo[3 * var11 + 1] != 1003 && this.elemInfo[3 * var11 + 1] != 2003) {
                var17[3 * var11] = var10;
                var17[3 * var11 + 1] = this.elemInfo[3 * var11 + 1];
                var17[3 * var11 + 2] = 1;

                for (var12 = 0; var12 < var6[var11].length; ++var12) {
                    var8[var12 + var10 - 1] = var6[var11][var12];
                }

                var10 += var6[var11].length;
            } else if (var6[var11].length == 4) {
                var17[3 * var11] = var10;
                var17[3 * var11 + 1] = 2;
                var17[3 * var11 + 2] = 1;

                for (var12 = 0; var12 < 2; ++var12) {
                    var8[var12 + var10 - 1] = var6[var11][var12];
                }

                var10 += 2;
                var8[var10 - 1] = this.ordinates[this.elemInfo[3 * var11] + 1];
                var8[var10] = this.ordinates[this.elemInfo[3 * var11] + 2];
                var10 += 2;
            } else if (var6[var11].length == 6) {
                var17[3 * var11] = var10;
                var17[3 * var11 + 1] = 2;
                var17[3 * var11 + 2] = 1;

                for (var12 = 0; var12 < 4; ++var12) {
                    var8[var12 + var10 - 1] = var6[var11][var12];
                }

                var10 += 4;
            } else {
                var17[3 * var11] = var10;
                var17[3 * var11 + 1] = this.elemInfo[3 * var11 + 1];
                var17[3 * var11 + 2] = 1;

                for (var12 = 0; var12 < var6[var11].length; ++var12) {
                    var8[var12 + var10 - 1] = var6[var11][var12];
                }

                var10 += var6[var11].length;
            }
        }

        var11 = 0;
        var12 = 0;
        int var13 = 0;

        int var14;
        for (var14 = 0; var14 < var3; ++var14) {
            if (var17[3 * var14 + 1] == 1003) {
                ++var12;
            } else if (var17[3 * var14 + 1] == 2) {
                ++var13;
            } else {
                ++var11;
            }
        }

        var14 = 0;
        if (var12 > 0) {
            ++var14;
        }

        if (var13 > 0) {
            ++var14;
        }

        if (var11 > 0) {
            ++var14;
        }

        short var15 = 2004;
        if (var14 > 1) {
            var15 = 2004;
        } else if (var12 == 1) {
            var15 = 2003;
        } else if (var12 > 1) {
            var15 = 2007;
        } else if (var13 == 1) {
            var15 = 2002;
        } else if (var13 > 1) {
            var15 = 2006;
        } else if (var11 == 1) {
            var15 = 2001;
        } else if (var11 > 1) {
            var15 = 2005;
        }

        return new JGeometry(var15, this.srid, var17, var8);
    }

    protected void finalize() {
    }

    private static double[][] translation(int var0, double var1, double var3, double var5) {
        double[][] var9 = new double[var0 + 1][var0 + 1];

        for (int var7 = 0; var7 < var0 + 1; ++var7) {
            for (int var8 = 0; var8 < var0 + 1; ++var8) {
                if (var7 == var8) {
                    var9[var7][var8] = 1.0D;
                } else {
                    var9[var7][var8] = 0.0D;
                }
            }
        }

        var9[0][var0] = var1;
        var9[1][var0] = var3;
        if (var0 == 3) {
            var9[2][var0] = var5;
        }

        return var9;
    }

    private static double[][] matrixMatrixMult(double[][] var0, double[][] var1) {
        int var2 = var0.length;
        int var3 = var0[0].length;
        int var4 = var1[0].length;
        double[][] var5 = new double[var2][var4];

        int var6;
        int var7;
        for (var6 = 0; var6 < var2; ++var6) {
            for (var7 = 0; var7 < var4; ++var7) {
                var5[var6][var7] = 0.0D;
            }
        }

        for (var6 = 0; var6 < var2; ++var6) {
            for (var7 = 0; var7 < var4; ++var7) {
                for (int var8 = 0; var8 < var3; ++var8) {
                    var5[var6][var7] += var0[var6][var8] * var1[var8][var7];
                }
            }
        }

        return var5;
    }

    private static double[] matvecMult(double[][] var0, double[] var1) {
        int var4 = var0.length;
        int var5 = var0[0].length;
        int var6 = var1.length;
        double[] var7 = new double[var4];

        int var2;
        for (var2 = 0; var2 < var4; ++var2) {
            var7[var2] = 0.0D;
        }

        for (var2 = 0; var2 < var4; ++var2) {
            for (int var3 = 0; var3 < var5; ++var3) {
                var7[var2] += var0[var2][var3] * var1[var3];
            }
        }

        return var7;
    }

    private void convertOrientedPointsFw(int[] var1, double[] var2) {
        for (int var3 = 0; var3 < var1.length; var3 += 3) {
            if (var1[var3 + 1] == 1 && var1[var3 + 2] == 0) {
                int var4 = var1[var3 + 0];
                int var5;
                if (this.getSRID() == 8307) {
                    for (var5 = 0; var5 < this.getDimensions(); ++var5) {
                        var2[var4 + var5] /= 100000.0D;
                    }
                }

                for (var5 = 0; var5 < this.getDimensions(); ++var5) {
                    var2[var4 + var5] += var2[var4 + var5 - 3];
                }
            }
        }

    }

    private void convertOrientedPointsRv(int[] var1, double[] var2) {
        for (int var3 = 0; var3 < var1.length; var3 += 3) {
            if (var1[var3 + 1] == 1 && var1[var3 + 2] == 0) {
                int var4 = var1[var3 + 0];

                for (int var5 = 0; var5 < this.getDimensions(); ++var5) {
                    var2[var4 + var5] -= var2[var4 + var5 - 3];
                }

                double var8 = Math.sqrt(var2[var4 + 0] * var2[var4 + 0] + var2[var4 + 1] * var2[var4 + 1]);

                for (int var7 = 0; var7 < this.getDimensions(); ++var7) {
                    var2[var4 + var7] /= var8;
                }
            }
        }

    }

    private double tfm_8307_to_PopularMercator_x(double var1) {
        return 0.0D + 6378137.0D * (var1 - 0.0D);
    }

    private double tfm_8307_to_PopularMercator_y(double var1, boolean var3) {
        double var4 = var3 ? MERCATOR_e54004 : MERCATOR_e3785;
        return 0.0D + 6378137.0D * Math.log(Math.tan(0.7853981633974483D + var1 / 2.0D) * Math.pow((1.0D - var4 * Math.sin(var1)) / (1.0D + var4 * Math.sin(var1)), var4 / 2.0D));
    }

    public void tfm_8307_to_PopularMercator(int[] var1, double[] var2, boolean var3) {
        if (var1 != null) {
            this.convertOrientedPointsFw(var1, var2);
        }

        if (var2 != null) {
            for (int var4 = 0; var4 < var2.length; var4 += this.getDimensions()) {
                var2[var4 + 0] = this.tfm_8307_to_PopularMercator_x(var2[var4 + 0] * 3.141592653589793D / 180.0D);
                var2[var4 + 1] = this.tfm_8307_to_PopularMercator_y(var2[var4 + 1] * 3.141592653589793D / 180.0D, var3);
            }
        }

        if (var1 != null) {
            this.convertOrientedPointsRv(var1, var2);
        }

    }

    public void tfm_8307_to_PopularMercator(boolean var1) {
        if (this.getSRID() != 8307) {
            throw new RuntimeException("Source geometry must be SRID 8307, when transforming to 3785/54004.");
        } else if (this.hasCircularArcs()) {
            throw new RuntimeException("Circular Arcs not allowed in 8307.");
        } else {
            double[] var2 = this.getPoint();
            this.tfm_8307_to_PopularMercator((int[]) null, var2, var1);
            this.x = var2[0];
            this.y = var2[1];
            this.tfm_8307_to_PopularMercator(this.getElemInfo(), this.getOrdinatesArray(), var1);
            this.setSRID(var1 ? '틴' : 3785);
        }
    }

    private double tfm_PopularMercator_to_8307_lon(double var1) {
        return (var1 - 0.0D) / 6378137.0D + 0.0D;
    }

    private double tfm_PopularMercator_to_8307_lat(double var1, boolean var3) {
        double var4 = var3 ? MERCATOR_e54004 : MERCATOR_e3785;
        return var1 + (var4 * var4 / 2.0D + Math.pow(var4, 6.0D) / 12.0D + 5.0D * Math.pow(var4, 4.0D) / 24.0D + 13.0D * Math.pow(var4, 8.0D) / 360.0D) * Math.sin(2.0D * var1)
                + (7.0D * Math.pow(var4, 4.0D) / 48.0D + 29.0D * Math.pow(var4, 6.0D) / 240.0D + 811.0D * Math.pow(var4, 8.0D) / 11520.0D) * Math.sin(4.0D * var1)
                + (7.0D * Math.pow(var4, 6.0D) / 120.0D + 81.0D * Math.pow(var4, 8.0D) / 1120.0D) * Math.sin(6.0D * var1) + 4279.0D * Math.pow(var4, 8.0D) / 161280.0D * Math.sin(8.0D * var1);
    }

    public void tfm_PopularMercator_to_8307(int[] var1, double[] var2, boolean var3) {
        if (var1 != null) {
            this.convertOrientedPointsFw(var1, var2);
        }

        if (var2 != null) {
            for (int var4 = 0; var4 < var2.length; var4 += this.getDimensions()) {
                double var5 = var2[var4 + 0];
                double var7 = var2[var4 + 1];
                double var9 = Math.pow(MERCATOR_B, (0.0D - var7) / 6378137.0D);
                double var11 = 1.5707963267948966D - 2.0D * Math.atan2(var9, 1.0D);
                var2[var4 + 1] = this.tfm_PopularMercator_to_8307_lat(var11, var3) * 180.0D / 3.141592653589793D;
                var2[var4 + 0] = this.tfm_PopularMercator_to_8307_lon(var5) * 180.0D / 3.141592653589793D;
            }
        }

        if (var1 != null) {
            this.convertOrientedPointsRv(var1, var2);
        }

    }

    public void tfm_PopularMercator_to_8307(boolean var1) {
        if (this.getSRID() != (var1 ? '틴' : 3785)) {
            throw new RuntimeException("Source geometry must be SRID 3785/54004, when transforming to 8307.");
        } else if (this.hasCircularArcs()) {
            throw new RuntimeException("Circular Arcs not allowed in 8307.");
        } else {
            double[] var2 = this.getPoint();
            this.tfm_PopularMercator_to_8307((int[]) null, var2, var1);
            this.x = var2[0];
            this.y = var2[1];
            this.tfm_PopularMercator_to_8307(this.getElemInfo(), this.getOrdinatesArray(), var1);
            this.setSRID(8307);
        }
    }

    private class Ring {

        JGeometry.Vertex startVertex;
        int numVertices;
        int numArcs;
        int numSubElements;
        ArrayList holeList = new ArrayList();

        Ring(JGeometry.Vertex var2) {
            this.startVertex = var2;
        }

        boolean pointInside(double var1, double var3) {
            boolean var5 = false;
            JGeometry.Vertex var6 = this.startVertex;

            do {
                if (var6.x < var1 != var6.next.x < var1 && (var6.y >= var3 || var6.next.y >= var3)
                        && (var6.y >= var3 && var6.next.y >= var3 || var6.y + (var1 - var6.x) * (var6.next.y - var6.y) / (var6.next.x - var6.x) >= var3)) {
                    var5 = !var5;
                }

                var6 = var6.next;
            } while (var6 != this.startVertex);

            return var5;
        }

        void addHole(JGeometry.Ring var1) {
            this.holeList.add(var1);
        }
    }

    private class ArcVertex extends JGeometry.Vertex {

        JPoint2DD center;
        boolean blackout;

        ArcVertex(double var2, double var4, double var6, double var8) {
            super(var2, var4);
            this.center = new JPoint2DD(var6, var8);
        }
    }

    private class Vertex {

        public double x;
        public double y;
        boolean isInside = false;
        boolean isIntersection = false;
        boolean isStarterIntersection = false;
        boolean isUsed = false;
        JGeometry.Vertex matchIntersection = null;
        JGeometry.Vertex prev = null;
        JGeometry.Vertex next = null;

        Vertex(double var2, double var4) {
            this.x = var2;
            this.y = var4;
        }

        void print() {
            System.out.print(this.toString());
            if (!(this instanceof JGeometry.ArcVertex)) {
                System.out.print("   ");
            }

            System.out.print(" x " + this.x + " y " + this.y + " inside " + (this.isInside ? 't' : 'f') + " intscn " + (this.isIntersection ? 't' : 'f') + " starter "
                    + (this.isStarterIntersection ? 't' : 'f') + " used " + (this.isUsed ? 't' : 'f'));
            if (this instanceof JGeometry.ArcVertex) {
                System.out.print(" blackout " + (((JGeometry.ArcVertex) this).blackout ? 't' : 'f'));
            }

            System.out.println();
            if (!this.isStarterIntersection && this.isIntersection) {
                System.out.println("match " + this.matchIntersection.toString());
            }

        }
    }

    protected static class ElementIterator {

        public int ei = 0;
        public int nextei = 0;
        public int dim = 2;
        public int gtype = 0;
        public int[] elemInfo = null;
        public double[] ordinates = null;
        public int ord_offset = 0;
        public int etype = 0;
        public int original_etype = 0;
        public int top_etype = 0;
        public int eitpr = 0;
        public int next_ord_offset = 0;
        public int nCoord = 0;
        public int nSubElement = 0;
        public boolean lastElem = false;
        public boolean isFirstElemOfCompound = false;
        public boolean isCompound = false;
        public boolean isOrientedPoint = false;
        public int orient_offset = 2;

        public ElementIterator(JGeometry var1) {
            this.gtype = var1.gtype;
            this.elemInfo = var1.elemInfo;
            this.ordinates = var1.ordinates;
            if (var1.dim > 0) {
                this.dim = var1.dim;
            }

            if (var1.isOrientedPoint() || var1.isOrientedMultiPoint()) {
                this.isOrientedPoint = true;
                if (var1.isOrientedMultiPoint()) {
                    this.orient_offset = var1.getOrientMultiPointOffset();
                }
            }

        }

        public boolean next() {
            if (this.elemInfo == null) {
                return false;
            } else {
                if (this.isFirstElemOfCompound && this.nSubElement > 0) {
                    this.isFirstElemOfCompound = false;
                }

                if (this.nSubElement > 0) {
                    --this.nSubElement;
                }

                if (this.nSubElement == 0) {
                    this.isCompound = false;
                }

                while (this.ei <= this.elemInfo.length - 3 && (this.ei != 3 || !this.isOrientedPoint)) {
                    this.etype = this.elemInfo[this.ei + 1] % 10;
                    this.original_etype = this.elemInfo[this.ei + 1];
                    if (!this.isCompound) {
                        this.top_etype = this.etype;
                    }

                    if (this.etype == 1) {
                        this.eitpr = 1;
                    } else {
                        this.eitpr = this.elemInfo[this.ei + 2];
                    }

                    if (this.etype != 0) {
                        if (this.etype >= 4) {
                            this.isFirstElemOfCompound = true;
                            this.isCompound = true;
                            this.nSubElement = this.eitpr;
                            this.ei += 3;
                            this.etype = this.elemInfo[this.ei + 1] % 10;
                            this.eitpr = this.elemInfo[this.ei + 2];
                        }

                        this.nextei = this.ei + 3;
                        this.ord_offset = this.elemInfo[this.ei] - 1;
                        this.etype = this.elemInfo[this.ei + 1];
                        if (this.etype == 1) {
                            this.eitpr = 1;
                        } else {
                            this.eitpr = this.elemInfo[this.ei + 2];
                        }

                        this.lastElem = this.nextei > this.elemInfo.length - 3 || this.nextei == 3 && this.isOrientedPoint;
                        if (this.isOrientedPoint) {
                            this.nCoord = this.ordinates.length / (this.dim + this.orient_offset);
                        } else {
                            this.next_ord_offset = this.lastElem ? this.ordinates.length : this.elemInfo[this.nextei] - 1;
                            this.nCoord = (this.next_ord_offset - this.ord_offset) / this.dim;
                        }

                        if (this.nSubElement > 1) {
                            ++this.nCoord;
                        }

                        this.ei += 3;
                        return true;
                    }

                    this.ei += 3;
                }

                return false;
            }
        }
    }

    public static class Point {

        double x;
        double y;
        double z;
        double m;

        public Point() {
            this.x = 0.0D;
            this.y = 0.0D;
        }

        public Point(double var1, double var3) {
            this.x = var1;
            this.y = var3;
        }

        public final boolean equals(JGeometry.Point var1) {
            return this.x == var1.x && this.y == var1.y;
        }

        public double getX() {
            return this.x;
        }

        public double getY() {
            return this.y;
        }

        public void set(double var1, double var3) {
            this.x = var1;
            this.y = var3;
        }
    }

    private class LT_transform {

        double ne;
        double xc;
        double yc;
        double zc;
        double[] xrow;
        double[] yrow;
        double[] zrow;

        public LT_transform(double var2, double var4, JGeometry.Gc_trans var6) {
            double var7 = Math.cos(var4);
            double var9 = Math.sin(var4);
            double var11 = Math.cos(var2);
            double var13 = Math.sin(var2);
            this.ne = var6.smax / Math.sqrt(1.0D - var6.esq * var9 * var9);
            this.xc = var7 * var11 * this.ne;
            this.yc = var7 * var13 * this.ne;
            this.zc = var9 * (1.0D - var6.esq) * this.ne;
            this.xrow = new double[3];
            this.yrow = new double[3];
            this.zrow = new double[3];
            this.xrow[0] = -var13;
            this.xrow[1] = var11;
            this.xrow[2] = 0.0D;
            this.yrow[0] = -var11 * var9;
            this.yrow[1] = -var13 * var9;
            this.yrow[2] = var7;
            this.zrow[0] = var7 * var11;
            this.zrow[1] = var7 * var13;
            this.zrow[2] = var9;
        }
    }

    private class Gc_trans {

        double smax;
        double smin;
        double flat;
        double esq;
        double e;
        double t;
        double radius;
        double area;
        double unitfactor;
        int txsrid;

        public Gc_trans(double var2, double var4) {
            this.txsrid = JGeometry.this.srid;
            this.smax = var2;
            this.flat = var4;
            this.smin = this.smax * (1.0D - this.flat);
            this.esq = this.flat * (-this.flat + 2.0D);
            this.e = Math.sqrt(this.esq);
            if (this.e < 1.0E-8D) {
                this.radius = this.smax * (1.0D - this.e / 4.0D);
            } else {
                this.radius = this.smax * Math.sqrt((2.0D * this.e + (1.0D - this.esq) * (Math.log(1.0D + this.e) - Math.log(1.0D - this.e))) / (4.0D * this.e));
            }

            this.t = Math.sqrt(1.0D - this.esq);
            this.unitfactor = 0.017453292519943295D;
            this.area = 12.566370614359172D * this.radius * this.radius;
        }
    }
}