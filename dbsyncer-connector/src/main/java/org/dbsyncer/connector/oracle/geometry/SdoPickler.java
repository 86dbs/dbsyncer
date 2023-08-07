package org.dbsyncer.connector.oracle.geometry;

import java.sql.SQLException;

import oracle.sql.NUMBER;

class SdoPickler {
    private static final double[][] powerTable = new double[][]{{128.0D, 1.0E256D, 1.0E-256D}, {64.0D, 1.0E128D, 1.0E-128D}, {32.0D, 1.0E64D, 1.0E-64D}, {16.0D, 1.0E32D, 1.0E-32D}, {8.0D, 1.0E16D, 1.0E-16D}, {4.0D, 1.0E8D, 1.0E-8D}, {2.0D, 10000.0D, 1.0E-4D}, {1.0D, 100.0D, 0.01D}};

    SdoPickler() {
    }

    static final JGeometry unpickle(byte[] var0) throws SQLException {
        if (var0 != null && var0.length >= 7) {
            SdoPickler.UnpickleHelper var1 = new SdoPickler.UnpickleHelper(var0);
            var1.checkImageHeader();
            var1.skipLength();
            int var2 = getInt(var1);
            int var3 = getInt0(var1);
            double var5 = 0.0D / 0.0;
            double var7 = 0.0D / 0.0;
            double var9 = 0.0D / 0.0;
            if ((var0[var1.offset] & 255) == 253) {
                ++var1.offset;
            } else {
                var5 = getDouble0(var1);
                var7 = getDouble0(var1);
                var9 = getDouble0(var1);
                if (Double.isNaN(var5) || Double.isNaN(var7)) {
                    var5 = 0.0D / 0.0;
                    var7 = 0.0D / 0.0;
                }
            }

            int[] var11 = getElemInfoArray(var1);
            double[] var12 = getOrdinates(var1, var2);
            if (var12 != null && var11 != null) {
                return new JGeometry(var2, var3, var5, var7, var9, var11, var12);
            } else if (!Double.isNaN(var5) && !Double.isNaN(var7)) {
                return !Double.isNaN(var9) ? new JGeometry(var5, var7, var9, var3) : new JGeometry(var5, var7, var3);
            } else {
                return new JGeometry(var2, var3, var11, var12);
            }
        } else {
            throw new SQLException("Invalid geometry image");
        }
    }

    private static final int getInt0(SdoPickler.UnpickleHelper var0) throws SQLException {
        byte[] var1 = var0.image;
        int var2 = var0.offset;
        byte var3 = var1[var2];
        if (var3 <= 0) {
            ++var0.offset;
            return 0;
        } else {
            var0.offset += 1 + var3;
            if (var3 == 1 && var1[var2 + 1] == -128) {
                return 0;
            } else {
                byte[] var4 = new byte[var3];
                System.arraycopy(var1, var2 + 1, var4, 0, var3);
                return NUMBER.toInt(var4);
            }
        }
    }

    private static final int getInt(SdoPickler.UnpickleHelper var0) throws SQLException {
        byte[] var1 = var0.image;
        int var2 = var0.offset;
        byte var3 = var1[var2];
        if (var3 <= 0) {
            throw new SQLException("Invalid NULL value is found in sdo_gtype or sdo_elem_info");
        } else {
            var0.offset += 1 + var3;
            if (var3 == 1 && var1[var2 + 1] == -128) {
                return 0;
            } else {
                byte[] var4 = new byte[var3];
                System.arraycopy(var1, var2 + 1, var4, 0, var3);
                return NUMBER.toInt(var4);
            }
        }
    }

    private static final double getDouble0(SdoPickler.UnpickleHelper var0) {
        byte[] var1 = var0.image;
        int var2 = var0.offset;
        byte var3 = var1[var2];
        if (var3 <= 0) {
            ++var0.offset;
            return 0.0D / 0.0;
        } else {
            var0.offset += 1 + var3;
            if (var3 == 1 && var1[var2 + 1] == -128) {
                return 0.0D;
            } else {
                byte[] var4 = new byte[var3];
                System.arraycopy(var1, var2 + 1, var4, 0, var3);
                return NUMBER.toDouble(var4);
            }
        }
    }

    private static final double getDouble(SdoPickler.UnpickleHelper var0) throws SQLException {
        double var1 = getDouble0(var0);
        if (Double.isNaN(var1)) {
            throw new SQLException("An invalid NULL value is found in sdo_ordinates");
        } else {
            return var1;
        }
    }

    private static final int[] getElemInfoArray(SdoPickler.UnpickleHelper var0) throws SQLException {
        byte var1 = var0.readByte();
        if ((var1 & 255) == 255) {
            return null;
        } else {
            var0.skipRestOfLength(var1);
            var0.checkArrayHeader();
            var0.readByte();
            int var2 = var0.readLength();
            if (var2 < 0) {
                return null;
            } else if (var2 % 3 != 0) {
                throw new SQLException("Corrupted element info array");
            } else {
                int[] var3 = new int[var2];

                for(int var4 = 0; var4 < var2; ++var4) {
                    var3[var4] = getInt(var0);
                }

                return var3;
            }
        }
    }

    private static final double[] getOrdinates(SdoPickler.UnpickleHelper var0, int var1) throws SQLException {
        byte var2 = var0.readByte();
        if ((var2 & 255) == 255) {
            return null;
        } else {
            var0.skipRestOfLength(var2);
            var0.checkArrayHeader();
            var0.readByte();
            int var3 = var0.readLength();
            if (var3 < 0) {
                return null;
            } else {
                double[] var4 = new double[var3];
                int var5;
                if (var1 % 1000 / 100 == 0) {
                    for(var5 = 0; var5 < var3; ++var5) {
                        var4[var5] = getDouble(var0);
                    }
                } else {
                    var5 = var1 % 1000 / 100;
                    int var6 = var1 / 1000 > 0 ? var1 / 1000 : 2;
                    if (var6 == 2 || (var6 != 3 || var6 != var5) && (var6 != 4 || var5 != 3 && var5 != 4)) {
                        throw new SQLException("An invalid sdo_gtype is found");
                    }

                    for(int var7 = 0; var7 < var3; ++var7) {
                        var4[var7] = getDouble0(var0);
                        if (var4[var7] == 0.0D / 0.0 && var7 % var6 != var5 - 1) {
                            throw new SQLException("An invalid null value is found in LRS sdo_ordinates");
                        }
                    }
                }

                return var4;
            }
        }
    }

    static final byte[] pickle(JGeometry var0) throws SQLException {
        if (var0 == null) {
            throw new SQLException("Found null geometry");
        } else {
            int var1 = var0.gtype + var0.linfo * 100 + var0.dim * 1000;
            byte[] var2 = NUMBER.toBytes(var1);
            int var3 = 1 + var2.length;
            byte[] var4 = new byte[]{-1};
            int var5 = 1;
            if (var0.srid != 0) {
                var4 = NUMBER.toBytes(var0.srid);
                var5 += var4.length;
            }

            if (var0.gtype == 1 && var0.dim == 3 && Double.isNaN(var0.z)) {
                throw new SQLException("Invalid z value in the JGeometry instance.");
            } else {
                byte[] var6 = new byte[]{-3};
                byte var7 = 1;
                int var8 = 1;
                byte[] var9 = null;
                byte[] var10 = new byte[]{-1};
                if (var0.gtype == 1 && !Double.isNaN(var0.x) && !Double.isNaN(var0.y)) {
                    var7 = 2;
                    var6 = toBytes(var0.x);
                    var8 = 1 + var6.length;
                    var9 = toBytes(var0.y);
                    var8 += 2 + var9.length;
                    if (var0.dim == 3) {
                        var10 = toBytes(var0.z);
                        var7 = 3;
                        var8 += var10.length;
                    }
                }

                byte[] var11 = new byte[]{-1};
                int var12 = 1;
                int var14;
                if (var0.elemInfo != null && var0.elemInfo.length > 0) {
                    var11 = new byte[20 + var0.elemInfo.length * 22];
                    var12 = 20;

                    for(var14 = 0; var14 < var0.elemInfo.length; ++var14) {
                        byte[] var13 = NUMBER.toBytes(var0.elemInfo[var14]);
                        var11[var12] = (byte)var13.length;
                        System.arraycopy(var13, 0, var11, var12 + 1, var13.length);
                        var12 += var13.length + 1;
                    }

                    var11[5] = -120;
                    var11[6] = 1;
                    writeLength5(var12 - 5, var11, 7);
                    var11[12] = 1;
                    var11[13] = 1;
                    var11[14] = 0;
                    writeLength5(var0.elemInfo.length, var11, 15);
                    writeLength5(var12 - 5, var11, 0);
                }

                int var21 = 7 + var3 + var5 + var8 + var12;
                var14 = var0.ordinates != null && var0.ordinates.length > 0 ? 20 + var0.ordinates.length * 22 : 1;
                byte[] var15 = new byte[var21 + var14];
                var15[0] = -124;
                var15[1] = 1;
                writeLength5(var21 + var14, var15, 2);
                byte var16 = 7;
                var15[var16] = (byte)var2.length;
                System.arraycopy(var2, 0, var15, var16 + 1, var2.length);
                int var22 = var16 + var2.length + 1;
                if (var0.srid != 0) {
                    var15[var22++] = (byte)var4.length;
                }

                System.arraycopy(var4, 0, var15, var22, var4.length);
                var22 += var4.length;
                if (var7 == 1) {
                    System.arraycopy(var6, 0, var15, var22++, 1);
                } else {
                    var15[var22++] = (byte)var6.length;
                    System.arraycopy(var6, 0, var15, var22, var6.length);
                    var22 += var6.length;
                    var15[var22++] = (byte)var9.length;
                    System.arraycopy(var9, 0, var15, var22, var9.length);
                    var22 += var9.length;
                    if (var7 == 3) {
                        var15[var22++] = (byte)var10.length;
                        System.arraycopy(var10, 0, var15, var22, var10.length);
                        var22 += var10.length;
                    } else {
                        System.arraycopy(var10, 0, var15, var22++, 1);
                    }
                }

                System.arraycopy(var11, 0, var15, var22, var12);
                var22 += var12;
                int var17 = 1;
                if (var0.ordinates != null && var0.ordinates.length > 0) {
                    var17 = 20;
                    boolean var19 = false;
                    byte[] var18;
                    int var20;
                    int var23;
                    if (var0.linfo == 0) {
                        for(var20 = 0; var20 < var0.ordinates.length; ++var20) {
                            var18 = toBytes(var0.ordinates[var20]);
                            var23 = var22 + var17;
                            var15[var23] = (byte)var18.length;
                            System.arraycopy(var18, 0, var15, var23 + 1, var18.length);
                            var17 += var18.length + 1;
                        }
                    } else {
                        if (var0.dim == 2 || var0.dim == 3 && var0.linfo != 3 || var0.dim == 4 && var0.linfo != 3 && var0.linfo != 4) {
                            throw new SQLException("An invalid gtype value for LRS is found.");
                        }

                        for(var20 = 0; var20 < var0.ordinates.length; ++var20) {
                            if (Double.isNaN(var0.ordinates[var20]) && var20 % var0.dim != var0.linfo - 1) {
                                throw new SQLException("An invalid Double.NaN value is found in LRS ordinates");
                            }

                            if (Double.isNaN(var0.ordinates[var20])) {
                                var15[var22 + var17] = -1;
                                ++var17;
                            } else {
                                var18 = toBytes(var0.ordinates[var20]);
                                var23 = var22 + var17;
                                var15[var23] = (byte)var18.length;
                                System.arraycopy(var18, 0, var15, var23 + 1, var18.length);
                                var17 += var18.length + 1;
                            }
                        }
                    }

                    var15[var22 + 5] = -120;
                    var15[var22 + 6] = 1;
                    writeLength5(var17 - 5, var15, var22 + 7);
                    var15[var22 + 12] = 1;
                    var15[var22 + 13] = 1;
                    var15[var22 + 14] = 0;
                    writeLength5(var0.ordinates.length, var15, var22 + 15);
                    writeLength5(var17 - 5, var15, var22);
                } else {
                    var15[var22] = -1;
                }

                var21 += var17;
                writeLength5(var21, var15, 2);
                return var15;
            }
        }
    }

    private static void writeLength5(int var0, byte[] var1, int var2) {
        var1[var2] = -2;
        var1[var2 + 1] = (byte)(var0 >> 24);
        var0 &= 16777215;
        var1[var2 + 2] = (byte)(var0 >> 16);
        var0 &= 65535;
        var1[var2 + 3] = (byte)(var0 >> 8);
        var0 &= 255;
        var1[var2 + 4] = (byte)var0;
    }

    private static byte[] toBytes(double var0) throws SQLException {
        byte[] var2 = new byte[20];
        int var3 = 0;
        boolean var4 = false;
        boolean var15 = false;
        boolean var16 = false;
        boolean var17 = false;
        boolean var20 = false;
        boolean var21 = false;
        boolean var5 = var0 >= 0.0D;
        var0 = Math.abs(var0);
        String var18 = Double.toString(var0);
        int var24 = var18.length();
        int var25 = var18.indexOf(69);
        byte var6;
        int var8;
        int var9;
        int var10;
        if (var25 != -1) {
            if (var25 == var25 / 2 * 2) {
                var4 = true;
            }

            var3 = Integer.valueOf(var18.substring(var25 + 1, var24));
            if (var3 != var3 / 2 * 2) {
                var9 = var3 / 2 + 1;
            } else {
                var9 = var3 / 2;
            }

            if (var9 > 62) {
                throw new SQLException("Overflow");
            }

            if (var9 < -65) {
                throw new SQLException("Underflow");
            }

            if (var3 != var3 / 2 * 2) {
                ++var3;
                var17 = true;
                var4 = !var4;
            }

            var6 = var4 ? (byte)(var25 / 2) : (byte)((var25 + 1) / 2);
            if (var17) {
                var2[0] = (byte)Integer.parseInt(var18.substring(0, 1) + var18.substring(2, 3));
                var9 = 3;
            } else {
                var2[0] = (byte)Integer.parseInt(var18.substring(0, 1));
                var9 = 2;
            }

            if (var25 <= 3) {
                if (var6 == 2) {
                    var2[1] = (byte)(Integer.parseInt(var18.substring(2, 3)) * 10);
                }
            } else {
                for(var8 = 1; var8 < var6 - 1; ++var8) {
                    var2[var8] = (byte)Integer.parseInt(var18.substring(var9, var9 + 2));
                    var9 += 2;
                }

                if (var4) {
                    var2[var6 - 1] = (byte)Integer.parseInt(var18.substring(var25 - 2, var25));
                } else {
                    var2[var6 - 1] = (byte)(Integer.parseInt(var18.substring(var25 - 1, var25)) * 10);
                }
            }

            var3 /= 2;

            for(var8 = var6 - 1; var8 != 0 && var2[var8] == 0; --var8) {
                --var6;
            }
        } else if (var24 >= 17) {
            if (var0 >= 1.0D) {
                for(var9 = 0; var9 < 8; ++var9) {
                    if (powerTable[var9][1] <= var0) {
                        var3 += (int)powerTable[var9][0];
                        var0 *= powerTable[var9][2];
                    }
                }
            } else {
                for(var9 = 0; var9 < 8; ++var9) {
                    if (powerTable[var9][2] >= var0) {
                        var3 -= (int)powerTable[var9][0];
                        var0 *= powerTable[var9][1];
                    }
                }

                if (var0 < 1.0D) {
                    --var3;
                    var0 *= 100.0D;
                }
            }

            if (var3 > 62) {
                throw new SQLException("Overflow");
            }

            if (var3 < -65) {
                throw new SQLException("Underflow");
            }

            boolean var22 = var0 >= 10.0D;
            var10 = var18.indexOf(46);
            if (var10 == -1) {
                throw new SQLException("Invalid double value found: " + var18);
            }

            var9 = var24 - var10 - 1;
            var4 = var9 == var9 / 2 * 2;
            StringBuffer var23 = new StringBuffer(20);

            for(var9 = 0; var9 < var24 && (var9 == var10 || var18.charAt(var9) == '0'); ++var9) {
            }

            for(; var9 < var24; ++var9) {
                if (var9 != var10) {
                    var23.append(var18.charAt(var9));
                }
            }

            String var19 = var23.toString();
            var25 = var19.length();
            var6 = (byte)((var25 + 1) / 2);
            if (var25 == 1) {
                if (var22) {
                    var2[0] = (byte)(Integer.parseInt(var19.substring(0, 1)) * 10);
                } else {
                    var2[0] = (byte)Integer.parseInt(var19.substring(0, 1));
                }
            } else if (var25 == 2) {
                if (var22) {
                    var2[0] = (byte)Integer.parseInt(var19.substring(0, 2));
                } else if (var19.charAt(1) == '0') {
                    var2[0] = (byte)Integer.parseInt(var19.substring(0, 1));
                } else {
                    ++var6;
                    var2[0] = (byte)Integer.parseInt(var19.substring(0, 1));
                    var2[1] = (byte)(Integer.parseInt(var19.substring(1, 2)) * 10);
                }
            } else {
                if (!var22 && var25 == var25 / 2 * 2) {
                    ++var6;
                }

                if (var22) {
                    var2[0] = (byte)Integer.parseInt(var19.substring(0, 2));
                    var9 = 2;
                } else {
                    var2[0] = (byte)Integer.parseInt(var18.substring(0, 1));
                    var9 = 1;
                }

                for(var8 = 1; var8 < var6 - 1; ++var8) {
                    var2[var8] = (byte)Integer.parseInt(var19.substring(var9, var9 + 2));
                    var9 += 2;
                }

                if (var4) {
                    var2[var6 - 1] = (byte)Integer.parseInt(var18.substring(var24 - 2, var24));
                } else {
                    var2[var6 - 1] = (byte)(Integer.parseInt(var18.substring(var24 - 1, var24)) * 10);
                }

                for(var8 = var6 - 1; var8 != 0 && var2[var8] == 0; --var8) {
                    --var6;
                }
            }
        } else {
            if (var0 >= 1.0D) {
                for(var9 = 0; var9 < 8; ++var9) {
                    if (powerTable[var9][1] <= var0) {
                        var3 += (int)powerTable[var9][0];
                        var0 *= powerTable[var9][2];
                    }
                }
            } else {
                for(var9 = 0; var9 < 8; ++var9) {
                    if (powerTable[var9][2] >= var0) {
                        var3 -= (int)powerTable[var9][0];
                        var0 *= powerTable[var9][1];
                    }
                }

                if (var0 < 1.0D) {
                    --var3;
                    var0 *= 100.0D;
                }
            }

            if (var3 != 256 && var3 > 62) {
                System.out.println(" exponent = " + var3 + " lnxexpmx - lNXEXPBS -1 = " + 62);
                throw new SQLException("Overflow");
            }

            if (var3 != -256 && var3 < -65) {
                System.out.println(" value = " + var0 + " exponent = " + var3 + " LNXEXPMN - LNXEXPBS -1 = " + -65);
                throw new SQLException("Underflow");
            }

            var4 = var0 < 10.0D;
            var6 = 8;
            var8 = 0;

            byte var7;
            for(var7 = (byte)((int)var0); var8 < var6; ++var8) {
                var2[var8] = var7;
                var0 = (var0 - (double)var7) * 100.0D;
                var7 = (byte)((int)var0);
            }

            var8 = 7;
            if (var4) {
                if (var7 >= 50) {
                    ++var2[var8];
                }
            } else if (var3 == 62 && (var2[var8] + 5) / 10 * 10 == 100) {
                var2[var8] = (byte)((var2[var8] - 5) / 10 * 10);
            } else {
                var2[var8] = (byte)((var2[var8] + 5) / 10 * 10);
            }

            while(var2[var8] == 100) {
                if (var8 == 0) {
                    ++var3;
                    var2[var8] = 1;
                    break;
                }

                var2[var8] = 0;
                --var8;
                ++var2[var8];
            }

            for(var8 = 7; var8 != 0 && var2[var8] == 0; --var8) {
                --var6;
            }
        }

        byte[] var26;
        if (var5) {
            var10 = var6 + 1;
            var26 = new byte[var10];
            var26[0] = (byte)(var3 + 128 + 64 + 1);
            if (var17) {
                --var26[0];
            }

            for(var9 = 1; var9 < var10; ++var9) {
                var26[var9] = (byte)(var2[var9 - 1] + 1);
            }

            return var26;
        } else {
            if (var6 < 20) {
                var10 = var6 + 2;
            } else {
                var10 = var6 + 1;
            }

            var26 = new byte[var10];
            var26[0] = (byte)(~(var3 + 128 + 64 + 1));
            if (var17) {
                ++var26[0];
            }

            for(var9 = 1; var9 < var10; ++var9) {
                var26[var9] = (byte)(101 - var2[var9 - 1]);
            }

            if (var6 < 20) {
                var26[var10 - 1] = 102;
            }

            return var26;
        }
    }

    private static final class UnpickleHelper {
        byte[] image = null;
        int offset = 0;

        protected UnpickleHelper(byte[] var1) {
            this.image = var1;
            this.offset = 0;
        }

        protected UnpickleHelper(byte[] var1, int var2) {
            this.image = var1;
            this.offset = var2;
        }

        protected int getOffset() {
            return this.offset;
        }

        protected byte[] getImage() {
            return this.image;
        }

        protected byte readByte() {
            byte var1 = this.image[this.offset];
            ++this.offset;
            return var1;
        }

        protected byte[] readBytes(int var1) {
            byte[] var2 = new byte[var1];
            System.arraycopy(this.image, this.offset, var2, 0, var1);
            this.offset += var1;
            return var2;
        }

        protected final void checkImageHeader() throws SQLException {
            byte var1 = this.image[0];
            if ((var1 & 255 & 128) == 0) {
                throw new SQLException("Image is not in 8.1 format");
            } else if ((var1 & 255 & 8) != 0) {
                throw new SQLException("Image is a collection image, expecting ADT");
            } else {
                byte var2 = this.image[1];
                if ((var2 & 255) > 1) {
                    throw new SQLException("Image version is not recognized");
                } else {
                    this.offset += 2;
                }
            }
        }

        protected final byte[] readDataValue() {
            int var1 = this.image[this.offset] & 255;
            if (var1 == 255) {
                ++this.offset;
                return null;
            } else {
                if (var1 > 245) {
                    var1 = (((this.image[this.offset + 1] & 255) * 256 + (this.image[this.offset + 2] & 255)) * 256 + (this.image[this.offset + 3] & 255)) * 256 + (this.image[this.offset + 4] & 255);
                    this.offset += 5;
                } else {
                    ++this.offset;
                }

                byte[] var2 = new byte[var1];
                System.arraycopy(this.image, this.offset, var2, 0, var2.length);
                this.offset += var2.length;
                return var2;
            }
        }

        protected byte[] readDataValue(int var1) {
            if (var1 == 0) {
                return null;
            } else {
                byte[] var2 = new byte[var1];
                System.arraycopy(this.image, this.offset, var2, 0, var1);
                this.offset += var1;
                return var2;
            }
        }

        protected boolean isElementNull(byte var1) {
            return (var1 & 255) == 255;
        }

        protected final void skipLength() {
            int var1 = this.image[this.offset] & 255;
            if (var1 > 245) {
                this.offset += 5;
            } else {
                ++this.offset;
            }

        }

        protected final void skipTo(int var1) {
            if (var1 > this.offset) {
                this.offset = var1;
            }

        }

        protected final int readLength() {
            int var1 = this.image[this.offset] & 255;
            if (var1 > 245) {
                if (var1 == 255) {
                    return -1;
                }

                var1 = (((this.image[this.offset + 1] & 255) * 256 + (this.image[this.offset + 2] & 255)) * 256 + (this.image[this.offset + 3] & 255)) * 256 + (this.image[this.offset + 4] & 255);
                this.offset += 5;
            } else {
                ++this.offset;
            }

            return var1;
        }

        protected final void skipRestOfLength(byte var1) throws SQLException {
            if ((var1 & 255) > 245) {
                if ((var1 & 255) != 254) {
                    throw new SQLException("Invalid first length byte");
                }

                this.offset += 4;
            }

        }

        protected final void checkArrayHeader() throws SQLException {
            byte var1 = this.readByte();
            if ((var1 & 255 & 4) != 0) {
                throw new SQLException("Array image has no prefix segment");
            } else {
                boolean var2 = true;
                if ((var1 & 255 & 8) != 0) {
                    var2 = true;
                } else {
                    if ((var1 & 255 & 16) == 0) {
                        throw new SQLException("Image is not a collection image");
                    }

                    var2 = false;
                }

                this.readByte();
                long var3 = (long)this.readLength();
                int var5 = this.readLength();
                byte var6 = this.readByte();
                this.readDataValue(var5 - 1);
            }
        }
    }
}