/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
import org.junit.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ByteOrderValues;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKBWriter;
import org.locationtech.jts.io.WKTReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

/**
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2025-01-04 13:22
 */
public class GeometryTest {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Test
    public void testGeometry() throws Exception {
        // POINT (121.474103 31.232862)
        byte[] point = new byte[]{0, 0, 0, 0, 1, 1, 0, 0, 0, -33, -5, 27, -76, 87, 94, 94, 64, 45, 123, 18, -40, -100, 59, 63, 64};
        testGeometry("point", point);

        // LINESTRING (121.474103 31.232862, 121.472462 31.231339, 121.471984 31.232821)
        byte[] linestring = new byte[]{0, 0, 0, 0, 1, 2, 0, 0, 0, 3, 0, 0, 0, -33, -5, 27, -76, 87, 94, 94, 64, 45, 123, 18, -40, -100, 59, 63, 64, -109, -90, 65, -47, 60, 94, 94, 64, 18, 74, 95, 8,
                57, 59, 63, 64, 15, 15, 97, -4, 52, 94, 94, 64, 112, -46, 52, 40, -102, 59, 63, 64};
        testGeometry("linestring", linestring);

        // POLYGON ((121.474243 31.234504, 121.471775 31.233348, 121.470724 31.23155, 121.471603 31.230229, 121.472655 31.230357, 121.475777 31.232045, 121.474243 31.234504))
        byte[] polygon = new byte[]{0, 0, 0, 0, 1, 3, 0, 0, 0, 1, 0, 0, 0, 7, 0, 0, 0, -40, -42, 79, -1, 89, 94, 94, 64, -4, -57, 66, 116, 8, 60, 63, 64, -127, 4, -59, -113, 49, 94, 94, 64, 70, -106,
                -52, -79, -68, 59, 63, 64, 79, 92, -114, 87, 32, 94, 94, 64, -120, 99, 93, -36, 70, 59, 63, 64, -125, 108, 89, -66, 46, 94, 94, 64, 62, -105, -87, 73, -16, 58, 63, 64, -98, -46, -63,
                -6, 63, 94, 94, 64, -103, 103, 37, -83, -8, 58, 63, 64, 24, -52, 95, 33, 115, 94, 94, 64, 72, 51, 22, 77, 103, 59, 63, 64, -40, -42, 79, -1, 89, 94, 94, 64, -4, -57, 66, 116, 8, 60,
                63, 64};
        testGeometry("polygon", polygon);

        // MULTIPOINT ((103 35), (104 34), (105 35))
        byte[] multipoint = new byte[]{0, 0, 0, 0, 1, 4, 0, 0, 0, 3, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, -64, 89, 64, 0, 0, 0, 0, 0, -128, 65, 64, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 90, 64, 0, 0, 0,
                0, 0, 0, 65, 64, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 64, 90, 64, 0, 0, 0, 0, 0, -128, 65, 64};
        testGeometry("multipoint", multipoint);

        // MULTILINESTRING ((103 35, 104 35), (105 36, 105 37))
        byte[] multilinestring = new byte[]{0, 0, 0, 0, 1, 5, 0, 0, 0, 2, 0, 0, 0, 1, 2, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, -64, 89, 64, 0, 0, 0, 0, 0, -128, 65, 64, 0, 0, 0, 0, 0, 0, 90, 64, 0, 0,
                0, 0, 0, -128, 65, 64, 1, 2, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 64, 90, 64, 0, 0, 0, 0, 0, 0, 66, 64, 0, 0, 0, 0, 0, 64, 90, 64, 0, 0, 0, 0, 0, -128, 66, 64};
        testGeometry("multilinestring", multilinestring);

        // MULTIPOLYGON (((103 35, 104 35, 104 36, 103 36, 103 35)), ((103 36, 104 36, 104 37, 103 36)))
        byte[] multipolygon = new byte[]{0, 0, 0, 0, 1, 6, 0, 0, 0, 2, 0, 0, 0, 1, 3, 0, 0, 0, 1, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 0, -64, 89, 64, 0, 0, 0, 0, 0, -128, 65, 64, 0, 0, 0, 0, 0, 0, 90,
                64, 0, 0, 0, 0, 0, -128, 65, 64, 0, 0, 0, 0, 0, 0, 90, 64, 0, 0, 0, 0, 0, 0, 66, 64, 0, 0, 0, 0, 0, -64, 89, 64, 0, 0, 0, 0, 0, 0, 66, 64, 0, 0, 0, 0, 0, -64, 89, 64, 0, 0, 0, 0, 0,
                -128, 65, 64, 1, 3, 0, 0, 0, 1, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, -64, 89, 64, 0, 0, 0, 0, 0, 0, 66, 64, 0, 0, 0, 0, 0, 0, 90, 64, 0, 0, 0, 0, 0, 0, 66, 64, 0, 0, 0, 0, 0, 0, 90, 64,
                0, 0, 0, 0, 0, -128, 66, 64, 0, 0, 0, 0, 0, -64, 89, 64, 0, 0, 0, 0, 0, 0, 66, 64};
        testGeometry("multipolygon", multipolygon);

        // GEOMETRYCOLLECTION (POINT (103 35), LINESTRING (103 35, 103 37))
        byte[] geometrycollection = new byte[]{0, 0, 0, 0, 1, 7, 0, 0, 0, 2, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, -64, 89, 64, 0, 0, 0, 0, 0, -128, 65, 64, 1, 2, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0,
                -64, 89, 64, 0, 0, 0, 0, 0, -128, 65, 64, 0, 0, 0, 0, 0, -64, 89, 64, 0, 0, 0, 0, 0, -128, 66, 64};
        testGeometry("geometrycollection", geometrycollection);
    }

    private void testGeometry(String typeName, byte[] bytes) throws ParseException {
        // 序列化
        byte[] geometryBytes = ByteBuffer.allocate(bytes.length - 4).order(ByteOrder.LITTLE_ENDIAN).put(bytes, 4, bytes.length - 4).array();
        WKBReader reader = new WKBReader();
        String text = reader.read(geometryBytes).toText();
        logger.info("test {}:{}", typeName, text);

        // 反序列化
        Geometry geometry = new WKTReader().read(text);
        byte[] bytesArray = new WKBWriter(2, ByteOrderValues.LITTLE_ENDIAN).write(geometry);
        byte[] array = ByteBuffer.allocate(bytesArray.length + 4).order(ByteOrder.LITTLE_ENDIAN).putInt(geometry.getSRID()).put(bytesArray).array();
        logger.info(Arrays.toString(array));
        logger.info("test {}:{}", typeName, Arrays.equals(bytes, array));
        assert Arrays.equals(bytes, array);
    }
}
