package org.dbsyncer.listener.mysql.common.glossary.column;/*
package org.dbsyncer.listener.mysql.common.glossary.column;

import com.vividsolutions.jts.geom.Geometry;
import org.dbsyncer.listener.mysql.common.glossary.Column;

public class GeometryColumn implements Column {
    private static final long serialVersionUID = -7414553076727661079L;
    private Geometry geometry;

    public GeometryColumn(Geometry g) {
        this.geometry = g;
    }

    public Geometry getValue() {
        return this.geometry;
    }

    public static GeometryColumn valueOf(Geometry g) {
        return new GeometryColumn(g);
    }
}*/
