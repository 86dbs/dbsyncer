/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.dbsyncer.listener.mysql.binlog.impl.event;

import org.dbsyncer.listener.mysql.binlog.BinlogEventV4Header;
import org.dbsyncer.listener.mysql.common.glossary.UnsignedLong;
import org.dbsyncer.listener.mysql.common.util.MySQLConstants;
import org.dbsyncer.listener.mysql.common.util.ToStringBuilder;

/**
 * Written every time a statement uses an AUTO_INCREMENT column or the LAST_INSERT_ID() function;
 * precedes other events for the statement. This is written only before a QUERY_EVENT and is not
 * used with row-based logging. An INTVAR_EVENT is written with a "subtype" in the event data part:
 * <ol type="1">
 * 	<li><dt>INSERT_ID_EVENT indicates the value to use for an AUTO_INCREMENT column in the next statement.</dt></li>
 * 	<li><dt>LAST_INSERT_ID_EVENT indicates the value to use for the LAST_INSERT_ID() function in the next statement.</dt></li>
 * </ol>
 * @ClassName: IntvarEvent
 * @author: AE86
 * @date: 2018年10月17日 下午2:27:13
 */
public final class IntvarEvent extends AbstractBinlogEventV4 {
    //
    public static final int EVENT_TYPE = MySQLConstants.INTVAR_EVENT;

    //
    private int type;
    private UnsignedLong value;

    /**
     *
     */
    public IntvarEvent() {
    }

    public IntvarEvent(BinlogEventV4Header header) {
        this.header = header;
    }

    /**
     *
     */
    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("header", header)
                .append("type", type)
                .append("value", value).toString();
    }

    /**
     *
     */
    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public UnsignedLong getValue() {
        return value;
    }

    public void setValue(UnsignedLong value) {
        this.value = value;
    }
}
