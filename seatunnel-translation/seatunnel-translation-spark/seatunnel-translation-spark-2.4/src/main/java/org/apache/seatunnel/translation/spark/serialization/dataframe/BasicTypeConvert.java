package org.apache.seatunnel.translation.spark.serialization.dataframe;

import org.apache.seatunnel.api.table.type.SqlType;

public enum BasicTypeConvert implements ColumnConvert {
    NotConvert(null) {
    };
    private SqlType sqlType;

    BasicTypeConvert(SqlType sqlType) {
        this.sqlType = sqlType;
    }


    @Override
    public Object convert(Object seatunnel) {
        return seatunnel;
    }

    @Override
    public Object reconvert(Object dataFrame) {
        return dataFrame;
    }
}
