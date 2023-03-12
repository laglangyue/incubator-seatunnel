package org.apache.seatunnel.translation.spark.serialization.dataframe;

public interface ColumnConvert {

    Object convert(Object seatunnel);

    Object reconvert(Object dataFrame);

}
