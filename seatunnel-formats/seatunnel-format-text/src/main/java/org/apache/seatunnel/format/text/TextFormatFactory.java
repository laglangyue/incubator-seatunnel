package org.apache.seatunnel.format.text;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.connector.DeserializationFormat;
import org.apache.seatunnel.api.table.connector.SerializationFormat;
import org.apache.seatunnel.api.table.factory.DeserializationFormatFactory;
import org.apache.seatunnel.api.table.factory.SerializationFormatFactory;
import org.apache.seatunnel.api.table.factory.TableFactoryContext;

import java.util.Map;

public class TextFormatFactory implements DeserializationFormatFactory, SerializationFormatFactory {

    public static final String IDENTIFIER = "text";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder().build();
    }

    @Override
    public DeserializationFormat createDeserializationFormat(TableFactoryContext context) {
        Map<String, String> options = context.getOptions().toMap();
        // TODO config SeaTunnelRowType
        return () ->
                TextDeserializationSchema.builder()
                        .delimiter(TextFormatOptions.getFieldSeparator(options))
                        .dateFormatter(TextFormatOptions.getDateFormat(options))
                        .timeFormatter(TextFormatOptions.getTimeFormat(options))
                        .dateTimeFormatter(TextFormatOptions.getDateTimeFormat(options))
                        .build();
    }

    @Override
    public SerializationFormat createSerializationFormat(TableFactoryContext context) {
        Map<String, String> options = context.getOptions().toMap();
        // TODO config SeaTunnelRowType
        return () ->
                TextSerializationSchema.builder()
                        .delimiter(TextFormatOptions.getFieldSeparator(options))
                        .dateFormatter(TextFormatOptions.getDateFormat(options))
                        .timeFormatter(TextFormatOptions.getTimeFormat(options))
                        .dateTimeFormatter(TextFormatOptions.getDateTimeFormat(options))
                        .build();
    }
}
