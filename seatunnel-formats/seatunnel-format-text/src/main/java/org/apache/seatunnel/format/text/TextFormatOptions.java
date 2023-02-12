package org.apache.seatunnel.format.text;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.util.OptionUtil;
import org.apache.seatunnel.common.utils.DateTimeUtils;
import org.apache.seatunnel.common.utils.DateUtils;
import org.apache.seatunnel.common.utils.TimeUtils;
import org.apache.seatunnel.common.utils.TimeUtils.Formatter;

import java.util.Map;

public class TextFormatOptions {

    public static final Option<String> FIELD_SEPARATOR =
            Options.key("field_separator")
                    .stringType()
                    .defaultValue(",")
                    .withDescription(
                            "Optional flag to The separator for splitting a line of strings");

    public static final Option<DateTimeUtils.Formatter> DATETIME_FORMAT =
            Options.key("date_format")
                    .objectType(DateTimeUtils.Formatter.class)
                    .defaultValue(DateTimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS)
                    .withDescription(
                            "Optional string to set the pattern of datetime type, default is yyyy-MM-dd HH:mm:ss;");

    public static final Option<DateUtils.Formatter> DATE_FORMAT =
            Options.key("date_format")
                    .objectType(DateUtils.Formatter.class)
                    .defaultValue(DateUtils.Formatter.YYYY_MM_DD)
                    .withDescription(
                            "Optional string to set the pattern of date type, default is yyyy-MM-dd;");

    public static final Option<TimeUtils.Formatter> TIME_FORMAT =
            Options.key("time_format")
                    .objectType(TimeUtils.Formatter.class)
                    .defaultValue(Formatter.HH_MM_SS)
                    .withDescription(
                            "Optional string to set the pattern of time type, default is HH:mm:ss;");

    public static String getFieldSeparator(Map<String, String> options) {
        return OptionUtil.get(FIELD_SEPARATOR, options, String::valueOf);
    }

    public static String getFieldSeparator(Config config) {
        return OptionUtil.get(FIELD_SEPARATOR, config, String::valueOf);
    }

    public static DateTimeUtils.Formatter getDateTimeFormat(Map<String, String> options) {
        return OptionUtil.get(DATETIME_FORMAT, options, DateTimeUtils.Formatter::parse);
    }

    public static DateUtils.Formatter getDateFormat(Map<String, String> options) {
        return OptionUtil.get(DATE_FORMAT, options, DateUtils.Formatter::parse);
    }

    public static TimeUtils.Formatter getTimeFormat(Map<String, String> options) {
        return OptionUtil.get(TIME_FORMAT, options, TimeUtils.Formatter::parse);
    }
}
