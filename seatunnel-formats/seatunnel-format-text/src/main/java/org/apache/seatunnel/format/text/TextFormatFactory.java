/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.format.text;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.connector.DeserializationFormat;
import org.apache.seatunnel.api.table.connector.SerializationFormat;
import org.apache.seatunnel.api.table.factory.DeserializationFormatFactory;
import org.apache.seatunnel.api.table.factory.SerializationFormatFactory;
import org.apache.seatunnel.api.table.factory.TableFactoryContext;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

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

    public static TextDeserializationSchema createDefaultDeserializationFormat(
            SeaTunnelRowType typeInfo) {
        return TextDeserializationSchema.builder()
                .seaTunnelRowType(typeInfo)
                .delimiter(String.valueOf('\002'))
                .build();
    }
}
