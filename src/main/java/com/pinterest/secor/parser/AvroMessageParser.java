/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.pinterest.secor.parser;

import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.message.Message;
import com.pinterest.secor.util.SchemaRegistryUtil;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroDecoder;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.time.Instant;

/**
 * AvroMessageParser extracts timestamp field (specified by 'message.timestamp.name')
 * from Avro data and partitions data by date.
 */
public class AvroMessageParser extends TimestampedMessageParser {

    private final KafkaAvroDecoder decoder;

    public AvroMessageParser(SecorConfig config) {
        super(config);
        decoder = new KafkaAvroDecoder(SchemaRegistryUtil.getSchemaRegistryClient());
    }

    @Override
    public long extractTimestampMillis(final Message message) throws IOException, RestClientException {
        if (message.getPayload() != null) {
            GenericRecord record = (GenericRecord) decoder.fromBytes(message.getPayload());
            if (record != null) {
                Object fieldValue = record.get(mConfig.getMessageTimestampName());
                if (fieldValue instanceof Long) {
                    // Assume already millis so can return as-is
                    return (Long)fieldValue;
                }
                return Instant.parse(fieldValue.toString()).toEpochMilli();
            }
        }
        return 0;
    }
}
