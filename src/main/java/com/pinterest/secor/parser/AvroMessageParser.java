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
import com.pinterest.secor.message.ParsedMessage;
import com.pinterest.secor.util.SchemaRegistryUtil;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroDecoder;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.errors.SerializationException;

import java.io.IOException;
import java.time.Instant;

/**
 * AvroMessageParser extracts timestamp field (specified by 'message.timestamp.name')
 * from Avro data and partitions data by date.
 */
public class AvroMessageParser extends TimestampedMessageParser {

    private static final int SCHEMA_NOT_FOUND = 40403;
    private static final boolean ignoreMissingSchema = Boolean.getBoolean("parser.ignore.missingSchemas");

    private final KafkaAvroDecoder decoder;

    public AvroMessageParser(SecorConfig config) {
        super(config);
        decoder = new KafkaAvroDecoder(SchemaRegistryUtil.getSchemaRegistryClient());
    }

    @Override
    public ParsedMessage parse(Message message) throws Exception {
        try {
            return super.parse(message);
        }
        catch (SerializationException e) {
            if (ignoreParseError(e)) {
                return null;
            }
            throw e;
        }
    }

    private static final boolean ignoreParseError(SerializationException e) {
        Throwable rootCause = e.getCause();
        if (rootCause instanceof RestClientException) {
            RestClientException cause = (RestClientException)rootCause;
            if (ignoreMissingSchema && cause.getErrorCode() == SCHEMA_NOT_FOUND) {
                // If we couldn't find the schema, it is unlikely we'll ever be
                // able to successfully decode this message.
                // Just discard so we can't get stuck on the message.
                return true;
            }
        }
        return false;
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
