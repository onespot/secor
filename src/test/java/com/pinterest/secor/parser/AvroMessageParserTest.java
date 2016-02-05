package com.pinterest.secor.parser;

import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.message.Message;
import com.pinterest.secor.util.SchemaRegistryUtil;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroEncoder;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.InputStream;

import static junit.framework.TestCase.assertEquals;

/**
 * Created by chrisreeves on 12/10/15.
 */
public class AvroMessageParserTest {

    private SecorConfig config;
    private Schema schema;
    private AvroMessageParser parser;

    @Before
    public void setup() throws IOException, RestClientException {
        config = Mockito.mock(SecorConfig.class);
        Mockito.when(config.getMessageTimestampName()).thenReturn("timestamp");

        SchemaRegistryClient client = Mockito.mock(SchemaRegistryClient.class);
        SchemaRegistryUtil.setSchemaRegistryClient(client);

        parser = new AvroMessageParser(config);


        String filename = "/test.avsc";
        InputStream schemaIn = getClass().getResourceAsStream(filename);
        schema = new Schema.Parser().parse(schemaIn);
        Mockito.when(client.getByID(0)).thenReturn(schema);
    }

    @Test
    public void testExtractTimestampMillisAsMillis() throws Exception {
        Mockito.when(config.getMessageTimestampName()).thenReturn("long1");
        KafkaAvroEncoder encoder = new KafkaAvroEncoder(SchemaRegistryUtil.getSchemaRegistryClient());
        GenericRecord record = new GenericData.Record(schema);
        long expectedTimestamp = 1448964000000L;
        record.put("timestamp", "1970-01-01T00:00:00Z");
        record.put("integer1", 1);
        record.put("long1", expectedTimestamp);
        record.put("string1", "s");

        byte[] payload = encoder.toBytes(record);
        Message message = new Message("test", 1, 0, payload);
        assertEquals(expectedTimestamp, parser.extractTimestampMillis(message));
    }

    @Test
    public void testExtractTimestampMillisAsDate() throws Exception {
        KafkaAvroEncoder encoder = new KafkaAvroEncoder(SchemaRegistryUtil.getSchemaRegistryClient());
        GenericRecord record = new GenericData.Record(schema);
        record.put("timestamp", "2015-12-01T10:00:00Z");
        record.put("integer1", 1);
        record.put("long1", 2L);
        record.put("string1", "s");

        byte[] payload = encoder.toBytes(record);
        Message message = new Message("test", 1, 0, payload);
        long expectedTimestamp = 1448964000000L;
        assertEquals(expectedTimestamp, parser.extractTimestampMillis(message));
    }

    @Test
    public void testNullPayload() throws Exception {
        long expectedTimestamp = 0L;

        Message message = new Message("test", 1, 0, null);

        assertEquals(expectedTimestamp, parser.extractTimestampMillis(message));
    }
}
