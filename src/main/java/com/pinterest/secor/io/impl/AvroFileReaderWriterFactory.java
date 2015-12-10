package com.pinterest.secor.io.impl;

import com.google.common.io.CountingOutputStream;
import com.pinterest.secor.common.LogFilePath;
import com.pinterest.secor.io.FileReader;
import com.pinterest.secor.io.FileReaderWriterFactory;
import com.pinterest.secor.io.FileWriter;
import com.pinterest.secor.io.KeyValue;
import com.pinterest.secor.util.SchemaRegistryUtil;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroDecoder;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Encoder;
import org.apache.hadoop.io.compress.CompressionCodec;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created by kirwin on 8/20/15.
 */
public class AvroFileReaderWriterFactory implements FileReaderWriterFactory {

    private final SchemaRegistryClient schemaRegistryClient;

    public AvroFileReaderWriterFactory() {
        schemaRegistryClient = SchemaRegistryUtil.getSchemaRegistryClient();
    }

    @Override
    public FileReader BuildFileReader(LogFilePath logFilePath, CompressionCodec codec) throws Exception {
        Schema schema = getSchemaForTopic(logFilePath.getTopic());
        return new AvroReader(schema, Paths.get(logFilePath.getLogFilePath()));
    }

    @Override
    public FileWriter BuildFileWriter(LogFilePath logFilePath, CompressionCodec codec) throws Exception {
        Schema schema = getSchemaForTopic(logFilePath.getTopic());
        return new AvroWriter(schema, Paths.get(logFilePath.getLogFilePath()),
                new KafkaAvroDecoder(schemaRegistryClient));
    }

    public Schema getSchemaForTopic(String topic) throws IOException, RestClientException {
        return extractSchemaFromResponse(schemaRegistryClient
                .getLatestSchemaMetadata(topic).getSchema());
    }

    private Schema extractSchemaFromResponse(String schema) {
        return new Schema.Parser().parse(schema);
    }

    private static class AvroReader implements FileReader {
        DataFileReader<GenericRecord> dataFileReader;
        GenericRecord record = null;

        public AvroReader(Schema schema, Path path) throws IOException {
            DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
            dataFileReader = new DataFileReader<>(path.toFile(), datumReader);
        }

        @Override
        public KeyValue next() throws IOException {
            if (dataFileReader.hasNext()) {
                record = dataFileReader.next(record);
                return new KeyValue(0, record.toString().getBytes());
            }
            return null;
        }

        @Override
        public void close() throws IOException {
            dataFileReader.close();
        }
    }

    private static class AvroWriter implements FileWriter {
        private final CountingOutputStream meteredOut;
        DataFileWriter<GenericRecord> dataFileWriter;
        private final Schema schema;
        private final KafkaAvroDecoder decoder;

        AvroWriter(Schema schema, Path file, KafkaAvroDecoder decoder) throws IOException {
            this.schema = schema;
            this.decoder = decoder;

            /**
             * We override the write method of the GenericDatumWriter as it doesnt support casting of ints to longs
             * for some reason
             */
            final GenericDatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema) {
                /** Called to write data.*/
                protected void write(Schema schema, Object datum, Encoder out)
                        throws IOException {
                    try {
                        switch (schema.getType()) {

                            case LONG:
                                out.writeLong(((Number) datum).longValue());
                                break;
                            case DOUBLE:
                                out.writeDouble(((Number) datum).doubleValue());
                                break;
                            case FLOAT:
                                out.writeFloat(((Number) datum).floatValue());
                                break;
                            default:
                                super.write(schema, datum, out);
                        }
                    } catch (NullPointerException e) {
                        throw npe(e, " of " + schema.getFullName());
                    }
                }
            };
            Files.createDirectories(file.getParent());
            final OutputStream out = Files.newOutputStream(file);
            meteredOut = new CountingOutputStream(out);
            dataFileWriter = new DataFileWriter<>(datumWriter);
            dataFileWriter.create(schema, meteredOut);
        }

        @Override
        public long getLength() throws IOException {
            return meteredOut.getCount();
        }

        @Override
        public void write(KeyValue keyValue) throws IOException {
            GenericRecord toConvert = (GenericRecord) decoder.fromBytes(keyValue.getValue());
            final GenericRecord datum = convertToRecord(toConvert, schema);
            if (datum != null) {
                dataFileWriter.append(datum);
            }
        }

        private GenericRecord convertToRecord(final GenericRecord toConvert, final Schema schema) {
            GenericRecordBuilder record = new GenericRecordBuilder(schema);
            for (Schema.Field field : schema.getFields()) {
                record.set(field.name(), toConvert.get(field.name()));
            }
            return record.build();
        }

        @Override
        public void close() throws IOException {
            dataFileWriter.close();
        }
    }
}
