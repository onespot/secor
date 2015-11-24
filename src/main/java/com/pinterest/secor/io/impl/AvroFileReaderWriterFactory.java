package com.pinterest.secor.io.impl;

import com.google.common.io.CountingOutputStream;
import com.pinterest.secor.common.LogFilePath;
import com.pinterest.secor.io.FileReader;
import com.pinterest.secor.io.FileReaderWriterFactory;
import com.pinterest.secor.io.FileWriter;
import com.pinterest.secor.io.KeyValue;
import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.*;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Encoder;
import org.apache.hadoop.io.compress.CompressionCodec;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.function.Function;

/**
 * Created by kirwin on 8/20/15.
 */
public class AvroFileReaderWriterFactory implements FileReaderWriterFactory {

    public static final String SCHEMA_REGISTRY_HOST_PROPERTY = "com.onespot.secor.schema.registryHost";
    public static final String SCHEMA_REGISTRY_PORT_PROPERTY = "com.onespot.secor.schema.registryPort";
    public static final String DEFAULT_SCHEMA_REGISTRY_PORT = "8081";

    private final SchemaLoader schemaLoader;

    public AvroFileReaderWriterFactory() {
        this(createSchemaLoader());
    }

    public AvroFileReaderWriterFactory(SchemaLoader schemaLoader) {
        this.schemaLoader = schemaLoader;
    }

    @Override
    public FileReader BuildFileReader(LogFilePath logFilePath, CompressionCodec codec) throws Exception {
        Schema schema = getTopicSchema(logFilePath.getTopic());
        return new AvroReader(schema, Paths.get(logFilePath.getLogFilePath()));
    }

    @Override
    public FileWriter BuildFileWriter(LogFilePath logFilePath, CompressionCodec codec) throws Exception {
        Schema schema = getTopicSchema(logFilePath.getTopic());
        return new AvroWriter(schema, Paths.get(logFilePath.getLogFilePath()), new FromJsonExtractor());
    }

    private Schema getTopicSchema(String topic) throws Exception {
        try {
            return schemaLoader.getSchemaForTopic(topic);
        }
        catch (Exception e) {
            throw new Exception("Failed to get schema for topic " + topic, e);
        }
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
        private final MessageExtractor extractor;

        AvroWriter(Schema schema, Path file, MessageExtractor extractor) throws IOException {
            this.schema = schema;
            this.extractor = extractor;
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
            final GenericRecord datum = extractor.extract(keyValue.getValue(), schema);
            if (datum != null) {
                dataFileWriter.append(datum);
            }
        }

        @Override
        public void close() throws IOException {
            dataFileWriter.close();
        }
    }

    public interface SchemaLoader {
        Schema getSchemaForTopic(String topic) throws Exception;
    }

    private static class ClasspathSchemaLoader implements SchemaLoader {
        @Override
        public Schema getSchemaForTopic(String topic) throws IOException {
            String filename = "/" + topic + ".avsc";
            InputStream schemaIn = AvroFileReaderWriterFactory.class.getResourceAsStream(filename);
            return new Schema.Parser().parse(schemaIn);
        }
    }

    static class RegistrySchemaLoader implements SchemaLoader {

        private final String registryHost;
        private final int registryPort;

        RegistrySchemaLoader(String registryHost, int registryPort) {
            this.registryHost = registryHost;
            this.registryPort = registryPort;
        }

        @Override
        public Schema getSchemaForTopic(String topic) throws IOException {
            // We always should be using the latest schema as all pending
            // messages in Kafka can be converted to this version.
            HttpURLConnection connection = buildRegistryRequest("/subjects/" + topic + "/versions/latest");
            try (InputStream in = connection.getInputStream()) {
                return ((Function<InputStream, Schema>) this::extractSchemaFromResponse).apply(in);
            }
            finally {
                connection.disconnect();
            }
        }

        private HttpURLConnection buildRegistryRequest(String requestPath) throws IOException {
            URL requestUrl = new URL("http", registryHost, registryPort, requestPath);
            HttpURLConnection connection = (HttpURLConnection) requestUrl.openConnection();
            connection.setRequestProperty("Content-Type", "application/vnd.schemaregistry.v1+json");
            connection.setRequestProperty("Accept", "*/*");
            return connection;
        }

        private Schema extractSchemaFromResponse(InputStream in) {
            JSONObject response = (JSONObject) JSONValue.parse(new BufferedInputStream(in));
            if (!response.containsKey("schema")) {
                throw new RuntimeException("No schema found in response");
            }

            return new Schema.Parser().parse(response.get("schema").toString());
        }
    }

    private static SchemaLoader createSchemaLoader() {
        String registryHost = System.getProperty(SCHEMA_REGISTRY_HOST_PROPERTY);
        if (registryHost != null) {
            String registryPort = System.getProperty(SCHEMA_REGISTRY_PORT_PROPERTY, DEFAULT_SCHEMA_REGISTRY_PORT);
            try {
                return new RegistrySchemaLoader(registryHost, Integer.parseInt(registryPort));
            }
            catch (NumberFormatException e) {
                throw new RuntimeException("Invalid port provided for schema registry", e);
            }
        }

        return new ClasspathSchemaLoader();
    }

    public interface MessageExtractor {
        GenericRecord extract(byte[] message, Schema storageSchema);
    }

    private static class FromJsonExtractor implements MessageExtractor {

        @Override
        public GenericRecord extract(final byte[] message, final Schema storageSchema) {
            final JSONObject jsonObject = (JSONObject) JSONValue.parse(message);
            if (jsonObject != null) {
                return convertJsonToRecord(jsonObject, storageSchema);
            }
            return null;
        }

        // We are writing using the latest schema, but during switchover, there may be messages
        // in the topic written w/o new required fields.  Therefore, need to ensure
        // we populate missing fields w/ defaults.  Do so by using the GenericRecordBuilder.

        private GenericRecord convertJsonToRecord(final JSONObject jsonObject, final Schema schema) {
            GenericRecordBuilder record = new GenericRecordBuilder(schema);
            for (Map.Entry<String, Object> entry : jsonObject.entrySet()) {
                record.set(entry.getKey(), entry.getValue());
            }
            return record.build();
        }
    }

}
