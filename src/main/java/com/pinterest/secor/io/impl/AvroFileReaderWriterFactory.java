package com.pinterest.secor.io.impl;

import com.google.common.io.CountingOutputStream;
import com.pinterest.secor.common.LogFilePath;
import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.io.FileReader;
import com.pinterest.secor.io.FileReaderWriterFactory;
import com.pinterest.secor.io.FileWriter;
import com.pinterest.secor.io.KeyValue;
import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
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

/**
 * Created by kirwin on 8/20/15.
 */
public class AvroFileReaderWriterFactory extends FileReaderWriterFactory {

    private final SchemaReader schemaReader;

    public AvroFileReaderWriterFactory(SecorConfig config) {
        super(config);
        schemaReader = new SchemaReader(config);
    }

    @Override
    public FileReader BuildFileReader(LogFilePath logFilePath, CompressionCodec codec) throws Exception {
        Schema schema = schemaReader.getSchemaForTopic(logFilePath.getTopic());
        return new AvroReader(schema, Paths.get(logFilePath.getLogFilePath()));
    }

    @Override
    public FileWriter BuildFileWriter(LogFilePath logFilePath, CompressionCodec codec) throws Exception {
        Schema schema = schemaReader.getSchemaForTopic(logFilePath.getTopic());
        return new AvroWriter(schema, Paths.get(logFilePath.getLogFilePath()));
    }

    private static class AvroReader implements FileReader {
        DataFileReader<GenericRecord> dataFileReader;
        GenericRecord record = null;

        public AvroReader(Schema schema, Path path) throws IOException {
            DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
            dataFileReader = new DataFileReader<GenericRecord>(path.toFile(), datumReader);
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
        private Schema schema;

        AvroWriter(Schema schema, Path file) throws IOException {
            this.schema = schema;
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
            dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
            dataFileWriter.create(schema, meteredOut);
        }

        @Override
        public long getLength() throws IOException {
            return meteredOut.getCount();
        }

        @Override
        public void write(KeyValue keyValue) throws IOException {
            JSONObject jsonObject = (JSONObject) JSONValue.parse(keyValue.getValue());
            if (jsonObject != null) {
                final GenericRecord datum = convertJsonToRecord(jsonObject);
                dataFileWriter.append(datum);
            }
        }

        private GenericRecord convertJsonToRecord(JSONObject jsonObject) {
            GenericRecord record = new GenericData.Record(schema);
            for (Map.Entry<String, Object> entry : jsonObject.entrySet()) {
                record.put(entry.getKey(), entry.getValue());
            }
            return record;
        }

        @Override
        public void close() throws IOException {
            dataFileWriter.close();
        }
    }

    static class SchemaReader {
        private final String schemaRegsitryUrl;
        private final boolean useFileReader;

        public SchemaReader(SecorConfig config) {
            useFileReader = config.getBoolean("secor.avro.reader.file", true);
            if (!useFileReader) {
                int registryPort = config.getInt("secor.avro.schema.registry.port");
                String registryHost = config.getString("secor.avro.schema.registry.host");
                schemaRegsitryUrl = "http://" + registryHost + ":" + registryPort;
            } else {
                schemaRegsitryUrl = null;
            }
        }

        public final Schema getSchemaForTopic(String topic) throws IOException {
            if (useFileReader) {
                return getSchemaFromFile(topic);
            } else {
                String topicPath = "/subjects/" + topic + "/versions/latest";
                return getSchemaFromSchemaRegistry(topicPath);
            }
        }

        private Schema getSchemaFromFile(String topic) throws IOException {
            String filename = "/" + topic + ".avsc";
            InputStream schemaIn = AvroFileReaderWriterFactory.class.getResourceAsStream(filename);
            return new Schema.Parser().parse(schemaIn);
        }

        private Schema getSchemaFromSchemaRegistry(String topicPath) throws IOException {
            HttpURLConnection connection = null;
            try {
                URL url = new URL(schemaRegsitryUrl + topicPath);
                connection = (HttpURLConnection) url.openConnection();
                connection.setRequestProperty("Content-Type", "application/vnd.schemaregistry.v1+json");
                connection.setRequestProperty("Accept", "*/*");
                // Get Response.
                InputStream inputStream = connection.getInputStream();
                BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
                Map response = (Map) JSONValue.parse(reader);
                if (response.containsKey("schema")) {
                    return new Schema.Parser().parse(response.get("schema").toString());
                } else {
                    throw new RuntimeException("No schema found");
                }
            } catch (IOException exception) {
                if (connection != null) {
                    connection.disconnect();
                }
                throw exception;
            }
        }
    }
}
