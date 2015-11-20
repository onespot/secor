/**
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
package com.pinterest.secor.io.impl;

import com.google.common.io.Files;
import com.pinterest.secor.common.LogFilePath;
import com.pinterest.secor.io.FileReader;
import com.pinterest.secor.io.FileWriter;
import com.pinterest.secor.io.KeyValue;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class AvroFileReaderWriterFactoryTest {
    private AvroFileReaderWriterFactory mFactory;

    public void setUp() throws Exception {
        mFactory = new AvroFileReaderWriterFactory();
    }

    @Test
    @Ignore("Integration test relies on specific server/schema in production")
    public void testSchemaReader() throws Exception {
        AvroFileReaderWriterFactory.SchemaReader schemaReader =
                new AvroFileReaderWriterFactory.SchemaReader();
        Schema schema = schemaReader.getSchemaForTopic("inbox_request");
        assertNotNull(schema);
    }

    final String JSON = "{\"integer1\": 1, \"long1\": 1000, \"string1\": \"thestring1\", \"timestamp\":\"2015-01-01 12:00:00\"}";
    final String JSON2 = "{\"integer1\": 2, \"long1\": 1001, \"string1\": \"thestring2\", \"timestamp\":\"2015-01-01 13:00:00\"}";

    @Test
    public void testReadWriteRoundTrip() throws Exception {
        AvroFileReaderWriterFactory factory = new AvroFileReaderWriterFactory();
        LogFilePath tempLogFilePath = new LogFilePath(Files.createTempDir().toString(),
                "test",
                new String[]{"part-1"},
                0,
                1,
                0,
                ".avro"
        );
        FileWriter fileWriter = factory.BuildFileWriter(tempLogFilePath, null);
        KeyValue kv1 = (new KeyValue(23232, JSON.getBytes()));
        KeyValue kv2 = (new KeyValue(23233, JSON2.getBytes()));
        fileWriter.write(kv1);
        fileWriter.write(kv2);
        fileWriter.close();

//        File file = new File(tempLogFilePath.getLogFilePath());
//        DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>();
//        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(file, datumReader);
//        GenericRecord user = null;
//        while (dataFileReader.hasNext()) {
//            user = dataFileReader.next(user);
//            System.out.println(user);
//
//        }
        FileReader fileReader = factory.BuildFileReader(tempLogFilePath, null);

        KeyValue kvout = fileReader.next();
        System.out.println(new String(kvout.getValue()));
     //   assertEquals(kv1.getKey(), kvout.getKey());
     //   assertArrayEquals(kv1.getValue(), kvout.getValue());
        kvout = fileReader.next();
        System.out.println(new String(kvout.getValue()));
     //   assertEquals(kv2.getKey(), kvout.getKey());
     //   assertArrayEquals(kv2.getValue(), kvout.getValue());
    }

    @Test
    public void testClassCastIssue() throws Exception {
        AvroFileReaderWriterFactory factory = new AvroFileReaderWriterFactory();
        LogFilePath tempLogFilePath = new LogFilePath(Files.createTempDir().toString(),
                "test",
                new String[]{"part-1"},
                0,
                1,
                0,
                ".avro"
        );
        FileWriter fileWriter = factory.BuildFileWriter(tempLogFilePath, null);
        KeyValue kv1 = (new KeyValue(23232, JSON.getBytes()));
        fileWriter.write(kv1);
        fileWriter.close();
    }
}