package com.pinterest.secor.util;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;

/**
 * Created by chrisreeves on 12/10/15.
 */
public class SchemaRegistryUtil {
    public static final String SCHEMA_REGISTRY_URL_PROPERTY = "com.onespot.secor.schema.registryUrl";
    private static volatile SchemaRegistryClient schemaRegistryClient;

    public static synchronized SchemaRegistryClient getSchemaRegistryClient() {
        if (schemaRegistryClient == null) {
            String registryUrl = System.getProperty(SCHEMA_REGISTRY_URL_PROPERTY);
            schemaRegistryClient = new CachedSchemaRegistryClient(registryUrl, 100);
        }
        return schemaRegistryClient;
    }

    public static void setSchemaRegistryClient(SchemaRegistryClient schemaRegistryClient) {
        SchemaRegistryUtil.schemaRegistryClient = schemaRegistryClient;
    }
}
