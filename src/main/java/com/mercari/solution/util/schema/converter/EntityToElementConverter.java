package com.mercari.solution.util.schema.converter;


import com.google.datastore.v1.Entity;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;

public class EntityToElementConverter {

    private static final String KEY_FIELD_NAME = "__key__";

    private static final Schema KEY_SCHEMA = Schema.builder()
            .withField("namespace", Schema.FieldType.STRING.withNullable(false))
            .withField("app", Schema.FieldType.STRING.withNullable(false))
            .withField("path", Schema.FieldType.STRING.withNullable(false))
            .withField("kind", Schema.FieldType.STRING.withNullable(false))
            .withField("name", Schema.FieldType.STRING.withNullable(true))
            .withField("id", Schema.FieldType.INT64.withNullable(true))
            .build();

    public static Schema addKeyToSchema(final Schema schema) {
        return Schema.builder(schema)
                .withField(KEY_FIELD_NAME, Schema.FieldType.element(KEY_SCHEMA).withNullable(true))
                .build();
    }

    public static MElement.Builder convert(Schema schema, Entity entity) {
        return null;
    }

}
