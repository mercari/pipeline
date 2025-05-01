package com.mercari.solution.module.transform;

import com.google.cloud.spanner.Struct;
import com.google.gson.JsonObject;
import com.mercari.solution.config.TransformConfig;
import com.mercari.solution.module.DataType;
import com.mercari.solution.module.FCollection;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

public class UnnestTransformTest {

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();

    /*
    @Test
    public void testUnnestRow() {

        final JsonObject parameters = new JsonObject();
        parameters.addProperty("path", "children");
        parameters.addProperty("prefix", true);

        final TransformConfig config = new TransformConfig();
        config.setName("unnest");
        config.setModule("unnest");
        config.setInputs(Arrays.asList("rowInput"));
        config.setParameters(parameters);

        final Row testRow = createTestRow();

        final PCollection<Row> inputRows = pipeline
                .apply("CreateDummy", Create.of(testRow, testRow));
        final FCollection<Row> fCollection = FCollection.of("rowInput", inputRows, DataType.ROW, testRow.getSchema());

        final Map<String, FCollection<?>> outputs = UnnestTransform.transform(Arrays.asList(fCollection), config);
        final PCollection<Row> outputRows = (PCollection<Row>) outputs.get("unnest").getCollection();

        PAssert.that(outputRows).satisfies(rows -> {
            int count = 0;
            for(Row row : rows) {
                Assert.assertEquals("stringValue", row.getString("stringField"));
                Assert.assertEquals("cstringValue", row.getString("children_cstringField"));

                final Row grandchildRow = row.getRow("children_grandchild");
                Assert.assertEquals("gcstringValue", grandchildRow.getString("gcstringField"));

                final Collection<Row> grandchildrenRows = row.getArray("children_grandchildren");
                Assert.assertEquals(2, grandchildrenRows.size());
                for(final Row grandchildrenRow : grandchildrenRows) {
                    Assert.assertEquals("gcstringValue", grandchildrenRow.getString("gcstringField"));
                }
                count++;
            }
            Assert.assertEquals(4, count);
            return null;
        });

        pipeline.run();

    }

    @Test
    public void testUnnestStruct() {

        final JsonObject parameters = new JsonObject();
        parameters.addProperty("path", "children");
        parameters.addProperty("prefix", true);

        final TransformConfig config = new TransformConfig();
        config.setName("unnest");
        config.setModule("unnest");
        config.setInputs(Arrays.asList("structInput"));
        config.setParameters(parameters);

        final Struct testStruct = createTestStruct();

        final PCollection<Struct> inputRows = pipeline
                .apply("CreateDummy", Create.of(testStruct, testStruct));
        final FCollection<Struct> fCollection = FCollection.of("structInput", inputRows, DataType.STRUCT, testStruct.getType());

        final Map<String, FCollection<?>> outputs = UnnestTransform.transform(Arrays.asList(fCollection), config);
        final PCollection<Struct> outputStructss = (PCollection<Struct>) outputs.get("unnest").getCollection();

        PAssert.that(outputStructss).satisfies(structs -> {
            int count = 0;
            for(final Struct struct : structs) {
                Assert.assertEquals("stringValue", struct.getString("stringField"));
                Assert.assertEquals("cstringValue", struct.getString("children_cstringField"));

                final Struct grandchildStruct = struct.getStruct("children_grandchild");
                Assert.assertEquals("gcstringValue", grandchildStruct.getString("gcstringField"));

                final Collection<Struct> grandchildrenStructs = struct.getStructList("children_grandchildren");
                Assert.assertEquals(2, grandchildrenStructs.size());
                for(final Struct grandchildrenStruct : grandchildrenStructs) {
                    Assert.assertEquals("gcstringValue", grandchildrenStruct.getString("gcstringField"));
                }
                count++;
            }
            Assert.assertEquals(4, count);
            return null;
        });

        pipeline.run();

    }

    private Row createTestRow() {
        final Schema grandchildSchema = Schema.builder()
                .addStringField("gcstringField")
                .build();
        final Row grandchild = Row.withSchema(grandchildSchema)
                .withFieldValue("gcstringField", "gcstringValue")
                .build();
        final Schema childSchema = Schema.builder()
                .addStringField("cstringField")
                .addRowField("grandchild", grandchildSchema)
                .addArrayField("grandchildren", Schema.FieldType.row(grandchildSchema))
                .build();
        final Row child = Row.withSchema(childSchema)
                .withFieldValue("cstringField", "cstringValue")
                .withFieldValue("grandchild", grandchild)
                .withFieldValue("grandchildren", Arrays.asList(grandchild, grandchild))
                .build();
        final Schema schema = Schema.builder()
                .addStringField("stringField")
                .addArrayField("children", Schema.FieldType.row(childSchema))
                .build();
        final Row row = Row.withSchema(schema)
                .withFieldValue("stringField", "stringValue")
                .withFieldValue("children", Arrays.asList(child, child))
                .build();

        return row;
    }

    private Struct createTestStruct() {
        final Struct grandchild = Struct.newBuilder()
                .set("gcstringField").to("gcstringValue")
                .build();
        final Struct child = Struct.newBuilder()
                .set("cstringField").to("cstringValue")
                .set("grandchild").to(grandchild)
                .set("grandchildren").toStructArray(grandchild.getType(), Arrays.asList(grandchild, grandchild))
                .set("grandchildrenNull").toStructArray(grandchild.getType(),null)
                .build();
        final Struct struct = Struct.newBuilder()
                .set("stringField").to("stringValue")
                .set("children").toStructArray(child.getType(), Arrays.asList(child, child))
                //.set("childrenNull").to((Struct)null)
                .build();

        return struct;
    }

     */

}
