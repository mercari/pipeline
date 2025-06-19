package com.mercari.solution.module;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public enum DataType implements Serializable {

    MAP(0),
    ELEMENT(1),
    // format
    ROW(2), // Apache Beam row
    AVRO(3), // Apache Avro record
    PROTO(4), // Protobuf dynamic message
    // datastore format
    STRUCT(11), // GCP Spanner struct
    DOCUMENT(12), // GCP Firestore document
    ENTITY(13), // GCP Datastore entity
    CELLS(14), // GCP Bigtable cells
    // message
    MESSAGE(21), // GCP Pub/Sub message
    KINESIS(22), // AWS Kinesis record
    KAFKA(23), // Apache Kafka message
    // change capture record
    DEBEZIUM(40),
    SPANNER_DATACHANGERECORD(41),
    BIGTABLE_DATACHANGERECORD(42),
    // mutation
    UNIFIEDMUTATION(60),
    MUTATION(61),
    MUTATIONGROUP(62),
    // multi
    UNION(100),
    // unknown
    UNKNOWN(127);

    private final int id;


    DataType(final int id) {
        this.id = id;
    }

    public int getId() {
        return id;
    }

    public static DataType of(final int id) {
        for(final DataType dataType : values()) {
            if(dataType.id == id) {
                return dataType;
            }
        }
        throw new IllegalArgumentException("No such enum object for DataType id: " + id);
    }

    public static List<String> symbols() {
        final List<String> symbols = new ArrayList<>();
        for(final DataType type : values()) {
            symbols.add(type.name());
        }
        return symbols;
    }
}
