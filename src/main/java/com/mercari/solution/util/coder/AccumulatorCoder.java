package com.mercari.solution.util.coder;

import com.mercari.solution.util.pipeline.aggregation.Accumulator;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.util.common.ElementByteSizeObserver;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;

public class AccumulatorCoder extends StructuredCoder<Accumulator> {

    private final MapCoder<String,Object> mapCoder;
    private final MapCoder<String,Set<Object>> setCoder;

    public AccumulatorCoder() {
        this.mapCoder = MapCoder.of(StringUtf8Coder.of(), UnionMapCoder.unionValueCoder());
        this.setCoder = MapCoder.of(StringUtf8Coder.of(), SetCoder.of(UnionMapCoder.primitiveUnionValueCoder()));
    }

    public static AccumulatorCoder of() {
        return new AccumulatorCoder();
    }

    @Override
    public void encode(Accumulator value, OutputStream outStream) throws IOException {
        encode(value, outStream, Context.NESTED);
    }

    @Override
    public void encode(Accumulator value, OutputStream outStream, Context context) throws IOException {
        mapCoder.encode(value.getMap(), outStream, context);
        setCoder.encode(value.getSets(), outStream, context);
    }

    @Override
    public Accumulator decode(InputStream inStream) throws IOException {
        return decode(inStream, Context.NESTED);
    }

    @Override
    public Accumulator decode(InputStream inStream, Context context) throws  IOException {
        final Map<String,Object> map = mapCoder.decode(inStream);
        final Map<String,Set<Object>> sets = setCoder.decode(inStream);
        return Accumulator.of(map, sets);
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
        return Collections.emptyList();
    }

    @Override
    public List<? extends Coder<?>> getComponents() {
        return List.of(mapCoder, setCoder);
    }

    @Override
    public void registerByteSizeObserver(Accumulator value, ElementByteSizeObserver observer) throws Exception {
        mapCoder.registerByteSizeObserver(value.getMap(), observer);
        setCoder.registerByteSizeObserver(value.getSets(), observer);

    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
        verifyDeterministic(this, "AccumulatorCoder is deterministic if all coders are deterministic");
    }

}