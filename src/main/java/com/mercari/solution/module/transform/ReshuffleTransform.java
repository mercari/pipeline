package com.mercari.solution.module.transform;

import com.mercari.solution.module.*;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Transform.Module(name="reshuffle")
public class ReshuffleTransform extends Transform {

    private static final Logger LOG = LoggerFactory.getLogger(ReshuffleTransform.class);

    @Override
    public MCollectionTuple expand(MCollectionTuple inputs) {
        MCollectionTuple tuple = MCollectionTuple.empty(inputs.getPipeline());
        for(final String tag : inputs.getAllInputs()) {
            final Schema schema = inputs.getSchema(tag);
            final PCollection<MElement> element = inputs.get(tag);
            final String name = (inputs.size() == 1 ? "" : tag);
            final PCollection<MElement> output = element
                    .apply(getName() + name, Reshuffle.viaRandomKey())
                    .setCoder(element.getCoder());
            tuple = tuple.and(name, output, schema);
        }
        return tuple;
    }

}
