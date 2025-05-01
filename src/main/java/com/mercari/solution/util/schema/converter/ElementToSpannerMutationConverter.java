package com.mercari.solution.util.schema.converter;

import com.google.cloud.spanner.Mutation;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;

import java.util.List;

public class ElementToSpannerMutationConverter {

    public static Mutation convert(
            final Schema schema,
            final MElement element,
            final String table,
            final String mutationOp,
            final List<String> keyFields,
            final List<String> allowCommitTimestampFields) {

        return null;
    }

}
