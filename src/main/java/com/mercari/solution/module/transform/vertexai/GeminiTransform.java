package com.mercari.solution.module.transform.vertexai;


import com.mercari.solution.module.MCollectionTuple;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Transform;
import com.mercari.solution.util.pipeline.Union;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import java.io.Serializable;
import java.util.List;

@Transform.Module(name="vertexai.gemini")
public class GeminiTransform extends Transform {

    private static class GeminiTransformParameters implements Serializable {

        private String modelId;

        public List<String> validate() {
            return null;
        }

        public void setDefaults() {

        }

    }

    @Override
    public MCollectionTuple expand(MCollectionTuple inputs) {
        final PCollection<MElement> output = inputs
                .apply("Union", Union
                        .flatten()
                        .withWaits(getWaits())
                        .withStrategy(getStrategy()))
                .apply("Print", ParDo.of(new PrintDoFn()));
        return MCollectionTuple.of(output, inputs.getSingleSchema());
    }

    private static class PrintDoFn extends DoFn<MElement, MElement> {

        @ProcessElement
        public void processElement(ProcessContext c) {
            final MElement input = c.element();
            System.out.println("debug: " + input);
            c.output(input);
        }

    }
}
