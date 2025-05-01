package com.mercari.solution.util.pipeline;

import com.mercari.solution.module.MElement;
import com.mercari.solution.util.coder.UnionMapCoder;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

import java.util.Map;

public class Views {

    public enum Type {
        singleton,
        map,
        list,
        iterable,
        multimap
    }

    public enum accumulation {
        latest,
        update
    }

    public static PTransform<PCollection<MElement>, PCollectionView<Map<String,Object>>> of(final Type type) {
        return switch (type) {
            case singleton -> new SingletonView();
            default -> throw new IllegalArgumentException("Not supported view type + " + type);
        };
    }

    public static SingletonView singleton() {
        return new SingletonView();
    }

    public static class SingletonView extends PTransform<PCollection<MElement>, PCollectionView<Map<String,Object>>> {

        @Override
        public PCollectionView<Map<String, Object>> expand(PCollection<MElement> input) {
            final PCollection<MElement> elements;
            if(OptionUtil.isStreaming(input)) {
                elements = input
                        .apply("Window", Window
                                .<MElement>into(new GlobalWindows())
                                .triggering(Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane()))
                                .discardingFiredPanes())
                        .apply("Latest", Latest.globally());
            } else {
                elements = input;
            }

            return elements
                    .apply("Map", ParDo.of(new MapDoFn()))
                    .setCoder(UnionMapCoder.mapCoder())
                    .apply("View", View.asSingleton());
        }

        private static class MapDoFn extends DoFn<MElement, Map<String,Object>> {

            @ProcessElement
            public void processElement(ProcessContext c) {
                final MElement element = c.element();
                if(element == null) {
                    return;
                }
                c.output(element.asPrimitiveMap());
            }

        }

    }

}
