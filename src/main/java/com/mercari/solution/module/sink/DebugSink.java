package com.mercari.solution.module.sink;

import com.google.gson.JsonObject;
import com.mercari.solution.module.MCollectionTuple;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.module.Sink;
import com.mercari.solution.util.TemplateUtil;
import com.mercari.solution.util.schema.converter.ElementToJsonConverter;
import freemarker.template.Template;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PDone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

@Sink.Module(name="debug")
public class DebugSink extends Sink {

    private static final Logger LOG = LoggerFactory.getLogger(DebugSink.class);

    private static class DebugSinkParameters implements Serializable {

        private LogLevel logLevel;
        private String logTemplate;

        private void setDefaults() {
            if(this.logLevel == null) {
                this.logLevel = LogLevel.debug;
            }
        }

    }

    private enum LogLevel implements Serializable {
        trace,
        debug,
        info,
        warn,
        error
    }

    @Override
    public MCollectionTuple expand(final MCollectionTuple inputs) {
        if (inputs.size() == 0) {
            throw new IllegalArgumentException("debug sink module requires inputs parameter");
        }
        final DebugSinkParameters parameters = getParameters(DebugSinkParameters.class);
        parameters.setDefaults();

        PCollectionList<Void> voids = PCollectionList.empty(inputs.getPipeline());
        for(final String tag : inputs.getAllInputs()) {
            final Schema schema = inputs.getSchema(tag);
            final PCollection<MElement> element = inputs.get(tag);
            final String name = String.format("%s.%s", getName(), tag);
            final PCollection<Void> done = element
                    .apply("Debug" + tag, ParDo.of(new DebugDoFn(name, schema, parameters)));
            voids = voids.and(done);
        }

        return MCollectionTuple
                .done(PDone.in(inputs.getPipeline()));
    }

    private static class DebugDoFn extends DoFn<MElement, Void> {

        private final String name;
        private final Schema schema;
        private final String templateText;
        private final LogLevel logLevel;

        private transient Template template;

        DebugDoFn(final String name, Schema schema, DebugSinkParameters parameters) {
            this.name = name;
            this.schema = schema;
            this.templateText = parameters.logTemplate;
            this.logLevel = parameters.logLevel;
        }

        @Setup
        public void setup() {
            if(templateText != null) {
                this.template = TemplateUtil.createSafeTemplate(name, templateText);
            } else {
                this.template = null;
            }
        }

        @ProcessElement
        public void processElement(final ProcessContext c, final BoundedWindow window) {
            final MElement element = c.element();
            if(element == null) {
                return;
            }
            final String text;
            final JsonObject json = ElementToJsonConverter.convert(schema, element.asPrimitiveMap());
            if(this.template == null) {
                final JsonObject output = new JsonObject();
                output.add("data", json);
                {
                    final JsonObject paneJson = new JsonObject();
                    paneJson.addProperty("timing", c.pane().getTiming().toString());
                    paneJson.addProperty("isFirst", c.pane().isFirst());
                    paneJson.addProperty("isLast", c.pane().isLast());
                    paneJson.addProperty("isUnknown", c.pane().isUnknown());
                    paneJson.addProperty("nonSpeculativeIndex", c.pane().getNonSpeculativeIndex());
                    if (!c.pane().isFirst() || !c.pane().isLast()) {
                        paneJson.addProperty("index", c.pane().getIndex());
                    } else {
                        paneJson.addProperty("index", -1);
                    }
                    output.add("pane", paneJson);
                }
                {
                    final JsonObject windowJson = new JsonObject();
                    if(window == GlobalWindow.INSTANCE)  {
                        windowJson.addProperty("type", "global");
                    } else if(window instanceof IntervalWindow) {
                        windowJson.addProperty("type", "interval");
                        windowJson.addProperty("maxTimestamp", window.maxTimestamp().toString());
                        final IntervalWindow iw = ((IntervalWindow)window);
                        windowJson.addProperty("start", iw.start().toString());
                        windowJson.addProperty("end", iw.end().toString());
                    }
                    output.add("window", windowJson);
                }
                text = output.toString();
            } else {
                final Map<String, Object> data = new HashMap<>();
                data.put("data", json);
                data.put("timestamp", c.timestamp().toString());

                data.put("paneTiming", c.pane().getTiming().toString());
                data.put("paneIsFirst", Boolean.valueOf(c.pane().isFirst()).toString());
                data.put("paneIsLast", Boolean.valueOf(c.pane().isLast()).toString());
                if (!c.pane().isFirst() || !c.pane().isLast()) {
                    data.put("paneIndex", c.pane().getIndex());
                } else {
                    data.put("paneIndex", -1);
                }

                data.put("windowMaxTimestamp", window.maxTimestamp().toString());
                if(window == GlobalWindow.INSTANCE) {
                    data.put("windowStart", "");
                    data.put("windowEnd", "");
                } else if(window instanceof IntervalWindow) {
                    final IntervalWindow iw = ((IntervalWindow)window);
                    data.put("windowStart", iw.start().toString());
                    data.put("windowEnd", iw.end().toString());
                } else {
                    data.put("windowStart", "");
                    data.put("windowEnd", "");
                }

                text = TemplateUtil.executeStrictTemplate(template, data);
            }

            final String message = String.format("%s: %s", name, text);
            switch (logLevel) {
                case trace -> LOG.trace(message);
                case debug -> LOG.debug(message);
                case info -> LOG.info(message);
                case warn -> LOG.warn(message);
                case error -> LOG.error(message);
            }
        }

    }
}
