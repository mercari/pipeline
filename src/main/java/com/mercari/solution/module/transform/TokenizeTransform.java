package com.mercari.solution.module.transform;

import com.mercari.solution.module.*;
import com.mercari.solution.util.domain.text.analyzer.TokenAnalyzer;
import com.mercari.solution.util.pipeline.Union;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.values.PCollection;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.ja.tokenattributes.BaseFormAttribute;
import org.apache.lucene.analysis.ja.tokenattributes.InflectionAttribute;
import org.apache.lucene.analysis.ja.tokenattributes.PartOfSpeechAttribute;
import org.apache.lucene.analysis.ja.tokenattributes.ReadingAttribute;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.io.StringReader;
import java.util.*;


@Transform.Module(name="tokenize")
public class TokenizeTransform extends Transform {

    private static final Logger LOG = LoggerFactory.getLogger(TokenizeTransform.class);

    private static class TokenizeTransformParameters implements Serializable {

        private List<TokenizeParameter> fields;

        public void validate() {
            final List<String> errorMessages = new ArrayList<>();
            if (this.fields == null || this.fields.isEmpty()) {
                errorMessages.add("parameters.fields must not be empty");
            } else {
                for(int i=0; i<fields.size(); i++) {
                    if(fields.get(i) == null) {
                        errorMessages.add("parameters.fields[" + i + "] must not be null");
                    } else {
                        errorMessages.addAll(fields.get(i).validate(i));
                    }
                }
            }

            if (!errorMessages.isEmpty()) {
                throw new IllegalModuleException(errorMessages);
            }
        }

        public void setDefaults() {
            for(TokenizeParameter field : fields) {
                field.setDefault();
            }
        }

        private static class TokenizeParameter implements Serializable {

            private String name;
            private String input;
            private List<TokenAnalyzer.CharFilterConfig> charFilters;
            private TokenAnalyzer.TokenizerConfig tokenizer;
            private List<TokenAnalyzer.TokenFilterConfig> filters;

            public List<String> validate(int n) {
                final List<String> errorMessages = new ArrayList<>();
                if (this.name == null) {
                    errorMessages.add("TokenizeTransform parameters.fields[" + n + "].name must not be null.");
                }
                if (this.input == null) {
                    errorMessages.add("TokenizeTransform parameters.fields[" + n + "].input must not be null.");
                }
                if (this.tokenizer == null) {
                    errorMessages.add("TokenizeTransform parameters.fields[" + n + "].tokenizer must not be null.");
                } else {
                    errorMessages.addAll(this.tokenizer.validate());
                }
                if (this.charFilters != null) {
                    for(final TokenAnalyzer.CharFilterConfig filter : this.charFilters) {
                        if(filter == null) {
                            errorMessages.add("CharFilter must not be null");
                        } else {
                            errorMessages.addAll(filter.validate());
                        }
                    }
                }
                if (this.filters != null) {
                    for(final TokenAnalyzer.TokenFilterConfig filter : this.filters) {
                        if(filter == null) {
                            errorMessages.add("TokenFilter must not be null");
                        } else {
                            errorMessages.addAll(filter.validate());
                        }
                    }
                }

                return errorMessages;
            }

            public void setDefault() {
                this.tokenizer.setDefault();

                if(this.charFilters == null) {
                    this.charFilters = new ArrayList<>();
                } else {
                    for(final TokenAnalyzer.CharFilterConfig filter : charFilters) {
                        filter.setDefaults();
                    }
                }

                if(this.filters == null) {
                    this.filters = new ArrayList<>();
                } else {
                    for(final TokenAnalyzer.TokenFilterConfig filter : filters) {
                        filter.setDefaults();
                    }
                }
            }

        }

    }

    @Override
    public MCollectionTuple expand(MCollectionTuple inputs) {
        final TokenizeTransformParameters parameters = getParameters(TokenizeTransformParameters.class);
        parameters.validate();
        parameters.setDefaults();

        final PCollection<MElement> input = inputs
                .apply("Union", Union.flatten()
                        .withWaits(getWaits())
                        .withStrategy(getStrategy()));
        final Schema inputSchema = Union.createUnionSchema(inputs);

        final List<Schema.Field> fields = new ArrayList<>(inputSchema.getFields());
        final Map<String, Schema> inputFieldSchemas = new HashMap<>();
        for(final TokenizeTransformParameters.TokenizeParameter param : parameters.fields) {
            final Schema fieldSchema = createOutputSchema(param);
            final Schema.Field field = Schema.Field.of(param.name, Schema.FieldType.array(Schema.FieldType.element(fieldSchema)).withNullable(true));
            fields.add(field);
            inputFieldSchemas.put(field.getName(), fieldSchema);
        }
        final Schema outputSchema = Schema.of(fields);
        final PCollection<MElement> output = input
                .apply("Reshuffle", Reshuffle.viaRandomKey())
                .apply("Tokenize", ParDo.of(new TokenizeDoFn(parameters.fields, inputSchema, inputFieldSchemas)));

        return MCollectionTuple
                .of(output, outputSchema);
    }

    private static class TokenizeDoFn extends DoFn<MElement, MElement> {

        private final List<TokenizeTransformParameters.TokenizeParameter> fields;
        private final Schema inputSchema;
        private final Map<String, Schema> inputFieldSchemas;
        private transient Map<String, Analyzer> analyzers;

        public TokenizeDoFn(final List<TokenizeTransformParameters.TokenizeParameter> fields,
                            final Schema inputSchema,
                            final Map<String, Schema> inputFieldSchemas) {

            this.fields = fields;
            this.inputSchema = inputSchema;
            this.inputFieldSchemas = inputFieldSchemas;
        }

        @Setup
        public void setup() {
            this.analyzers = new HashMap<>();
            for(final TokenizeTransformParameters.TokenizeParameter field : this.fields) {
                LOG.info("Add analyzer for field: {}", field.name);
                this.analyzers.put(field.name, new TokenAnalyzer(field.charFilters, field.tokenizer, field.filters));
            }
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final MElement element = c.element();
            if(element == null) {
                return;
            }
            final Map<String, Object> results = new HashMap<>();
            for(final TokenizeTransformParameters.TokenizeParameter field : fields) {
                final String text = element.getAsString(field.input);
                if(text != null) {
                    final Analyzer analyzer = analyzers.get(field.name);
                    final List<Map<String, Object>> tokens = extractTokens(analyzer, field.name, text, field.tokenizer.getType());
                    results.put(field.name, tokens);
                } else {
                    results.put(field.name, new ArrayList<>());
                }
            }
            final MElement output = MElement.of(results, c.timestamp());
            c.output(output);
        }

        private List<Map<String, Object>> extractTokens(final Analyzer analyzer,
                                                        final String field,
                                                        final String text,
                                                        final TokenAnalyzer.TokenizerType type) {

            final List<Map<String, Object>> tokens = new ArrayList<>();
            try(final TokenStream tokenStream = analyzer.tokenStream(field, new StringReader(text))) {
                tokenStream.reset();
                while(tokenStream.incrementToken()) {
                    final Map<String, Object> token = new HashMap<>();
                    final CharTermAttribute cta = tokenStream.getAttribute(CharTermAttribute.class);
                    final OffsetAttribute oa = tokenStream.getAttribute(OffsetAttribute.class);
                    final TypeAttribute ta = tokenStream.getAttribute(TypeAttribute.class);
                    token.put("token", cta.toString());
                    token.put("startOffset", oa.startOffset());
                    token.put("endOffset", oa.endOffset());
                    token.put("type", ta.type());

                    if(TokenAnalyzer.TokenizerType.JapaneseTokenizer.equals(type)) {
                        final PartOfSpeechAttribute psa = tokenStream.getAttribute(PartOfSpeechAttribute.class);
                        final InflectionAttribute ia = tokenStream.getAttribute(InflectionAttribute.class);
                        final BaseFormAttribute bfa = tokenStream.getAttribute(BaseFormAttribute.class);
                        final ReadingAttribute ra = tokenStream.getAttribute(ReadingAttribute.class);
                        token.put("partOfSpeech", psa.getPartOfSpeech());
                        token.put("inflectionForm", ia.getInflectionForm());
                        token.put("inflectionType", ia.getInflectionType());
                        token.put("baseForm", bfa.getBaseForm());
                        token.put("pronunciation", ra.getPronunciation());
                        token.put("reading", ra.getReading());
                    }

                    tokens.add(token);
                }
                tokenStream.end();
                return tokens;
            } catch (IOException e) {
                return tokens;
            }
        }

    }

    private static Schema createOutputSchema(final TokenizeTransformParameters.TokenizeParameter parameter) {
        final Schema.Builder builder = Schema.builder();
        builder.withField("token", Schema.FieldType.STRING);
        builder.withField("startOffset", Schema.FieldType.INT32);
        builder.withField("endOffset", Schema.FieldType.INT32);
        builder.withField("type", Schema.FieldType.STRING.withNullable(true));

        if(TokenAnalyzer.TokenizerType.JapaneseTokenizer.equals(parameter.tokenizer.getType())) {
            builder.withField("partOfSpeech", Schema.FieldType.STRING.withNullable(true));
            builder.withField("inflectionForm", Schema.FieldType.STRING.withNullable(true));
            builder.withField("inflectionType", Schema.FieldType.STRING.withNullable(true));
            builder.withField("baseForm", Schema.FieldType.STRING.withNullable(true));
            builder.withField("pronunciation", Schema.FieldType.STRING.withNullable(true));
            builder.withField("reading", Schema.FieldType.STRING.withNullable(true));
        }

        return builder.build();
    }


}
