package com.mercari.solution.util.domain.text.analyzer;

import com.ibm.icu.text.*;
import com.mercari.solution.util.gcp.StorageUtil;
import opennlp.tools.chunker.ChunkerModel;
import opennlp.tools.lemmatizer.LemmatizerModel;
import opennlp.tools.postag.POSModel;
import opennlp.tools.sentdetect.SentenceModel;
import opennlp.tools.tokenize.TokenizerModel;
import org.apache.lucene.analysis.*;
import org.apache.lucene.analysis.charfilter.HTMLStripCharFilter;
import org.apache.lucene.analysis.charfilter.MappingCharFilter;
import org.apache.lucene.analysis.charfilter.NormalizeCharMap;
import org.apache.lucene.analysis.cjk.CJKWidthFilter;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.analysis.core.TypeTokenFilter;
import org.apache.lucene.analysis.core.UpperCaseFilter;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.en.PorterStemFilter;
import org.apache.lucene.analysis.icu.ICUNormalizer2CharFilter;
import org.apache.lucene.analysis.ja.*;
import org.apache.lucene.analysis.ja.dict.UserDictionary;
import org.apache.lucene.analysis.minhash.MinHashFilter;
import org.apache.lucene.analysis.miscellaneous.*;
import org.apache.lucene.analysis.ngram.EdgeNGramTokenFilter;
import org.apache.lucene.analysis.ngram.NGramTokenFilter;
import org.apache.lucene.analysis.ngram.NGramTokenizer;
import org.apache.lucene.analysis.opennlp.OpenNLPChunkerFilter;
import org.apache.lucene.analysis.opennlp.OpenNLPLemmatizerFilter;
import org.apache.lucene.analysis.opennlp.OpenNLPPOSFilter;
import org.apache.lucene.analysis.opennlp.OpenNLPTokenizer;
import org.apache.lucene.analysis.opennlp.tools.*;
import org.apache.lucene.analysis.pattern.*;
import org.apache.lucene.analysis.shingle.ShingleFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.email.UAX29URLEmailTokenizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URL;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;


public class TokenAnalyzer extends Analyzer {

    private static final Logger LOG = LoggerFactory.getLogger(TokenAnalyzer.class);

    private final List<CharFilterConfig> charFilterConfigs;
    private final TokenizerConfig tokenizerConfig;
    private final List<TokenFilterConfig> tokenFilterConfigs;

    public TokenAnalyzer(final List<CharFilterConfig> charFilterConfigs, final TokenizerConfig tokenizerConfig, final List<TokenFilterConfig> tokenFilterConfigs) {
        this.charFilterConfigs = charFilterConfigs;
        this.tokenizerConfig = tokenizerConfig;
        this.tokenFilterConfigs = tokenFilterConfigs;
    }

    @Override
    protected TokenStreamComponents createComponents(String fieldName) {
        final Tokenizer tokenizer = selectTokenizer(tokenizerConfig);
        TokenStream filter = tokenizer;
        for(final TokenFilterConfig f : tokenFilterConfigs) {
            filter = selectFilter(f, filter);
        }
        return new TokenStreamComponents(tokenizer, filter);
    }

    @Override
    protected Reader initReader(final String fieldName, final Reader reader) {
        if(charFilterConfigs == null || charFilterConfigs.size() == 0) {
            return reader;
        }
        Reader charFilter = reader;
        for(CharFilterConfig config : charFilterConfigs) {
            charFilter = selectCharFilter(charFilter, config);
        }
        return charFilter;
    }

    public Reader selectCharFilter(final Reader reader, final CharFilterConfig config) {
        if(config.getSkip()) {
            return reader;
        }
        switch (config.getType()) {
            case MappingCharFilter: {
                final NormalizeCharMap.Builder builder = new NormalizeCharMap.Builder();
                for(final Map.Entry<String, String> entry : config.getNormMap().entrySet()) {
                    builder.add(entry.getKey(), entry.getValue());
                }
                return new MappingCharFilter(builder.build(), reader);
            }
            case NormalizeCharFilter: {
                final Normalizer2 normalizer = Normalizer2.getInstance(null, config.getNormalizeName(), config.getNormalizeMode());
                return new ICUNormalizer2CharFilter(reader, normalizer);
            }
            case HTMLStripCharFilter: {
                if(config.getEscapedTags() == null) {
                    return new HTMLStripCharFilter(reader);
                } else {
                    return new HTMLStripCharFilter(reader, config.getEscapedTags());
                }
            }
            case PatternReplaceCharFilter: {
                return new PatternReplaceCharFilter(Pattern.compile(config.getPattern()), config.getReplacement(), reader);
            }
            default: {
                throw new IllegalStateException("Not supported charFilter: " + config.getType());
            }
        }
    }

    public Tokenizer selectTokenizer(final TokenizerConfig config) {
        switch(config.getType()) {
            case StandardTokenizer: {
                LOG.info("use standard tokenizer");
                return new StandardTokenizer();
            }
            case NGramTokenizer: {
                LOG.info("use ngram tokenizer");
                return new NGramTokenizer(config.getMinGram(), config.getMaxGram());
            }
            case JapaneseTokenizer: {
                LOG.info("use japanese tokenizer kuromoji");
                final UserDictionary userDictionary;
                if(config.getUserDictionary() != null) {
                    userDictionary = readUserDictionary(config.getUserDictionary());
                } else {
                    userDictionary = null;
                }
                return new JapaneseTokenizer(userDictionary, config.getDiscardPunctuation(), config.getDiscardCompoundToken(), config.getMode());
            }
            case WhitespaceTokenizer: {
                LOG.info("use whitespace tokenizer");
                return new WhitespaceTokenizer();
            }
            case PatternTokenizer: {
                LOG.info("use pattern tokenizer");
                return new PatternTokenizer(Pattern.compile(config.getPattern()), config.getGroup());
            }
            case SimplePatternTokenizer: {
                LOG.info("use simple pattern tokenizer");
                return new SimplePatternTokenizer(config.getPattern());
            }
            case SimplePatternSplitTokenizer: {
                LOG.info("use simple pattern split tokenizer");
                return new SimplePatternSplitTokenizer(config.getPattern());
            }
            case KeywordTokenizer: {
                LOG.info("use keyword tokenizer");
                return new KeywordTokenizer();
            }
            case URLEmailTokenizer: {
                LOG.info("use url email tokenizer");
                return new UAX29URLEmailTokenizer();
            }
            case OpenNLPTokenizer: {
                LOG.info("use opennlp tokenizer");
                try(final InputStream tokenizerIS = inputStream(config.getTokenizerModel());
                    final InputStream sentenceIS = inputStream(config.getSentenceModel())) {

                    final TokenizerModel tokenizerModel = new TokenizerModel(tokenizerIS);
                    final SentenceModel sentenceModel = new SentenceModel(sentenceIS);
                    final NLPTokenizerOp tokenizerOp = new NLPTokenizerOp(tokenizerModel);
                    final NLPSentenceDetectorOp sentenceDetectorOp = new NLPSentenceDetectorOp(sentenceModel);
                    return new OpenNLPTokenizer(TokenStream.DEFAULT_TOKEN_ATTRIBUTE_FACTORY, sentenceDetectorOp, tokenizerOp);
                } catch (IOException e) {
                    throw new RuntimeException("Failed to create OpenNLPTokenizer with tokenizerModel: "
                            + config.getTokenizerModel() + ", sentenceModel: " + config.getSentenceModel(), e);
                }
            }
            default:
                throw new IllegalArgumentException("Not supported tokenizer: " + config.getType());
        }
    }

    public TokenStream selectFilter(TokenFilterConfig filter, TokenStream preTokenStream) {
        if(filter.getSkip()) {
            return preTokenStream;
        }
        switch(filter.getType()) {
            case StopFilter:
                return new StopFilter(preTokenStream, StopFilter.makeStopSet(filter.getWords()));
            case KeepWordFilter: {
                final CharArraySet set = new CharArraySet(filter.getWords().size(), false);
                set.addAll(filter.getWords());
                return new KeepWordFilter(preTokenStream, set);
            }
            case TypeTokenFilter:
                return new TypeTokenFilter(preTokenStream, new HashSet<>(filter.getTags()), filter.getUseWhiteList());
            case LengthFilter:
                return new LengthFilter(preTokenStream, filter.getMin(), filter.getMax());
            case LimitTokenCountFilter:
                return new LimitTokenCountFilter(preTokenStream, filter.getMaxTokenCount());
            case LowerCaseFilter:
                return new LowerCaseFilter(preTokenStream);
            case UpperCaseFilter:
                return new UpperCaseFilter(preTokenStream);
            case CJKWidthFilter:
                return new CJKWidthFilter(preTokenStream);
            case ASCIIFoldingFilter:
                return new ASCIIFoldingFilter(preTokenStream, filter.getPreserveOriginal());
            case PorterStemFilter:
                return new PorterStemFilter(preTokenStream);
            case EdgeNGramTokenFilter:
                return new EdgeNGramTokenFilter(preTokenStream, filter.getMin(), filter.getMax(), filter.getPreserveOriginal());
            case ShingleFilter: {
                final ShingleFilter shingleFilter = new ShingleFilter(preTokenStream, filter.getMin(), filter.getMax());
                shingleFilter.setOutputUnigrams(filter.getOutputUnigrams());
                return shingleFilter;
            }
            case PatternReplaceFilter:
                return new PatternReplaceFilter(preTokenStream, Pattern.compile(filter.getPattern()), filter.getReplacement(), filter.getReplaceAll());
            case PatternCaptureGroupTokenFilter: {
                final Pattern[] patterns = filter.getPatterns()
                        .stream()
                        .map(Pattern::compile)
                        .collect(Collectors.toList())
                        .toArray(new Pattern[filter.getPatterns().size()]);
                return new PatternCaptureGroupTokenFilter(preTokenStream, filter.getPreserveOriginal(), patterns);
            }
            case PatternTypingFilter: {
                final PatternTypingFilter.PatternTypingRule[] rules = filter.getPatternTypingRules()
                        .stream()
                        .map(TokenFilterConfig.PatternTypingRuleParameter::toPatternTypingRule)
                        .collect(Collectors.toList())
                        .toArray(new PatternTypingFilter.PatternTypingRule[filter.getPatternTypingRules().size()]);
                return new PatternTypingFilter(preTokenStream, rules);
            }
            case FingerprintFilter: {
                final char separator = filter.getSeparator().charAt(0);
                return new FingerprintFilter(preTokenStream, filter.getMaxOutputTokenSize(), separator);
            }
            case WordDelimiterGraphFilter: {
                final int configurationFlags =
                        (filter.generateWordParts ? 1 : 0)
                        + (filter.generateNumberParts ? 2 : 0)
                        + (filter.catenateWords ? 4 : 0)
                        + (filter.catenateNumbers ? 8 : 0)
                        + (filter.catenateAll ? 16 : 0)
                        + (filter.preserveOriginal ? 32 : 0)
                        + (filter.splitOnCaseChange ? 64 : 0)
                        + (filter.splitOnNumerics ? 128 : 0)
                        + (filter.stemEnglishPossessive ? 256 : 0);
                final CharArraySet set;
                if(filter.protWords != null && filter.protWords.size() > 0) {
                    set = new CharArraySet(filter.protWords.size(), false);
                    set.addAll(filter.protWords);
                } else {
                    set = null;
                }
                return new WordDelimiterGraphFilter(preTokenStream, filter.adjustInternalOffsets, WordDelimiterIterator.DEFAULT_WORD_DELIM_TABLE, configurationFlags, set);
            }
            case MinHashFilter: {
                return new MinHashFilter(preTokenStream, filter.getHashCount(), filter.getBucketCount(), filter.getHashSetSize(), filter.getWithRotation());
            }
            case JapaneseNumberFilter:
                return new JapaneseNumberFilter(preTokenStream);
            case JapaneseKatakanaStemFilter:
                return new JapaneseKatakanaStemFilter(preTokenStream, filter.getMin());
            case JapaneseBaseFormFilter:
                return new JapaneseBaseFormFilter(preTokenStream);
            case JapaneseReadingFormFilter: {
                return new JapaneseReadingFormFilter(preTokenStream, filter.getUseRomaji());
            }
            case JapanesePartOfSpeechStopFilter: {
                if(filter.getTags() == null) {
                    return new JapanesePartOfSpeechStopFilter(preTokenStream, JapaneseAnalyzer.getDefaultStopTags());
                } else {
                    return new JapanesePartOfSpeechStopFilter(preTokenStream, new HashSet<>(filter.getTags()));
                }
            }
            case OpenNLPChunkerFilter: {
                try(final InputStream is = inputStream(filter.getModel())) {
                    final ChunkerModel model = new ChunkerModel(is);
                    final NLPChunkerOp chunkerOp = new NLPChunkerOp(model);
                    return new OpenNLPChunkerFilter(preTokenStream, chunkerOp);
                } catch (IOException e) {
                    throw new RuntimeException("Failed to create OpenNLPChunkerFilter with model: " + filter.getModel(), e);
                }
            }
            case OpenNLPLemmatizerFilter: {
                try(final InputStream is = inputStream(filter.getModel())) {
                    final LemmatizerModel model = new LemmatizerModel(is);
                    final NLPLemmatizerOp lemmatizerOp = new NLPLemmatizerOp(null, model);
                    return new OpenNLPLemmatizerFilter(preTokenStream, lemmatizerOp);
                } catch (IOException e) {
                    throw new RuntimeException("Failed to create OpenNLPLemmatizerFilter with model: " + filter.getModel(), e);
                }
            }
            case OpenNLPPOSFilter: {
                try(final InputStream is = inputStream(filter.getModel())) {
                    final POSModel model = new POSModel(is);
                    final NLPPOSTaggerOp taggerOp = new NLPPOSTaggerOp(model);
                    return new OpenNLPPOSFilter(preTokenStream, taggerOp);
                } catch (IOException e) {
                    throw new RuntimeException("Failed to create OpenNLPPOSFilter with model: " + filter.getModel(), e);
                }
            }
            default:
                throw new IllegalArgumentException("Not supported filter type: " + filter.getType());
        }
    }

    public static class CharFilterConfig implements Serializable {

        private CharFilterType type;
        private Boolean skip;

        // for MappingCharFilter
        private Map<String, String> normMap;

        // for HTMLStripCharFilter
        private Set<String> escapedTags;

        // for PatternReplaceCharFilter
        private String pattern;
        private String replacement;

        // for NormalizeCharFilter
        private String normalizeName;
        private Normalizer2.Mode normalizeMode;


        public CharFilterType getType() {
            return type;
        }

        public void setType(CharFilterType type) {
            this.type = type;
        }

        public Boolean getSkip() {
            return skip;
        }

        public void setSkip(Boolean skip) {
            this.skip = skip;
        }

        public Map<String, String> getNormMap() {
            return normMap;
        }

        public void setNormMap(Map<String, String> normMap) {
            this.normMap = normMap;
        }

        public Set<String> getEscapedTags() {
            return escapedTags;
        }

        public void setEscapedTags(Set<String> escapedTags) {
            this.escapedTags = escapedTags;
        }

        public String getPattern() {
            return pattern;
        }

        public void setPattern(String pattern) {
            this.pattern = pattern;
        }

        public String getReplacement() {
            return replacement;
        }

        public void setReplacement(String replacement) {
            this.replacement = replacement;
        }

        public String getNormalizeName() {
            return normalizeName;
        }

        public void setNormalizeName(String normalizeName) {
            this.normalizeName = normalizeName;
        }

        public Normalizer2.Mode getNormalizeMode() {
            return normalizeMode;
        }

        public void setNormalizeMode(Normalizer2.Mode normalizeMode) {
            this.normalizeMode = normalizeMode;
        }

        public List<String> validate() {
            final List<String> errorMessages = new ArrayList<>();
            if(skip != null && skip) {
                return errorMessages;
            }
            if (this.type == null) {
                errorMessages.add("CharFilter.type parameter must not be null.");
            }
            return errorMessages;
        }

        public void setDefaults() {
            if (this.skip == null) {
                this.skip = false;
            }
            if (this.replacement == null) {
                this.replacement = "";
            }
            if (this.normalizeName == null) {
                this.normalizeName = "nfkc_cf";
            }
            if (this.normalizeMode == null) {
                this.normalizeMode = Normalizer2.Mode.COMPOSE;
            }
        }
    }

    public static class TokenizerConfig implements Serializable {

        private TokenizerType type;

        // for JapaneseTokenizer
        private String userDictionary;
        private JapaneseTokenizer.Mode mode;
        private Boolean discardPunctuation;
        private Boolean discardCompoundToken;

        // for NGramTokenizer
        private Integer minGram;
        private Integer maxGram;

        // for PatternTokenizer
        private String pattern;
        private Integer group;

        // for OpenNLPTokenizer
        private String tokenizerModel;
        private String sentenceModel;

        public TokenizerType getType() {
            return type;
        }

        public void setType(TokenizerType type) {
            this.type = type;
        }

        public String getUserDictionary() {
            return userDictionary;
        }

        public void setUserDictionary(String userDictionary) {
            this.userDictionary = userDictionary;
        }

        public JapaneseTokenizer.Mode getMode() {
            return mode;
        }

        public void setMode(JapaneseTokenizer.Mode mode) {
            this.mode = mode;
        }

        public Boolean getDiscardPunctuation() {
            return discardPunctuation;
        }

        public void setDiscardPunctuation(Boolean discardPunctuation) {
            this.discardPunctuation = discardPunctuation;
        }

        public Boolean getDiscardCompoundToken() {
            return discardCompoundToken;
        }

        public void setDiscardCompoundToken(Boolean discardCompoundToken) {
            this.discardCompoundToken = discardCompoundToken;
        }

        public Integer getMinGram() {
            return minGram;
        }

        public void setMinGram(Integer minGram) {
            this.minGram = minGram;
        }

        public Integer getMaxGram() {
            return maxGram;
        }

        public void setMaxGram(Integer maxGram) {
            this.maxGram = maxGram;
        }

        public String getPattern() {
            return pattern;
        }

        public void setPattern(String pattern) {
            this.pattern = pattern;
        }

        public Integer getGroup() {
            return group;
        }

        public void setGroup(Integer group) {
            this.group = group;
        }

        public String getTokenizerModel() {
            return tokenizerModel;
        }

        public void setTokenizerModel(String tokenizerModel) {
            this.tokenizerModel = tokenizerModel;
        }

        public String getSentenceModel() {
            return sentenceModel;
        }

        public void setSentenceModel(String sentenceModel) {
            this.sentenceModel = sentenceModel;
        }

        public List<String> validate() {
            final List<String> errorMessages = new ArrayList<>();
            if (this.type == null) {
                errorMessages.add("Tokenizer.type parameter must not be null.");
            } else {
                switch (type) {
                    case PatternTokenizer:
                    case SimplePatternTokenizer:
                    case SimplePatternSplitTokenizer: {
                        if(pattern == null) {
                            errorMessages.add("Tokenizer.pattern parameter must not be null for pattern tokenizer");
                        }
                        break;
                    }
                    case OpenNLPTokenizer: {
                        if(tokenizerModel == null) {
                            errorMessages.add("Tokenizer.tokenizerModel parameter must not be null for opennlp tokenizer");
                        }
                        if(sentenceModel == null) {
                            errorMessages.add("Tokenizer.sentenceModel parameter must not be null for opennlp tokenizer");
                        }
                    }
                }
            }

            return errorMessages;
        }

        public void setDefault() {
            switch (type) {
                case JapaneseTokenizer: {
                    if(mode == null) {
                        mode = JapaneseTokenizer.Mode.SEARCH;
                    }
                    if(discardPunctuation == null) {
                        discardPunctuation = false;
                    }
                    if(discardCompoundToken == null) {
                        discardCompoundToken = false;
                    }
                    break;
                }
                case NGramTokenizer: {
                    if(minGram == null) {
                        minGram = 2;
                    }
                    if(maxGram == null) {
                        maxGram = Math.max(minGram, 2);
                    }
                    break;
                }
                case PatternTokenizer: {
                    if(group == null) {
                        group = -1;
                    }
                    break;
                }
            }

        }
    }

    public static class TokenFilterConfig implements Serializable {

        private TokenFilterType type;
        private Boolean skip;

        // for LengthFilter, NGramTokenFilter, TypeTokenFilter
        private Integer min;
        private Integer max;

        // for TypeTokenFilter
        private Boolean useWhiteList;

        // for StopFilter, KeepWordFilter
        private List<String> words;

        // for TypeTokenFilter, JapanesePartOfSpeechStopFilter
        private List<String> tags;

        // for ShingleFilter
        private Boolean outputUnigrams;

        // for PatternReplaceFilter
        private String pattern;
        private String replacement;
        private Boolean replaceAll;

        // for PatternCaptureGroupTokenFilter
        private Boolean preserveOriginal;
        private List<String> patterns;

        // for PatternTypingFilter
        private List<PatternTypingRuleParameter> patternTypingRules;

        // for WordDelimiterGraphFilter
        private Boolean adjustInternalOffsets;
        private Boolean generateWordParts;
        private Boolean generateNumberParts;
        private Boolean catenateWords;
        private Boolean catenateNumbers;
        private Boolean catenateAll;
        private Boolean splitOnCaseChange;
        private Boolean splitOnNumerics;
        private Boolean stemEnglishPossessive;
        private Boolean ignoreKeywords;
        private List<String> protWords;

        // for FingerprintFilter
        private Integer maxOutputTokenSize;
        private String separator;

        // for MinHashFilter
        private Integer hashCount;
        private Integer bucketCount;
        private Integer hashSetSize;
        private Boolean withRotation;

        // for LimitTokenCountFilter
        private Integer maxTokenCount;

        // for JapaneseReadingFormFilter
        private Boolean useRomaji;

        // for OpenNLP
        private String model;


        TokenFilterConfig() {}

        public TokenFilterType getType() {
            return type;
        }

        public void setType(TokenFilterType type) {
            this.type = type;
        }

        public Boolean getSkip() {
            return skip;
        }

        public void setSkip(Boolean skip) {
            this.skip = skip;
        }

        public Integer getMin() {
            return min;
        }

        public void setMin(Integer min) {
            this.min = min;
        }

        public Integer getMax() {
            return max;
        }

        public void setMax(Integer max) {
            this.max = max;
        }

        public Boolean getUseWhiteList() {
            return useWhiteList;
        }

        public void setUseWhiteList(Boolean useWhiteList) {
            this.useWhiteList = useWhiteList;
        }

        public List<String> getWords() {
            return words;
        }

        public void setWords(List<String> words) {
            this.words = words;
        }

        public List<String> getTags() {
            return tags;
        }

        public void setTags(List<String> tags) {
            this.tags = tags;
        }

        public Boolean getOutputUnigrams() {
            return outputUnigrams;
        }

        public void setOutputUnigrams(Boolean outputUnigrams) {
            this.outputUnigrams = outputUnigrams;
        }

        public String getPattern() {
            return pattern;
        }

        public void setPattern(String pattern) {
            this.pattern = pattern;
        }

        public String getReplacement() {
            return replacement;
        }

        public void setReplacement(String replacement) {
            this.replacement = replacement;
        }

        public Boolean getReplaceAll() {
            return replaceAll;
        }

        public void setReplaceAll(Boolean replaceAll) {
            this.replaceAll = replaceAll;
        }

        public Boolean getPreserveOriginal() {
            return preserveOriginal;
        }

        public void setPreserveOriginal(Boolean preserveOriginal) {
            this.preserveOriginal = preserveOriginal;
        }

        public List<String> getPatterns() {
            return patterns;
        }

        public void setPatterns(List<String> patterns) {
            this.patterns = patterns;
        }

        public List<PatternTypingRuleParameter> getPatternTypingRules() {
            return patternTypingRules;
        }

        public void setPatternTypingRules(List<PatternTypingRuleParameter> patternTypingRules) {
            this.patternTypingRules = patternTypingRules;
        }

        public Integer getMaxOutputTokenSize() {
            return maxOutputTokenSize;
        }

        public void setMaxOutputTokenSize(Integer maxOutputTokenSize) {
            this.maxOutputTokenSize = maxOutputTokenSize;
        }

        public String getSeparator() {
            return separator;
        }

        public void setSeparator(String separator) {
            this.separator = separator;
        }

        public Integer getHashCount() {
            return hashCount;
        }

        public void setHashCount(Integer hashCount) {
            this.hashCount = hashCount;
        }

        public Integer getBucketCount() {
            return bucketCount;
        }

        public void setBucketCount(Integer bucketCount) {
            this.bucketCount = bucketCount;
        }

        public Integer getHashSetSize() {
            return hashSetSize;
        }

        public void setHashSetSize(Integer hashSetSize) {
            this.hashSetSize = hashSetSize;
        }

        public Boolean getWithRotation() {
            return withRotation;
        }

        public void setWithRotation(Boolean withRotation) {
            this.withRotation = withRotation;
        }

        public Integer getMaxTokenCount() {
            return maxTokenCount;
        }

        public void setMaxTokenCount(Integer maxTokenCount) {
            this.maxTokenCount = maxTokenCount;
        }

        public Boolean getUseRomaji() {
            return useRomaji;
        }

        public void setUseRomaji(Boolean useRomaji) {
            this.useRomaji = useRomaji;
        }

        public String getModel() {
            return model;
        }

        public void setModel(String model) {
            this.model = model;
        }

        public List<String> validate() {
            final List<String> errorMessages = new ArrayList<>();
            if(skip != null && skip) {
                return errorMessages;
            }

            if (type == null) {
                errorMessages.add("Filter.type parameter must not be null.");
            } else {
                switch (type) {
                    case KeepWordFilter: {
                        if(words == null) {
                            errorMessages.add("KeepWordFilter.words parameter must not be null.");
                        } else if(words.size() == 0) {
                            errorMessages.add("KeepWordFilter.words parameter size must not be zero.");
                        }
                        break;
                    }
                    case PatternTypingFilter: {
                        if(patternTypingRules == null) {
                            errorMessages.add("PatternTypingFilter.patternTypingRules parameter must not be null.");
                        } else if(patternTypingRules.size() == 0) {
                            errorMessages.add("PatternTypingFilter.patternTypingRules parameter size must not be zero.");
                        }
                        break;
                    }
                    case OpenNLPChunkerFilter: {
                        if(model == null) {
                            errorMessages.add("OpenNLPChunkerFilter.model parameter must not be null.");
                        } else if(!model.startsWith("gs://") && !model.startsWith("http")) {
                            errorMessages.add("OpenNLPChunkerFilter.model parameter must not be starts with `gs://` or `https://` or `http://`.");
                        }
                        break;
                    }
                    case OpenNLPPOSFilter: {
                        if(model == null) {
                            errorMessages.add("OpenNLPPOSFilter.model parameter must not be null.");
                        } else if(!model.startsWith("gs://") && !model.startsWith("http")) {
                            errorMessages.add("OpenNLPPOSFilter.model parameter must not be starts with `gs://` or `https://` or `http://`.");
                        }
                        break;
                    }
                    case OpenNLPLemmatizerFilter: {
                        if(model == null) {
                            errorMessages.add("OpenNLPLemmatizerFilter.model parameter must not be null.");
                        } else if(!model.startsWith("gs://") && !model.startsWith("http")) {
                            errorMessages.add("OpenNLPLemmatizerFilter.model parameter must not be starts with `gs://` or `https://` or `http://`.");
                        }
                        break;
                    }
                }
            }

            return errorMessages;
        }

        public void setDefaults() {
            if(skip == null) {
                skip = false;
            }
            switch (type) {
                case TypeTokenFilter: {
                    if(useWhiteList == null) {
                        useWhiteList = false;
                    }
                    break;
                }
                case StopFilter: {
                    if(words == null) {
                        final List<String> japaneseStopWords = JapaneseAnalyzer.getDefaultStopSet().stream().map(Object::toString).collect(Collectors.toList());
                        final List<String> englishStopWords = EnglishAnalyzer.getDefaultStopSet().stream().map(Object::toString).collect(Collectors.toList());
                        words = new ArrayList<>();
                        words.addAll(japaneseStopWords);
                        words.addAll(englishStopWords);
                    }
                    break;
                }
                case LengthFilter: {
                    if(min == null) {
                        min = 2;
                    }
                    if(max == null) {
                        max = 20;
                    }
                    break;
                }
                case EdgeNGramTokenFilter: {
                    if(min == null) {
                        min = 2;
                    }
                    if(max == null) {
                        max = 2;
                    }
                    if(preserveOriginal == null) {
                        preserveOriginal = NGramTokenFilter.DEFAULT_PRESERVE_ORIGINAL;
                    }
                    break;
                }
                case ShingleFilter: {
                    if(outputUnigrams == null) {
                        outputUnigrams = true;
                    }
                    if(min == null) {
                        min = 2;
                    }
                    if(max == null) {
                        max = Math.max(min, 2);
                    }
                    break;
                }
                case PatternReplaceFilter: {
                    if(replacement == null) {
                        replacement = "";
                    }
                    if(replaceAll == null) {
                        replaceAll = true;
                    }
                    break;
                }
                case ASCIIFoldingFilter:
                case PatternCaptureGroupTokenFilter: {
                    if(preserveOriginal == null) {
                        preserveOriginal = false;
                    }
                    break;
                }
                case PatternTypingFilter: {
                    for(final PatternTypingRuleParameter p : patternTypingRules) {
                        p.setDefault();
                    }
                    break;
                }
                case WordDelimiterGraphFilter: {
                    if(adjustInternalOffsets == null) {
                        adjustInternalOffsets = true;
                    }
                    if(catenateAll == null) {
                        catenateAll = false;
                    }
                    if(catenateNumbers == null) {
                        catenateNumbers = false;
                    }
                    if(catenateWords == null) {
                        catenateWords = false;
                    }
                    if(generateNumberParts == null) {
                        generateNumberParts = true;
                    }
                    if(generateWordParts == null) {
                        generateWordParts = true;
                    }
                    if(ignoreKeywords == null) {
                        ignoreKeywords = false;
                    }
                    if(preserveOriginal == null) {
                        preserveOriginal = false;
                    }
                    if(splitOnCaseChange == null) {
                        splitOnCaseChange = true;
                    }
                    if(splitOnNumerics == null) {
                        splitOnNumerics = true;
                    }
                    if(stemEnglishPossessive == null) {
                        stemEnglishPossessive = true;
                    }

                    break;
                }
                case FingerprintFilter: {
                    if(maxOutputTokenSize == null) {
                        maxOutputTokenSize = FingerprintFilter.DEFAULT_MAX_OUTPUT_TOKEN_SIZE;
                    }
                    if(separator == null) {
                        separator = String.valueOf(FingerprintFilter.DEFAULT_SEPARATOR);
                    }
                    break;
                }
                case MinHashFilter: {
                    if(hashCount == null) {
                        hashCount = MinHashFilter.DEFAULT_HASH_COUNT;
                    }
                    if(bucketCount == null) {
                        bucketCount = MinHashFilter.DEFAULT_BUCKET_COUNT;
                    }
                    if(hashSetSize == null) {
                        hashSetSize = MinHashFilter.DEFAULT_HASH_SET_SIZE;
                    }
                    if(withRotation == null) {
                        withRotation = true;
                    }
                    break;
                }
                case LimitTokenCountFilter: {
                    if(maxTokenCount == null) {
                        maxTokenCount = 20;
                    }
                    break;
                }
                case JapaneseKatakanaStemFilter: {
                    if(min == null) {
                        min = JapaneseKatakanaStemFilter.DEFAULT_MINIMUM_LENGTH;
                    }
                }
                case JapanesePartOfSpeechStopFilter: {
                    if(tags == null) {
                        tags = new ArrayList<>(JapaneseAnalyzer.getDefaultStopTags());
                    }
                    break;
                }
                case JapaneseReadingFormFilter: {
                    if(useRomaji == null) {
                        useRomaji = false;
                    }
                    break;
                }
            }
        }

        public static class PatternTypingRuleParameter implements Serializable {

            private String pattern;
            private Integer flags;
            private String typeTemplate;

            public String getPattern() {
                return pattern;
            }

            public void setPattern(String pattern) {
                this.pattern = pattern;
            }

            public Integer getFlags() {
                return flags;
            }

            public void setFlags(Integer flags) {
                this.flags = flags;
            }

            public String getTypeTemplate() {
                return typeTemplate;
            }

            public void setTypeTemplate(String typeTemplate) {
                this.typeTemplate = typeTemplate;
            }

            public List<String> validate() {
                final List<String> errorMessages = new ArrayList<>();
                if(pattern == null) {
                    errorMessages.add("PatternTypingFilter.pattern parameter must not be null.");
                }
                if(typeTemplate == null) {
                    errorMessages.add("PatternTypingFilter.typeTemplate parameter must not be null.");
                }
                return errorMessages;
            }

            public void setDefault() {
                if(flags == null) {
                    flags = 0;
                }
            }

            public PatternTypingFilter.PatternTypingRule toPatternTypingRule() {
                return new PatternTypingFilter.PatternTypingRule(
                        Pattern.compile(pattern),
                        flags,
                        typeTemplate
                );
            }

        }

        public PatternTypingRuleParameter of() {
            final PatternTypingRuleParameter parameter = new PatternTypingRuleParameter();

            return parameter;
        }

    }

    public enum CharFilterType implements Serializable {
        MappingCharFilter,
        NormalizeCharFilter,
        HTMLStripCharFilter,
        PatternReplaceCharFilter
    }

    public enum TokenizerType implements Serializable {
        StandardTokenizer,
        JapaneseTokenizer,
        NGramTokenizer,
        WhitespaceTokenizer,
        PatternTokenizer,
        SimplePatternTokenizer,
        SimplePatternSplitTokenizer,
        KeywordTokenizer,
        URLEmailTokenizer,
        OpenNLPTokenizer
    }

    public enum TokenFilterType implements Serializable {
        LengthFilter,
        StopFilter,
        KeepWordFilter,
        TypeTokenFilter,
        LowerCaseFilter,
        UpperCaseFilter,
        CJKWidthFilter,
        ASCIIFoldingFilter,
        PorterStemFilter,
        ShingleFilter,
        PatternReplaceFilter,
        PatternCaptureGroupTokenFilter,
        PatternTypingFilter,
        LimitTokenCountFilter,
        WordDelimiterGraphFilter,
        FingerprintFilter,
        EdgeNGramTokenFilter,
        SynonymGraphFilter,
        MinHashFilter,
        JapaneseBaseFormFilter,
        JapaneseKatakanaStemFilter,
        JapanesePartOfSpeechStopFilter,
        JapaneseNumberFilter,
        JapaneseReadingFormFilter,
        OpenNLPChunkerFilter,
        OpenNLPLemmatizerFilter,
        OpenNLPPOSFilter
    }

    public static UserDictionary readUserDictionary(String source) {
        if(source.startsWith("gs://")) {
            try (final InputStream is = StorageUtil.readStream(StorageUtil.storage(), source);
                 final InputStreamReader isr = new InputStreamReader(is);
                 final BufferedReader br = new BufferedReader(isr)) {
                return UserDictionary.open(br);
            } catch (IOException e) {
                throw new RuntimeException("Failed to read user dictionary: " + source, e);
            }
        } else {
            try {
                return UserDictionary.open(new StringReader(source));
            } catch (IOException e) {
                throw new RuntimeException("Failed to read user dictionary: " + source, e);
            }
        }
    }

    private static InputStream inputStream(final String path) {
        if(path == null) {
            return null;
        }
        if(path.startsWith("gs://")) {
            return StorageUtil.readStream(StorageUtil.storage(), path);
        } else if(path.startsWith("http://") || path.startsWith("https://")) {
            try {
                return (new URL(path)).openStream();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } else {
            throw new IllegalStateException("Illegal model input: " + path);
        }
    }

}