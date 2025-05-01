package com.mercari.solution.util.domain.text;

import com.mercari.solution.util.domain.text.analyzer.TokenAnalyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.w3c.dom.Node;
import org.xml.sax.InputSource;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class HtmlUtil {

    public static boolean isZip(final byte[] bytes) {
        if(bytes == null || bytes.length < 2) {
            return false;
        }
        return "PK".equals(new String(bytes, 0, 2));
    }

    public static String stripHtml(final String html) throws IOException {
        final List<TokenAnalyzer.CharFilterConfig> charFilters = new ArrayList<>();
        final TokenAnalyzer.CharFilterConfig c = new TokenAnalyzer.CharFilterConfig();
        c.setType(TokenAnalyzer.CharFilterType.HTMLStripCharFilter);
        c.setSkip(false);
        charFilters.add(c);
        final TokenAnalyzer.TokenizerConfig tokenizer = new TokenAnalyzer.TokenizerConfig();
        tokenizer.setType(TokenAnalyzer.TokenizerType.KeywordTokenizer);

        final TokenAnalyzer htmlStripAnalyzer = new TokenAnalyzer(charFilters, tokenizer, new ArrayList<>());

        final List<String> lines = new ArrayList<>();
        try(final TokenStream tokenStream = htmlStripAnalyzer.tokenStream("", new StringReader(html))) {
            tokenStream.reset();
            while(tokenStream.incrementToken()) {
                final CharTermAttribute cta = tokenStream.getAttribute(CharTermAttribute.class);
                final String line = cta.toString()
                        .strip()
                        .replaceAll("\t", " ")
                        .replaceAll("\n", " ")
                        .replaceAll("\\s+",  " ");
                if(line.length() > 0) {
                    lines.add(line);
                }
            }
            tokenStream.end();
        }

        return String.join(" ", lines);
    }

    public static EPUBDocument readEPUB(final byte[] bytes) throws IOException {
        try(final InputStream is = new ByteArrayInputStream(bytes);
            final BufferedInputStream bis = new BufferedInputStream(is);
            final ZipInputStream zis = new ZipInputStream(bis)) {

            final List<String> texts = new ArrayList<>();

            long page = 0;
            ZipEntry zipEntry;
            while((zipEntry = zis.getNextEntry()) != null) {
                final String name = zipEntry.getName();
                System.out.println(name);
                if(name.endsWith(".xhtml") || name.endsWith(".html")) {
                    final String html = readText(zis);
                    final String text = HtmlUtil.stripHtml(html);
                    texts.add(text);
                    page += 1;
                } else if(name.endsWith("content.opf")) {
                    final String xml = readText(zis);
                    //parse(xml);
                    //System.out.println(xml);
                }
                zis.closeEntry();
            }
            final String content = String.join(" ", texts);

            return EPUBDocument.of(content, page);
        } catch (EOFException e) {
            throw new IllegalStateException(e);
        }
    }

    private static String readText(final ZipInputStream zis) throws IOException {
        final StringBuilder sb = new StringBuilder();
        int len;
        byte[] buf = new byte[1024];
        while ((len = zis.read(buf)) != -1) {
            sb.append(new String(buf, 0, len));
        }
        return sb.toString();
    }

    private static void parse(final String xmlStr) {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        try {
            DocumentBuilder builder = factory.newDocumentBuilder();
            InputSource is = new InputSource(new StringReader(xmlStr));
            org.w3c.dom.Document document = builder.parse(is);
            org.w3c.dom.NodeList metadata = document.getElementsByTagName("metadata");
            if(metadata != null) {
                final org.w3c.dom.NodeList meta = metadata.item(0).getChildNodes();
                for(int i=0; i<meta.getLength(); i++) {
                    org.w3c.dom.Node node = meta.item(i);
                    switch (node.getNodeName()) {
                        case "meta": {
                            final Node nameNode = node.getAttributes().getNamedItem("name");
                            if(nameNode != null) {
                                switch (nameNode.getNodeValue()) {
                                    case "generator": {
                                        System.out.println("generator: " + node.getAttributes().getNamedItem("content").getNodeValue());
                                        break;
                                    }
                                    case "cover": {
                                        System.out.println("cover: " + node.getAttributes().getNamedItem("content").getNodeValue());
                                        break;
                                    }
                                }
                            }
                            break;
                        }
                        case "dc:title": {
                            System.out.println("title: " + node.getTextContent());
                            break;
                        }
                        case "dc:creator": {
                            System.out.println("creator: " + node.getTextContent());
                            break;

                        }
                        case "dc:language": {
                            System.out.println("language: " + node.getTextContent());
                            break;
                        }

                    }

                }
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public static class EPUBDocument {

        private String title;
        private String creator;
        private String generator;
        private String date;
        private String language;
        private String content;
        private Long page;

        public String getTitle() {
            return title;
        }

        public void setTitle(String title) {
            this.title = title;
        }

        public String getCreator() {
            return creator;
        }

        public void setCreator(String creator) {
            this.creator = creator;
        }

        public String getGenerator() {
            return generator;
        }

        public void setGenerator(String generator) {
            this.generator = generator;
        }

        public String getDate() {
            return date;
        }

        public void setDate(String date) {
            this.date = date;
        }

        public String getLanguage() {
            return language;
        }

        public void setLanguage(String language) {
            this.language = language;
        }

        public String getContent() {
            return content;
        }

        public void setContent(String content) {
            this.content = content;
        }

        public Long getPage() {
            return page;
        }

        public void setPage(Long page) {
            this.page = page;
        }

        public static EPUBDocument of(final String content, final long page) {
            final  EPUBDocument document = new EPUBDocument();
            document.content = content;
            document.page = page;
            return document;
        }

    }

}
