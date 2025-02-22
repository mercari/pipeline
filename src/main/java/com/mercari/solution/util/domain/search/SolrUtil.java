package com.mercari.solution.util.domain.search;

/*
import com.mercari.solution.util.gcp.StorageUtil;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.response.JSONResponseWriter;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.SolrIndexWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;
 */

public class SolrUtil {

    /*
    private static final Logger LOG = LoggerFactory.getLogger(SolrUtil.class);

    private static final String SOLR_BASE_PATH = "/solr/";

    public static SolrIndexWriter createWriter(final SolrCore core, final String name, final boolean create) throws IOException {
        return SolrIndexWriter.create(core, name,
                core.getIndexDir(),
                core.getDirectoryFactory(), create,
                core.getLatestSchema(),
                core.getSolrConfig().indexConfig,
                core.getDeletionPolicy(),
                core.getCodec());
    }

    public static String sendLocalRequest(final SolrCore core, final String handlerPath, final ModifiableSolrParams params) throws IOException {
        final SolrQueryRequest request = new LocalSolrQueryRequest(core, params);
        final SolrQueryResponse response = new SolrQueryResponse();

        try(final SolrRequestHandler handler = core.getRequestHandler(handlerPath);
            final StringWriter stringWriter = new StringWriter()) {

            core.execute(handler, request, response);

            final JSONResponseWriter jsonResponseWriter = new JSONResponseWriter();
            jsonResponseWriter.write(stringWriter, request, response);

            if(request.getSchema() != null) {
                for(final Map.Entry<String, SchemaField> field : request.getSchema().getFields().entrySet()) {
                    //LOG.info("schemaFieldName: " + field.getKey() + ", typeClass: " + field.getValue().getType().getClassArg());
                }
            }

            if(response.getReturnFields() != null) {
                if(response.getReturnFields().getRequestedFieldNames() != null) {
                    for (final String fieldName : response.getReturnFields().getRequestedFieldNames()) {
                        //LOG.info("returnFieldName: " + fieldName);
                    }
                }

                if(response.getReturnFields().getExplicitlyRequestedFieldNames() != null){
                    for (final String fieldName : response.getReturnFields().getExplicitlyRequestedFieldNames()) {
                        //LOG.info("explicitlyRequestedFieldNames: " + fieldName);
                    }
                }
            }

            return stringWriter.toString();
        }
    }

    public void addDocument(final SolrCore core, final SolrInputDocument solrDoc) throws IOException {
        final LocalSolrQueryRequest req = new LocalSolrQueryRequest(core, new HashMap<>());
        AddUpdateCommand addUpdateCommand = new AddUpdateCommand(req);
        addUpdateCommand.solrDoc = solrDoc;
        core.getUpdateHandler().addDoc(addUpdateCommand);
    }

    public void commit(final SolrCore core) throws IOException {
        final LocalSolrQueryRequest req = new LocalSolrQueryRequest(core, new HashMap<>());
        final CommitUpdateCommand commitUpdateCommand = new CommitUpdateCommand(req, true);
        commitUpdateCommand.openSearcher = true;
        commitUpdateCommand.softCommit = true;
        commitUpdateCommand.waitSearcher = true;
        core.getUpdateHandler().commit(commitUpdateCommand);
    }

    public static void addAndCommitDocs(final SolrCore core, final List<SolrInputDocument> solrDocs) throws IOException {
        final LocalSolrQueryRequest req = new LocalSolrQueryRequest(core, new HashMap<>());
        for(final SolrInputDocument solrDoc : solrDocs) {
            AddUpdateCommand updateCommand = new AddUpdateCommand(req);
            updateCommand.solrDoc = solrDoc;
            core.getUpdateHandler().addDoc(updateCommand);
        }

        final CommitUpdateCommand cmd = new CommitUpdateCommand(req, true);
        cmd.openSearcher = true;
        cmd.softCommit = true;
        cmd.waitSearcher = true;
        core.getUpdateHandler().commit(cmd);
    }



    public static void uploadIndexFiles(final String coreName, final OutputStream outputStream) throws IOException {
        final Path indexDirPath = Paths.get(SOLR_BASE_PATH + coreName);
        LOG.info(String.format("Core: %s Finished to create index at [%s].", coreName, indexDirPath.toFile().getAbsolutePath()));
        try (final ZipOutputStream zos = new ZipOutputStream(new BufferedOutputStream(outputStream))) {
            zipIndexFile(zos, indexDirPath.toFile());
            zos.flush();
        }
    }

    public static void uploadIndexFiles(final String coreName, final String gcsPath, final String contentType) throws IOException {
        final Path indexDirPath = Paths.get(SOLR_BASE_PATH + coreName);
        LOG.info(String.format("Core: %s Finished to create index at [%s].", coreName, indexDirPath.toFile().getAbsolutePath()));

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (final ZipOutputStream zos = new ZipOutputStream(baos)) {

            zipIndexFile(zos, indexDirPath.toFile());

            zos.flush();
            baos.flush();
        }

        baos.close();
        StorageUtil.writeBytes(gcsPath, baos.toByteArray(), contentType, new HashMap<>(), new HashMap<>());
    }

    public static void downloadIndexFiles(final String coreName, final String gcsPath) throws IOException {
        final File coreDir = new File(SOLR_BASE_PATH + coreName);
        if(!coreDir.exists() || coreDir.isFile()) {
            coreDir.mkdirs();
        }
        try(final InputStream is = StorageUtil.readStream(StorageUtil.storage(), gcsPath);
            final BufferedInputStream bis = new BufferedInputStream(is);
            final ZipInputStream zis = new ZipInputStream(bis)) {

            ZipEntry zipEntry;
            while((zipEntry = zis.getNextEntry()) != null) {
                unzipIndexFile(zis, zipEntry, SOLR_BASE_PATH);
                zis.closeEntry();
            }
        } catch (EOFException e) {
            LOG.error("error: " + e.getMessage());
        }
    }

    private static void zipIndexFile(final ZipOutputStream zos, final File file) throws IOException {
        if (file.isDirectory()) {
            for (final File childFile : file.listFiles()) {
                zipIndexFile(zos, childFile);
            }
            return;
        }
        zos.putNextEntry(new ZipEntry(file.getAbsolutePath().replaceFirst(SOLR_BASE_PATH, "")));
        try (final InputStream is = new BufferedInputStream(new FileInputStream(file))) {
            LOG.info(file.getAbsolutePath() + " ");
            int len;
            byte[] buf = new byte[1024 * 1024];
            while ((len = is.read(buf)) != -1) {
                zos.write(buf, 0, len);
            }
        }
    }

    private static void unzipIndexFile(final ZipInputStream zis, final ZipEntry zipEntry, final String basePath) throws IOException {
        final File file = new File(basePath + zipEntry.getName());
        if (zipEntry.isDirectory()) {
            if(!file.exists()) {
                file.mkdirs();
            }
            return;
        }
        if(!file.exists()) {
            file.getParentFile().mkdirs();
            file.createNewFile();
        }
        try (final FileOutputStream fos = new FileOutputStream(file);
             final BufferedOutputStream bos = new BufferedOutputStream(fos)) {

            LOG.info("write: " + file.getAbsolutePath() + " ");
            int len;
            byte[] buf = new byte[1024];
            while ((len = zis.read(buf)) != -1) {
                bos.write(buf, 0, len);
            }
            bos.flush();
        }
    }

     */

}
