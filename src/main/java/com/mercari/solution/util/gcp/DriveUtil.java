package com.mercari.solution.util.gcp;

import com.google.api.client.http.ByteArrayContent;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.InputStreamContent;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.model.File;
import com.google.api.services.drive.model.User;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ImpersonatedCredentials;
import com.google.cloud.hadoop.util.ChainingHttpRequestInitializer;
import com.google.common.collect.ImmutableList;
import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.schema.converter.RowToRecordConverter;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.extensions.gcp.util.RetryHttpRequestInitializer;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class DriveUtil {

    private static final Pattern PATTERN_FIELDS = Pattern.compile("[a-z]+\\([a-zA-Z,]+\\)");
    public static final String MIMETYPE_APPS_FOLDER = "application/vnd.google-apps.folder";

    public static Drive drive(final String targetPrincipalAccount, final String... args) {

        final HttpTransport transport = new NetHttpTransport();
        final JsonFactory jsonFactory = GsonFactory.getDefaultInstance();
        try {
            final GoogleCredentials credentials;
            if(args.length > 0) {
                credentials = GoogleCredentials.getApplicationDefault().createScoped(args);
            } else {
                credentials = GoogleCredentials.getApplicationDefault();
            }

            final ImpersonatedCredentials targetCredentials = ImpersonatedCredentials.create(
                    credentials,
                    targetPrincipalAccount,
                    null,
                    Arrays.asList(args),
                    3600);

            final HttpRequestInitializer initializer = new ChainingHttpRequestInitializer(
                    new HttpCredentialsAdapter(targetCredentials),
                    // Do not log 404. It clutters the output and is possibly even required by the caller.
                    new RetryHttpRequestInitializer(ImmutableList.of(404)));
            return new Drive.Builder(transport, jsonFactory, initializer)
                    .setApplicationName("MercariDataflowTemplate")
                    .build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static boolean isFolder(final File file) {
        if(MIMETYPE_APPS_FOLDER.equals(file.getMimeType())) {
            return true;
        }
        return false;
    }

    public static void copy(final Drive drive, final String sourceFileId, final String destinationFileId, final Map<String, Object> attributes) throws IOException {
        final File file;
        if(attributes == null || attributes.size() == 0) {
            file = drive.files().get(sourceFileId).execute();
            file.setId(null);
        } else {
            file = new File();
            for(final Map.Entry<String, Object> entry : attributes.entrySet()) {
                file.set(entry.getKey(), entry.getValue());
            }
        }
        file.setParents(Arrays.asList(destinationFileId));
        drive.files().copy(sourceFileId, file).execute();
    }

    public static byte[] download(final Drive drive, final String fileId) {
        try {
            return drive.files().get(fileId).executeMediaAsInputStream().readAllBytes();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void downloadTo(final Drive drive, final String fileId, final OutputStream os) {
        try {
            drive.files().get(fileId).executeMediaAndDownloadTo(os);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void createFile(final Drive drive, final File file, final byte[] bytes) {
        final ByteArrayContent content = new ByteArrayContent(file.getMimeType(), bytes);
        try {
            drive.files().create(file, content).execute();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void createFile(final Drive drive, final File file, final InputStream is) {
        final InputStreamContent content = new InputStreamContent(file.getMimeType(), is);
        try {
            drive.files().create(file, content).execute();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Schema createFileSchema(final String fields) {
        final Schema.Builder builder = Schema.builder();

        final String fields_ = fields.trim().replaceAll(" ", "");
        final Matcher matcher = PATTERN_FIELDS.matcher(fields_);

        if(matcher.find()) {
            final String group = matcher.group();
            final int start = group.indexOf("(");
            final int end = group.lastIndexOf(")");
            if(start > 0 && end > 0) {
                final String fields__ = fields_.substring(start + 1, end);
                for(final String field : fields__.split(",")) {
                    builder.addField(field, convertFieldType(field));
                }
            }

            /*
            final String others = fields_.substring(end + 1);
            for(final String field : others.split(",")) {
                if(field.length() == 0) {
                    continue;
                }
                builder.addField(field, convertFieldType(field));
            }
             */
        } else {
            throw new IllegalArgumentException("Failed to create schema from fields: " + fields_);
        }

        return builder.build();
    }

    public static com.mercari.solution.module.Schema createFileSchema2(final String fields) {
        final com.mercari.solution.module.Schema.Builder builder = com.mercari.solution.module.Schema.builder();

        final String fields_ = fields.trim().replaceAll(" ", "");
        final Matcher matcher = PATTERN_FIELDS.matcher(fields_);

        if(matcher.find()) {
            final String group = matcher.group();
            final int start = group.indexOf("(");
            final int end = group.lastIndexOf(")");
            if(start > 0 && end > 0) {
                final String fields__ = fields_.substring(start + 1, end);
                for(final String field : fields__.split(",")) {
                    builder.withField(field, convertFieldType2(field));
                }
            }

            /*
            final String others = fields_.substring(end + 1);
            for(final String field : others.split(",")) {
                if(field.length() == 0) {
                    continue;
                }
                builder.addField(field, convertFieldType(field));
            }
             */
        } else {
            throw new IllegalArgumentException("Failed to create schema from fields: " + fields_);
        }

        return builder.build();
    }

    public static org.apache.avro.Schema createAvroFileSchema(final String fields) {
        return RowToRecordConverter.convertSchema(createFileSchema(fields));
    }

    public static Map<String, Object> convertPrimitives(final com.mercari.solution.module.Schema schema, final File file) {
        final Map<String, Object> values = new HashMap<>();
        for(final com.mercari.solution.module.Schema.Field field : schema.getFields()) {
            final Object value = switch (field.getName()) {
                // STRING
                case "id", "driveId", "name", "description", "originalFilename", "kind", "mimeType", "fileExtension",
                     "fullFileExtension", "resourceKey", "webContentLink", "webViewLink", "iconLink", "thumbnailLink",
                     "folderColorRgb", "md5Checksum", "headRevisionId", "nextPageToken",
                     // BOOLEAN
                     "starred", "trashed", "explicitlyTrashed", "viewedByMe", "shared", "ownedByMe", "viewerCanCopyContent",
                     "writerCanShare", "isAppAuthorized", "hasThumbnail", "modifiedByMe", "hasAugmentedPermissions",
                     // INT64
                     "size", "version", "quotaBytesUsed", "thumbnailVersion",
                     // STRING ARRAY
                     "parents", "spaces", "permissionIds" ->  file.get(field.getName());
                // DATETIME
                case "createdTime" -> file.getCreatedTime() == null ? null : DateTimeUtil.toEpochMicroSecond(file.getCreatedTime().toStringRfc3339());
                case "modifiedTime" -> file.getModifiedTime() == null ? null : DateTimeUtil.toEpochMicroSecond(file.getModifiedTime().toStringRfc3339());
                case "viewedByMeTime" -> file.getViewedByMeTime() == null ? null : DateTimeUtil.toEpochMicroSecond(file.getViewedByMeTime().toStringRfc3339());
                case "modifiedByMeTime" -> file.getModifiedByMeTime() == null ? null : DateTimeUtil.toEpochMicroSecond(file.getModifiedByMeTime().toStringRfc3339());
                case "sharedWithMeTime" -> file.getSharedWithMeTime() == null ? null : DateTimeUtil.toEpochMicroSecond(file.getSharedWithMeTime().toStringRfc3339());
                case "trashedTime" -> file.getTrashedTime() == null ? null : DateTimeUtil.toEpochMicroSecond(file.getTrashedTime().toStringRfc3339());
                // User
                case "trashingUser" -> convertDriveUserMap(file.getTrashingUser());
                case "sharingUser" -> convertDriveUserMap(file.getSharingUser());
                case "lastModifyingUser" -> convertDriveUserMap(file.getLastModifyingUser());
                // User ARRAY
                case "owners" -> {
                    if(file.getOwners() == null) {
                        yield null;
                    } else {
                        final List<Map<String, Object>> owners = new ArrayList<>();
                        for(final User owner : file.getOwners()) {
                            if(owner != null) {
                                owners.add(convertDriveUserMap(owner));
                            }
                        }
                        yield owners;
                    }
                }
                default -> throw new IllegalArgumentException();
            };
            values.put(field.getName(), value);
        }
        return values;
    }

    // https://developers.google.com/drive/api/v3/reference/files
    public static Row convertToRow(final Schema schema, final File file) {
        final Row.FieldValueBuilder builder = Row.withSchema(schema).withFieldValues(new HashMap<>());
        for(final Schema.Field field : schema.getFields()) {
            switch (field.getName()) {
                // STRING
                case "id":
                case "driveId":
                case "name":
                case "description":
                case "originalFilename":
                case "kind":
                case "mimeType":
                case "fileExtension":
                case "fullFileExtension":
                case "resourceKey":
                case "webContentLink":
                case "webViewLink":
                case "iconLink":
                case "thumbnailLink":
                case "folderColorRgb":
                case "md5Checksum":
                case "headRevisionId":
                case "nextPageToken":
                // BOOLEAN
                case "starred":
                case "trashed":
                case "explicitlyTrashed":
                case "viewedByMe":
                case "shared":
                case "ownedByMe":
                case "viewerCanCopyContent":
                case "writerCanShare":
                case "isAppAuthorized":
                case "hasThumbnail":
                case "modifiedByMe":
                case "hasAugmentedPermissions":
                // INT64
                case "size":
                case "version":
                case "quotaBytesUsed":
                case "thumbnailVersion":
                // STRING ARRAY
                case "parents":
                case "spaces":
                case "permissionIds":
                    builder.withFieldValue(field.getName(), file.get(field.getName()));
                    break;
                // DATETIME
                case "createdTime":
                    builder.withFieldValue(field.getName(), file.getCreatedTime() == null ? null : Instant.parse(file.getCreatedTime().toStringRfc3339()));
                    break;
                case "modifiedTime":
                    builder.withFieldValue(field.getName(), file.getModifiedTime() == null ? null : Instant.parse(file.getModifiedTime().toStringRfc3339()));
                    break;
                case "viewedByMeTime":
                    builder.withFieldValue(field.getName(), file.getViewedByMeTime() == null ? null : Instant.parse(file.getViewedByMeTime().toStringRfc3339()));
                    break;
                case "modifiedByMeTime":
                    builder.withFieldValue(field.getName(), file.getModifiedByMeTime() == null ? null : Instant.parse(file.getModifiedByMeTime().toStringRfc3339()));
                    break;
                case "sharedWithMeTime":
                    builder.withFieldValue(field.getName(), file.getSharedWithMeTime() == null ? null : Instant.parse(file.getSharedWithMeTime().toStringRfc3339()));
                    break;
                case "trashedTime":
                    builder.withFieldValue(field.getName(), file.getTrashedTime() == null ? null : Instant.parse(file.getTrashedTime().toStringRfc3339()));
                    break;
                // User
                case "trashingUser": {
                    builder.withFieldValue(field.getName(), convertDriveUser(file.getTrashingUser()));
                    break;
                }
                case "sharingUser":{
                    builder.withFieldValue(field.getName(), convertDriveUser(file.getSharingUser()));
                    break;
                }
                case "lastModifyingUser":{
                    builder.withFieldValue(field.getName(), convertDriveUser(file.getLastModifyingUser()));
                    break;
                }
                // User ARRAY
                case "owners":{
                    if(file.getOwners() == null) {
                        builder.withFieldValue(field.getName(), null);
                    } else {
                        final List<Row> owners = new ArrayList<>();
                        for(final User owner : file.getOwners()) {
                            if(owner != null) {
                                owners.add(convertDriveUser(owner));
                            }
                        }
                        builder.withFieldValue(field.getName(), owners);
                    }
                    break;
                }
            }
        }
        return builder.build();
    }

    public static GenericRecord convertToRecord(final org.apache.avro.Schema schema, final File file) {

        final GenericRecordBuilder builder = new GenericRecordBuilder(schema);

        for(final org.apache.avro.Schema.Field field : schema.getFields()) {
            switch (field.name()) {
                // STRING
                case "id":
                case "driveId":
                case "name":
                case "description":
                case "originalFilename":
                case "kind":
                case "mimeType":
                case "fileExtension":
                case "fullFileExtension":
                case "resourceKey":
                case "webContentLink":
                case "webViewLink":
                case "iconLink":
                case "thumbnailLink":
                case "folderColorRgb":
                case "md5Checksum":
                case "headRevisionId":
                case "nextPageToken":
                    // BOOLEAN
                case "starred":
                case "trashed":
                case "explicitlyTrashed":
                case "viewedByMe":
                case "shared":
                case "ownedByMe":
                case "viewerCanCopyContent":
                case "writerCanShare":
                case "isAppAuthorized":
                case "hasThumbnail":
                case "modifiedByMe":
                case "hasAugmentedPermissions":
                    // INT64
                case "size":
                case "version":
                case "quotaBytesUsed":
                case "thumbnailVersion":
                    // STRING ARRAY
                case "parents":
                case "spaces":
                case "permissionIds":
                    builder.set(field.name(), file.get(field.name()));
                    break;
                // DATETIME
                case "createdTime":
                    builder.set(field.name(), file.getCreatedTime() == null ? null : Instant.parse(file.getCreatedTime().toStringRfc3339()).getMillis() * 1000L);
                    break;
                case "modifiedTime":
                    builder.set(field.name(), file.getModifiedTime() == null ? null : Instant.parse(file.getModifiedTime().toStringRfc3339()).getMillis() * 1000L);
                    break;
                case "viewedByMeTime":
                    builder.set(field.name(), file.getViewedByMeTime() == null ? null : Instant.parse(file.getViewedByMeTime().toStringRfc3339()).getMillis() * 1000L);
                    break;
                case "modifiedByMeTime":
                    builder.set(field.name(), file.getModifiedByMeTime() == null ? null : Instant.parse(file.getModifiedByMeTime().toStringRfc3339()).getMillis() * 1000L);
                    break;
                case "sharedWithMeTime":
                    builder.set(field.name(), file.getSharedWithMeTime() == null ? null : Instant.parse(file.getSharedWithMeTime().toStringRfc3339()).getMillis() * 1000L);
                    break;
                case "trashedTime":
                    builder.set(field.name(), file.getTrashedTime() == null ? null : Instant.parse(file.getTrashedTime().toStringRfc3339()).getMillis() * 1000L);
                    break;
                // User
                case "trashingUser": {
                    builder.set(field.name(), convertDriveUserRecord(file.getTrashingUser()));
                    break;
                }
                case "sharingUser":{
                    builder.set(field.name(), convertDriveUserRecord(file.getSharingUser()));
                    break;
                }
                case "lastModifyingUser":{
                    builder.set(field.name(), convertDriveUserRecord(file.getLastModifyingUser()));
                    break;
                }
                // User ARRAY
                case "owners":{
                    if(file.getOwners() == null) {
                        builder.set(field.name(), null);
                    } else {
                        final List<GenericRecord> owners = new ArrayList<>();
                        for(final User owner : file.getOwners()) {
                            if(owner != null) {
                                owners.add(convertDriveUserRecord(owner));
                            }
                        }
                        builder.set(field.name(), owners);
                    }
                    break;
                }
            }
        }
        return builder.build();
    }

    private static com.mercari.solution.module.Schema.FieldType convertFieldType2(final String field) {
        return switch (field) {
            case "id", "driveId", "name", "description", "originalFilename", "kind", "mimeType", "fileExtension",
                 "fullFileExtension", "resourceKey", "webContentLink", "webViewLink", "iconLink", "thumbnailLink",
                 "folderColorRgb", "md5Checksum", "headRevisionId", "nextPageToken"
                    -> com.mercari.solution.module.Schema.FieldType.STRING.withNullable(true);
            case "starred", "trashed", "explicitlyTrashed", "viewedByMe",
                 "shared", "ownedByMe", "viewerCanCopyContent", "writerCanShare",
                 "isAppAuthorized", "hasThumbnail", "modifiedByMe", "hasAugmentedPermissions"
                    -> com.mercari.solution.module.Schema.FieldType.BOOLEAN.withNullable(true);
            case "size", "version", "quotaBytesUsed", "thumbnailVersion"
                    -> com.mercari.solution.module.Schema.FieldType.INT64.withNullable(true);
            case "createdTime", "modifiedTime", "viewedByMeTime", "modifiedByMeTime", "sharedWithMeTime", "trashedTime"
                    -> com.mercari.solution.module.Schema.FieldType.TIMESTAMP.withNullable(true);
            case "parents", "spaces", "permissionIds"
                    -> com.mercari.solution.module.Schema.FieldType.array(com.mercari.solution.module.Schema.FieldType.STRING).withNullable(true);
            case "trashingUser", "sharingUser", "lastModifyingUser"
                -> com.mercari.solution.module.Schema.FieldType.element(createDriveUserSchema2()).withNullable(true);
            case "owners" -> com.mercari.solution.module.Schema.FieldType.array(com.mercari.solution.module.Schema.FieldType.element(createDriveUserSchema2())).withNullable(true);
            default -> throw new IllegalStateException("Not supported field: " + field);
        };
    }

    private static Schema.FieldType convertFieldType(final String field) {
        switch (field) {
            case "id":
            case "driveId":
            case "name":
            case "description":
            case "originalFilename":
            case "kind":
            case "mimeType":
            case "fileExtension":
            case "fullFileExtension":
            case "resourceKey":
            case "webContentLink":
            case "webViewLink":
            case "iconLink":
            case "thumbnailLink":
            case "folderColorRgb":
            case "md5Checksum":
            case "headRevisionId":
            case "nextPageToken":
                return Schema.FieldType.STRING.withNullable(true);
            case "starred":
            case "trashed":
            case "explicitlyTrashed":
            case "viewedByMe":
            case "shared":
            case "ownedByMe":
            case "viewerCanCopyContent":
            case "writerCanShare":
            case "isAppAuthorized":
            case "hasThumbnail":
            case "modifiedByMe":
            case "hasAugmentedPermissions":
                return Schema.FieldType.BOOLEAN.withNullable(true);
            case "size":
            case "version":
            case "quotaBytesUsed":
            case "thumbnailVersion":
                return Schema.FieldType.INT64.withNullable(true);
            case "createdTime":
            case "modifiedTime":
            case "viewedByMeTime":
            case "modifiedByMeTime":
            case "sharedWithMeTime":
            case "trashedTime":
                return Schema.FieldType.DATETIME.withNullable(true);
            case "parents":
            case "spaces":
            case "permissionIds":
                return Schema.FieldType.array(Schema.FieldType.STRING).withNullable(true);
            case "trashingUser":
            case "sharingUser":
            case "lastModifyingUser":
                return Schema.FieldType.row(createDriveUserSchema()).withNullable(true);
            case "owners":
                return Schema.FieldType.array(Schema.FieldType.row(createDriveUserSchema())).withNullable(true);
            default:
                throw new IllegalStateException("Not supported field: " + field);
        }
    }

    private static Schema createDriveUserSchema() {
        return Schema.builder()
                .addField("kind", Schema.FieldType.STRING.withNullable(true))
                .addField("displayName", Schema.FieldType.STRING.withNullable(true))
                .addField("photoLink", Schema.FieldType.STRING.withNullable(true))
                .addField("me", Schema.FieldType.BOOLEAN.withNullable(true))
                .addField("permissionId", Schema.FieldType.STRING.withNullable(true))
                .addField("emailAddress", Schema.FieldType.STRING.withNullable(true))
                .build();
    }

    private static com.mercari.solution.module.Schema createDriveUserSchema2() {
        return com.mercari.solution.module.Schema.builder()
                .withField("kind", com.mercari.solution.module.Schema.FieldType.STRING.withNullable(true))
                .withField("displayName", com.mercari.solution.module.Schema.FieldType.STRING.withNullable(true))
                .withField("photoLink", com.mercari.solution.module.Schema.FieldType.STRING.withNullable(true))
                .withField("me", com.mercari.solution.module.Schema.FieldType.BOOLEAN.withNullable(true))
                .withField("permissionId", com.mercari.solution.module.Schema.FieldType.STRING.withNullable(true))
                .withField("emailAddress", com.mercari.solution.module.Schema.FieldType.STRING.withNullable(true))
                .build();
    }


    private static org.apache.avro.Schema createDriveUserAvroSchema() {
        return SchemaBuilder
                .record("user")
                .namespace("")
                .fields()
                .name("kind").type(AvroSchemaUtil.NULLABLE_STRING).noDefault()
                .name("displayName").type(AvroSchemaUtil.NULLABLE_STRING).noDefault()
                .name("photoLink").type(AvroSchemaUtil.NULLABLE_STRING).noDefault()
                .name("me").type(AvroSchemaUtil.NULLABLE_BOOLEAN).noDefault()
                .name("permissionId").type(AvroSchemaUtil.NULLABLE_STRING).noDefault()
                .name("emailAddress").type(AvroSchemaUtil.NULLABLE_STRING).noDefault()
                .endRecord();
    }

    private static Map<String,Object> convertDriveUserMap(final User user) {
        if(user == null) {
            return null;
        }
        final Map<String, Object> values = new HashMap<>();
        values.put("kind", user.getKind());
        values.put("displayName", user.getDisplayName());
        values.put("photoLink", user.getPhotoLink());
        values.put("me", user.getMe());
        values.put("permissionId", user.getPermissionId());
        values.put("emailAddress", user.getEmailAddress());
        return values;
    }

    private static Row convertDriveUser(final User user) {
        if(user == null) {
            return null;
        }
        return Row.withSchema(createDriveUserSchema())
                .withFieldValue("kind", user.getKind())
                .withFieldValue("displayName", user.getDisplayName())
                .withFieldValue("photoLink", user.getPhotoLink())
                .withFieldValue("me", user.getMe())
                .withFieldValue("permissionId", user.getPermissionId())
                .withFieldValue("emailAddress", user.getEmailAddress())
                .build();
    }

    private static GenericRecord convertDriveUserRecord(final User user) {
        if(user == null) {
            return null;
        }
        return new GenericRecordBuilder(createDriveUserAvroSchema())
                .set("kind", user.getKind())
                .set("displayName", user.getDisplayName())
                .set("photoLink", user.getPhotoLink())
                .set("me", user.getMe())
                .set("permissionId", user.getPermissionId())
                .set("emailAddress", user.getEmailAddress())
                .build();
    }

}
