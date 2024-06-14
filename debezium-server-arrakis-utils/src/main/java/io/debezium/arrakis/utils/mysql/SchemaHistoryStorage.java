package io.debezium.arrakis.utils.mysql;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Optional;
import java.util.zip.GZIPInputStream;

import org.apache.commons.io.FileUtils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.document.Document;
import io.debezium.document.DocumentReader;
import io.debezium.document.DocumentWriter;

public class SchemaHistoryStorage {

    private final Path path;
    private final boolean compressSchemaHistoryForState;

    private final ObjectMapper objectMapper = new ObjectMapper();

    private final DocumentReader reader = DocumentReader.defaultReader();
    private final DocumentWriter writer = DocumentWriter.defaultWriter();

    public SchemaHistoryStorage(final Path path, final boolean compressSchemaHistoryForState) {
        this.path = path;
        this.compressSchemaHistoryForState = compressSchemaHistoryForState;
    }

    public Path getPath() {
        return path;
    }

    private void makeSureFileExists() {
        try {
            // Make sure the file exists ...
            if (!Files.exists(path)) {
                // Create parent directories if we have them ...
                if (path.getParent() != null) {
                    Files.createDirectories(path.getParent());
                }
                try {
                    Files.createFile(path);
                }
                catch (final FileAlreadyExistsException e) {
                    // do nothing
                }
            }
        }
        catch (final IOException e) {
            throw new IllegalStateException(
                    "Unable to check or create history file at " + path + ": " + e.getMessage(), e);
        }
    }

    private void writeToFile(final String fileAsString) {
        try {
            final String[] split = fileAsString.split(System.lineSeparator());
            for (final String element : split) {
                final Document read = reader.read(element);
                final String line = writer.write(read);

                try (final BufferedWriter historyWriter = Files
                        .newBufferedWriter(path, StandardOpenOption.APPEND)) {
                    try {
                        historyWriter.append(line);
                        historyWriter.newLine();
                    }
                    catch (final IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
        catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void writeCompressedStringToFile(final String compressedString) {
        try (final ByteArrayInputStream inputStream = new ByteArrayInputStream(objectMapper.readValue(compressedString, byte[].class));
                final GZIPInputStream gzipInputStream = new GZIPInputStream(inputStream);
                final FileOutputStream fileOutputStream = new FileOutputStream(path.toFile())) {
            final byte[] buffer = new byte[1024];
            int bytesRead;
            while ((bytesRead = gzipInputStream.read(buffer)) != -1) {
                fileOutputStream.write(buffer, 0, bytesRead);
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void persist(final SchemaHistory<Optional<JsonNode>> schemaHistory) {
        if (schemaHistory.getSchema().isEmpty()) {
            return;
        }
        final String fileAsString = objectMapper.convertValue(schemaHistory.getSchema().get(), String.class);

        if (fileAsString == null || fileAsString.isEmpty()) {
            return;
        }

        FileUtils.deleteQuietly(path.toFile());
        makeSureFileExists();
        if (schemaHistory.isCompressed()) {
            writeCompressedStringToFile(fileAsString);
        }
        else {
            writeToFile(fileAsString);
        }
    }

    public static SchemaHistoryStorage initializeDBHistory(final SchemaHistory<Optional<JsonNode>> schemaHistory, boolean compressSchemaHistoryForState) {
        final Path dbHistoryWorkingDir;
        try {
            dbHistoryWorkingDir = Files.createTempDirectory(Path.of("/tmp"), "cdc-db-history");
        }
        catch (final IOException e) {
            throw new RuntimeException(e);
        }
        final Path dbHistoryFilePath = dbHistoryWorkingDir.resolve("dbhistory.dat");

        final SchemaHistoryStorage schemaHistoryManager = new SchemaHistoryStorage(dbHistoryFilePath, compressSchemaHistoryForState);
        schemaHistoryManager.persist(schemaHistory);
        return schemaHistoryManager;
    }

}
