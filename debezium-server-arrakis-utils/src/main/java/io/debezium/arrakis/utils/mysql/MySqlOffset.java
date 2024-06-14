package io.debezium.arrakis.utils.mysql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.connect.json.JsonConverterConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class MySqlOffset {

    private final Logger LOGGER = LoggerFactory.getLogger(MySqlOffset.class);

    private final String userName;
    private final String password;
    private final String hostname;
    private final int port;

    private static final BiFunction<String, String, String> SQL_SERVER_STATE_MUTATION = (key, databaseName) -> key.substring(0, key.length() - 2)
            + ",\"database\":\"" + databaseName + "\"" + key.substring(key.length() - 2);

    Map<String, String> INTERNAL_CONVERTER_CONFIG = Map.of(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, Boolean.FALSE.toString());

    public MySqlOffset(String userName, String password, String hostname, int port) {
        this.userName = userName;
        this.password = password;
        this.hostname = hostname;
        this.port = port;
    }

    public MysqlDebeziumStateAttributes getStateAttributesFromDB() throws SQLException {
        String url = String.format("jdbc:mysql://%s:%d/", hostname, port);
        try (Connection connection = DriverManager.getConnection(url, userName, password);
                Stream<MysqlDebeziumStateAttributes> stream = unsafeResultSetQuery(
                        connection,
                        "SHOW MASTER STATUS")) {
            List<MysqlDebeziumStateAttributes> stateAttributes = new ArrayList<>();
            stream.forEach(stateAttributes::add);
            if (stateAttributes.size() == 1) {
                return stateAttributes.get(0);
            }
            else {
                throw new RuntimeException("No master status found or multiple entries returned");
            }
        }
    }

    public List<String> getExistingLogFiles() throws SQLException {
        String url = String.format("jdbc:mysql://%s:%d/", hostname, port);
        try (Connection connection = DriverManager.getConnection(url, userName, password);
                Stream<String> stream = unsafeResultSetLogQuery(
                        connection,
                        "SHOW BINARY LOGS")) {
            List<String> stateAttributes = new ArrayList<>();
            stream.forEach(stateAttributes::add);
            return stateAttributes;
        }
        catch (Exception e) {
            throw new RuntimeException("No binary logs found");
        }
    }

    public boolean savedOffsetStillValid(String binlogFileName) throws SQLException {
        final List<String> existingLogFiles = getExistingLogFiles();
        final boolean found = existingLogFiles.stream().anyMatch(binlogFileName::equals);
        if (!found) {
            LOGGER.info("Connector requires binlog file '{}', but MySQL server only has {}", binlogFileName,
                    String.join(", ", existingLogFiles));
        }
        else {
            LOGGER.info("MySQL server has the binlog file '{}' required by the connector", binlogFileName);
        }

        return found;
    }

    private Stream<String> unsafeResultSetLogQuery(Connection connection, String query) throws SQLException {
        ResultSet resultSet = connection.createStatement().executeQuery(query);
        List<String> results = new ArrayList<>();
        while (resultSet.next()) { // Ensure you call next() before accessing the resultSet
            String file = resultSet.getString("Log_name");
            if (file != null) {
                results.add(file);
            }
        }
        return results.stream();
    }

    public Map<String, String> offsetAsMap(JsonNode cdcState, String dbName) {

        final ObjectMapper objectMapper = new ObjectMapper();

        final Map<String, String> mapAsString = cdcState != null ? objectMapper.convertValue(cdcState, Map.class) : Collections.emptyMap();

        final Map<String, String> updatedMap = new LinkedHashMap<>();
        if (mapAsString.size() > 0) {
            final String key = mapAsString.keySet().stream().collect(Collectors.toList()).get(0);
            final int i = key.indexOf('[');
            final int i1 = key.lastIndexOf(']');

            if (i == 0 && i1 == key.length() - 1) {
                // The state is Debezium 2.1 compatible. No need to change anything.
                return mapAsString;
            }

            final String newKey = SQL_SERVER_STATE_MUTATION.apply(key.substring(i, i1 + 1), dbName);
            final String value = mapAsString.get(key);
            updatedMap.put(newKey, value);
        }
        return updatedMap;

    }

    private Stream<MysqlDebeziumStateAttributes> unsafeResultSetQuery(Connection connection, String query) throws SQLException {
        ResultSet resultSet = connection.createStatement().executeQuery(query);
        List<MysqlDebeziumStateAttributes> results = new ArrayList<>();
        while (resultSet.next()) { // Ensure you call next() before accessing the resultSet
            String file = resultSet.getString("File");
            long position = resultSet.getLong("Position");
            if (file != null && position >= 0) {
                Optional<String> gtidSet = Optional.empty();
                if (resultSet.getMetaData().getColumnCount() > 4) {
                    gtidSet = Optional.ofNullable(removeNewLineChars(resultSet.getString(5)));
                }
                results.add(new MysqlDebeziumStateAttributes(file, position, gtidSet));
            }
        }
        return results.stream();
    }

    private String removeNewLineChars(String input) {
        return input == null ? null : input.replaceAll("[\\r\\n]", "");
    }

}
