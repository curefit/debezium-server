package io.debezium.arrakis.utils;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.DebeziumException;

public class StateManager {

    private final Logger LOGGER = LoggerFactory.getLogger(StateManager.class);

    private final Config config = ConfigProvider.getConfig();

    private JsonNode saveState(String url, JsonNode state) throws IOException, InterruptedException, RuntimeException {
        // Create HttpClient
        HttpClient client = HttpClient.newHttpClient();
        LOGGER.info("Sending PUT request to: " + url);

        // Convert state JsonNode to String
        ObjectMapper objectMapper = new ObjectMapper();
        String requestBody = objectMapper.writeValueAsString(state);

        // Create HttpRequest with PUT method and JSON body
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Content-Type", "application/json")
                .header("Accept", "application/json")
                .PUT(HttpRequest.BodyPublishers.ofString(requestBody))
                .build();

        // Send request and get response
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        // Check if the response status is 200 (OK)
        if (response.statusCode() != 200) {
            throw new RuntimeException("Failed to get response: "
                    + response.statusCode()
                    + " " + response.body()
                    + " for url: " + url);
        }

        // Convert response body to JsonNode
        return objectMapper.readValue(response.body(), JsonNode.class);
    }

    public boolean saveState(JsonNode state) throws IOException, InterruptedException {

        String ARRAKIS_URL = "arrakis.backend.url";
        String ARRAKIS_PIPE_ID = "arrakis.pipeline.id";
        String SAVE_STATE_API = "/api/v1/pipes/";

        try {
            JsonNode savedState = saveState("http://" + config.getValue(ARRAKIS_URL, String.class)
                    + ":9094"
                    + SAVE_STATE_API
                    + config.getValue(ARRAKIS_PIPE_ID, String.class)
                    + "/state",
                    state);

            LOGGER.info("Saved state: " + savedState.toString());
            return true;
        }
        catch (IOException | InterruptedException e) {
            throw new DebeziumException("Failed to save state: " + e.getMessage(), e);
        }

    }

}
