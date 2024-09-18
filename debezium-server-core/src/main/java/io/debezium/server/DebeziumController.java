package io.debezium.server;

import java.time.Instant;

import jakarta.annotation.PostConstruct;
import jakarta.enterprise.event.Event;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.server.events.NoRecordsReadEvent;
import io.quarkus.scheduler.Scheduled;

@Path("/debezium")
public class DebeziumController {

    private static final Logger LOGGER = LoggerFactory.getLogger(DebeziumController.class);

    @Inject
    DebeziumServer debeziumServer;

    public Instant serverStartTime;

    @Inject
    Event<NoRecordsReadEvent> debeziumEvent;

    @PostConstruct
    void start() {
        LOGGER.info("Debezium Controller started.");
        serverStartTime = Instant.now();
    }

    // REST API to manually trigger check
    @GET
    @Path("/check")
    @Produces(MediaType.TEXT_PLAIN)
    public String checkDebezium() {
        return checkAndFireEvent();
    }

    // Scheduled to run every 5 minutes
    @Scheduled(every = "3m")
    void scheduledCheck() {
        checkAndFireEvent();
    }

    // Method to check the variable and fire an event if needed
    private String checkAndFireEvent() {
        if (serverStartTime == null) {
            LOGGER.info("Debezium server is not started yet.");
            return "Debezium server is not started yet.";
        }

        LOGGER.info("Records received last time: " + debeziumServer.lastTimeRecordsReceived);
        LOGGER.info("Server Start time: " + serverStartTime);
        Instant twoMinutesAgo = Instant.now().minusSeconds(180);
        if ((debeziumServer.lastTimeRecordsReceived == null
                && (serverStartTime.isBefore(twoMinutesAgo) ||
                        serverStartTime.equals(twoMinutesAgo)))
                || (debeziumServer.lastTimeRecordsReceived != null &&
                        debeziumServer.lastTimeRecordsReceived.isBefore(Instant.now().minusSeconds(180)))) {
            LOGGER.info("Records are not read for more than 2.5m mins. Firing event...");
            debeziumEvent.fire(new NoRecordsReadEvent("Event fired: No Records sent to Kafka for last 2.5 minutes."));
            return "Periodic check: No Records sent to Kafka for last 2.5 minutes.";
        }
        else {
            LOGGER.info("Debezium server is running fine.");
            return "Debezium server is running fine.";
        }
    }
}
