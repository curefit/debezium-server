package io.debezium.server.events;

public class NoRecordsReadEvent {
    private final String message;

    public NoRecordsReadEvent(String message) {
        this.message = message;
    }

    public String getMessage() {
        return message;
    }

}
