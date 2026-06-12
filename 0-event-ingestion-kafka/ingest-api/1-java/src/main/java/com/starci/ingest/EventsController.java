package com.starci.ingest;

import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * HTTP entrypoint for click ingestion.
 *
 * <p>One route only: {@code POST /clicks}. The 202 status code makes the
 * fire-and-forget contract explicit — the event is accepted into the log, not
 * yet processed.
 */
@RestController
@RequestMapping("/clicks")
public class EventsController {

    private final EventsService events;

    public EventsController(EventsService events) {
        this.events = events;
    }

    /**
     * Accepts a click event and appends it to the Kafka topic.
     *
     * @param dto validated click payload
     * @return HTTP 202 with {@code {status, topic, clickId, adId}}
     */
    @PostMapping
    public ResponseEntity<Map<String, String>> publish(@RequestBody PublishClickDto dto) throws Exception {
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(events.publish(dto));
    }
}
