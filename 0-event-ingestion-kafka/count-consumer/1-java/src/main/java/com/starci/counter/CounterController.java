package com.starci.counter;

import java.util.Map;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Exposes the running counts over HTTP so the e2e test and learners can observe
 * the deduplicated totals without scraping logs.
 */
@RestController
public class CounterController {

    private final CounterService counter;

    public CounterController(CounterService counter) {
        this.counter = counter;
    }

    /** Returns the current per-ad counts and duplicate tally. */
    @GetMapping("/counts")
    public Map<String, Object> counts() {
        return counter.snapshot();
    }
}
