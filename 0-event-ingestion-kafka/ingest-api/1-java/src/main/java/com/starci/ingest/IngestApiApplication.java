package com.starci.ingest;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Boots the ingest-api HTTP producer.
 *
 * <p>It owns no counting logic: it only guarantees every click event carries a
 * stable {@code clickId} so the downstream consumer can deduplicate.
 */
@SpringBootApplication
public class IngestApiApplication {
    public static void main(String[] args) {
        SpringApplication.run(IngestApiApplication.class, args);
    }
}
