package com.starci.ingest;

import java.util.Map;

/**
 * Request body for {@code POST /clicks}.
 *
 * <p>{@code clickId} is the idempotency key: re-sending the same {@code clickId}
 * (retry, double-fire) must be counted once by the consumer, so the producer
 * treats it as a first-class field rather than free-form data. It is optional so
 * the API can mint one when omitted. {@code adId} is required and also serves as
 * the Kafka partition key. {@code payload} is arbitrary contextual data.
 */
public class PublishClickDto {
    public String clickId;
    public String adId;
    public Map<String, Object> payload;
}
