import { IsNotEmpty, IsObject, IsOptional, IsString } from "class-validator"

/**
 * Request body for `POST /clicks`.
 *
 * `clickId` is the idempotency key: the consumer uses it to drop duplicates,
 * so the producer treats it as a first-class field rather than free-form data.
 */
export class PublishClickDto {
    /**
     * Client-generated unique id for this click (idempotency key).
     *
     * Re-sending the same `clickId` (retry, double-fire) must be counted once
     * by the consumer. Optional here so the API can mint one when omitted.
     */
    @IsString()
    @IsOptional()
    clickId?: string

    /** Ad identifier the click belongs to; also used as the Kafka partition key. */
    @IsString()
    @IsNotEmpty()
    adId!: string

    /** Arbitrary contextual payload (device, geo, referrer...). */
    @IsObject()
    @IsOptional()
    payload?: Record<string, unknown>
}
