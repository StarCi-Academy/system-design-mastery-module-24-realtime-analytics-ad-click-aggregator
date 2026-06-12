import {
    IsOptional,
    IsString,
    IsNotEmpty,
    IsObject,
} from "class-validator"

/**
 * Validated payload for `POST /clicks`.
 *
 * `clickId` is optional — the service mints a UUID when absent.
 * `eventTime` is optional — defaults to server wall-clock; can be overridden
 *   to simulate back-dated events for late-data and watermark testing.
 * `payload` is a free-form object; include `ip` inside it to enable
 *   per-IP fraud detection in the processor.
 */
export class PublishClickDto {
    @IsOptional()
    @IsString()
    clickId?: string

    @IsNotEmpty()
    @IsString()
    adId!: string

    @IsOptional()
    @IsString()
    eventTime?: string

    @IsOptional()
    @IsObject()
    payload?: Record<string, unknown>
}
