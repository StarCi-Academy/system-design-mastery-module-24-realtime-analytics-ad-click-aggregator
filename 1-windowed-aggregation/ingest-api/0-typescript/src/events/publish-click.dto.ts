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
 */
export class PublishClickDto {
    @IsOptional()
    @IsString()
    clickId?: string

    @IsNotEmpty()
    @IsString()
    adId!: string

    @IsOptional()
    @IsObject()
    payload?: Record<string, unknown>
}
