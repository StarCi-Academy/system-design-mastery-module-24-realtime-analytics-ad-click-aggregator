import { registerAs } from "@nestjs/config"

/** ClickHouse connection parameters. */
export interface ClickHouseConfig {
    url: string
    database: string
    username: string
    password: string
}

/**
 * Reads ClickHouse settings from environment variables with safe defaults.
 */
export default registerAs(
    "clickhouse",
    (): ClickHouseConfig => ({
        url: process.env.CLICKHOUSE_URL ?? "http://localhost:8123",
        database: process.env.CLICKHOUSE_DATABASE ?? "default",
        username: process.env.CLICKHOUSE_USERNAME ?? "default",
        password: process.env.CLICKHOUSE_PASSWORD ?? "",
    }),
)
