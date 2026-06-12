import { Logger, ValidationPipe } from "@nestjs/common"
import { NestFactory } from "@nestjs/core"
import { AppModule } from "./app.module"

/**
 * Bootstraps the ingest-api HTTP producer.
 *
 * Binds to `0.0.0.0` so the container is reachable from the host; defaults to
 * port 3000 but honours `PORT` for flexibility.
 */
async function bootstrap(): Promise<void> {
    const app = await NestFactory.create(AppModule)
    // Reject unknown fields so the click contract stays strict.
    app.useGlobalPipes(
        new ValidationPipe({ whitelist: true, forbidNonWhitelisted: true, transform: true }),
    )
    const port = Number(process.env.PORT ?? 3000)
    // Bind 0.0.0.0 — inside Docker the default 127.0.0.1 would refuse host curl.
    await app.listen(port, "0.0.0.0")
    new Logger("Bootstrap").log(`ingest-api listening on :${port}`)
}

void bootstrap()
