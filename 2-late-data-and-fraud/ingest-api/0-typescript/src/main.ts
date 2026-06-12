import { NestFactory } from "@nestjs/core"
import { ValidationPipe } from "@nestjs/common"
import { AppModule } from "./app.module"

/**
 * Bootstrap the ingest-api HTTP server.
 *
 * Listens on the port provided by the PORT environment variable (default 3000).
 * Inside Docker Compose the binding is 0.0.0.0; Docker bridge provides isolation.
 */
async function bootstrap(): Promise<void> {
    const app = await NestFactory.create(AppModule)
    app.useGlobalPipes(
        new ValidationPipe({ whitelist: true, forbidNonWhitelisted: true }),
    )
    const port = parseInt(process.env.PORT ?? "3000", 10)
    // Bind 0.0.0.0 so the host can reach the container across the Docker bridge.
    await app.listen(port, "0.0.0.0")
}
bootstrap()
