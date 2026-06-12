# Multi-stage build: compile with the full toolchain, ship a slim runtime image.
FROM node:22-alpine AS builder
WORKDIR /app
COPY package*.json ./
# Install all deps (incl. dev) so `nest build` can run.
RUN npm install
COPY tsconfig.json nest-cli.json ./
COPY src ./src
RUN npm run build

FROM node:22-alpine AS runtime
WORKDIR /app
COPY package*.json ./
# Production-only deps keep the final image small.
RUN npm install --omit=dev
COPY --from=builder /app/dist ./dist
EXPOSE 3002
CMD ["node", "dist/main.js"]
