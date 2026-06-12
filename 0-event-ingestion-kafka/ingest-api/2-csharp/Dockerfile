# Multi-stage build: publish a self-contained app, ship it on the ASP.NET runtime.
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS builder
WORKDIR /app
COPY *.csproj ./
RUN dotnet restore
COPY . .
RUN dotnet publish -c Release -o /out

FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS runtime
WORKDIR /app
COPY --from=builder /out ./
EXPOSE 3000
ENTRYPOINT ["dotnet", "IngestApi.dll"]
