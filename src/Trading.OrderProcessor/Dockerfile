# Build context is the repository root (see docker-compose.yml).
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src
COPY global.json ./
# Restore first, in its own layer, so it is cached until a .csproj changes.
COPY src/Trading.Contracts/Trading.Contracts.csproj src/Trading.Contracts/
COPY src/Trading.OrderProcessor/Trading.OrderProcessor.csproj src/Trading.OrderProcessor/
RUN dotnet restore src/Trading.OrderProcessor/Trading.OrderProcessor.csproj
COPY src/ src/
RUN dotnet publish src/Trading.OrderProcessor/Trading.OrderProcessor.csproj -c Release -o /app --no-restore

FROM mcr.microsoft.com/dotnet/runtime:8.0
WORKDIR /app
COPY --from=build /app .
USER app
ENTRYPOINT ["dotnet", "Trading.OrderProcessor.dll"]
