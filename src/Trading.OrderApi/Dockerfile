# Build context is the repository root (see docker-compose.yml).
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src
COPY global.json ./
# Restore first, in its own layer, so it is cached until a .csproj changes.
COPY src/Trading.Contracts/Trading.Contracts.csproj src/Trading.Contracts/
COPY src/Trading.OrderApi/Trading.OrderApi.csproj src/Trading.OrderApi/
RUN dotnet restore src/Trading.OrderApi/Trading.OrderApi.csproj
COPY src/ src/
RUN dotnet publish src/Trading.OrderApi/Trading.OrderApi.csproj -c Release -o /app --no-restore

FROM mcr.microsoft.com/dotnet/aspnet:8.0
WORKDIR /app
COPY --from=build /app .
USER app
EXPOSE 8080
ENTRYPOINT ["dotnet", "Trading.OrderApi.dll"]
