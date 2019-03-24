# Collector Service

Application Spring Boot that collect data from public dataset of Malaga.

This service belong to MLGIA.

This service is configured to start in port 8084. It does not contains any exposed endpoint but it contains a scheduled method to collect data each 10 minutes.

### Build Docker image
```
mvn clean package docker:build
```
