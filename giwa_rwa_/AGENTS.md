# AGENTS
This repository is organized into shared assets, smart-contract tooling, backend APIs, an event ingester, Flink processing, and local infrastructure.

- shared: cross-module constants, ABIs, deployment addresses, DB schema, and event signature lists.
- contracts: Hardhat project for Solidity contracts and tests.
- server: Spring Boot API server built with Gradle.
- ingester: Kafka producer ingester built with Gradle.
- flink: Flink consumer/processor job built with Gradle.
- infra: local docker-compose for Postgres and Kafka.
