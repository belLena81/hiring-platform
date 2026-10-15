package com.example.graphQL.cats.service.resilience

/** The closed set of guarded dependencies; `wire` is the configuration key and the public log value. */
enum CircuitBreakerName(val wire: String) {
  case Voyage extends CircuitBreakerName("voyage")
  case AtlasSearch extends CircuitBreakerName("atlas-search")
  case KafkaPublisher extends CircuitBreakerName("kafka-publisher")
  case MongoWrites extends CircuitBreakerName("mongo-writes")
}
