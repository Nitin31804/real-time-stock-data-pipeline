from datetime import datetime

import producer as producer_module


class FakeProducer:
    def __init__(self):
        self.calls = []

    def produce(self, **kwargs):
        self.calls.append(kwargs)

    def poll(self, timeout):
        assert timeout == 0


def test_gbm_generator_produces_positive_well_formed_ticks():
    generator = producer_module.GBMMockGenerator(["AAPL"])

    tick = generator.next_tick("AAPL")

    assert tick["symbol"] == "AAPL"
    assert tick["price"] > 0
    assert tick["volume"] > 0
    assert tick["source"] == "simulation_gbm"
    assert datetime.fromisoformat(tick["timestamp"]).tzinfo is not None


def test_publish_tick_keys_kafka_messages_by_symbol():
    fake = FakeProducer()
    tick = {
        "event_id": "38cbaf34-dd28-4199-b758-ef198dbb3190",
        "symbol": "MSFT",
        "price": 410.0,
        "volume": 3,
        "timestamp": "2026-01-01T00:00:00+00:00",
        "source": "test",
    }

    producer_module.publish_tick(fake, tick)

    assert len(fake.calls) == 1
    assert fake.calls[0]["topic"] == producer_module.KAFKA_TOPIC
    assert fake.calls[0]["key"] == b"MSFT"
    assert b'"event_id"' in fake.calls[0]["value"]
