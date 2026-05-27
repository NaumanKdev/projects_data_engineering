"""
Kafka Producer for real-time event streaming
Generates events from multiple sources and streams to Kafka topics
"""

import json
import logging
from datetime import datetime, timedelta
from typing import Dict, Any
from kafka import KafkaProducer
from kafka.errors import KafkaError
import time
import random
from dataclasses import dataclass, asdict

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class Event:
    """Data class for event structure"""
    event_id: str
    event_type: str
    user_id: str
    timestamp: str
    properties: Dict[str, Any]
    source: str

class KafkaEventProducer:
    """Producer for streaming events to Kafka"""
    
    def __init__(self, bootstrap_servers: str = "localhost:9092"):
        """Initialize Kafka producer"""
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',  # Wait for all replicas
            retries=3,
            max_in_flight_requests_per_connection=1,  # Ensure ordering
        )
        self.logger = logger
        
    def send_event(self, topic: str, event: Dict[str, Any], key: str = None) -> bool:
        """Send event to Kafka topic with error handling"""
        try:
            future = self.producer.send(
                topic, 
                value=event,
                key=key.encode('utf-8') if key else None
            )
            
            # Wait for send to complete
            record_metadata = future.get(timeout=10)
            
            self.logger.info(
                f"Event sent: topic={record_metadata.topic}, "
                f"partition={record_metadata.partition}, "
                f"offset={record_metadata.offset}"
            )
            return True
            
        except KafkaError as e:
            self.logger.error(f"Failed to send event: {e}")
            return False
    
    def send_batch_events(self, topic: str, events: list) -> int:
        """Send batch of events"""
        success_count = 0
        for event in events:
            if self.send_event(topic, event, key=event.get('user_id')):
                success_count += 1
        
        self.logger.info(f"Batch sent: {success_count}/{len(events)} events")
        return success_count
    
    def close(self):
        """Close producer connection"""
        self.producer.flush()
        self.producer.close()


class EventGenerator:
    """Generate synthetic events for testing"""
    
    EVENT_TYPES = ['page_view', 'click', 'purchase', 'login', 'logout', 'search']
    SOURCES = ['web', 'mobile', 'api']
    
    @staticmethod
    def generate_event(event_id: str, user_id: str) -> Dict[str, Any]:
        """Generate random event"""
        event_type = random.choice(EventGenerator.EVENT_TYPES)
        
        event = Event(
            event_id=event_id,
            event_type=event_type,
            user_id=user_id,
            timestamp=datetime.utcnow().isoformat(),
            properties={
                'source': random.choice(EventGenerator.SOURCES),
                'session_id': f"session_{random.randint(1000, 9999)}",
                'user_agent': 'Mozilla/5.0',
                'ip_address': f"192.168.{random.randint(1,255)}.{random.randint(1,255)}",
            },
            source=random.choice(EventGenerator.SOURCES)
        )
        
        return asdict(event)


def simulate_event_stream(producer: KafkaEventProducer, topic: str, duration_seconds: int = 60):
    """Simulate continuous event stream"""
    logger.info(f"Starting event stream simulation for {duration_seconds}s...")
    
    start_time = time.time()
    event_count = 0
    
    try:
        while time.time() - start_time < duration_seconds:
            # Generate batch of events
            batch_size = random.randint(10, 50)
            events = []
            
            for i in range(batch_size):
                event_id = f"evt_{datetime.utcnow().timestamp()}_{i}"
                user_id = f"user_{random.randint(1, 10000)}"
                event = EventGenerator.generate_event(event_id, user_id)
                events.append(event)
            
            # Send batch
            sent = producer.send_batch_events(topic, events)
            event_count += sent
            
            # Small delay to simulate realistic event rate
            time.sleep(1)
    
    except KeyboardInterrupt:
        logger.info("Stream simulation interrupted")
    
    finally:
        producer.close()
        logger.info(f"Total events sent: {event_count}")


if __name__ == "__main__":
    # Example usage
    producer = KafkaEventProducer(bootstrap_servers="localhost:9092")
    
    # Single event
    sample_event = EventGenerator.generate_event("evt_001", "user_123")
    producer.send_event("events", sample_event, key="user_123")
    
    # Stream simulation
    # simulate_event_stream(producer, "events", duration_seconds=60)
