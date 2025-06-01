from kafka import KafkaProducer
import json
import pickle

def produce_json_message(bootstrap_servers: str, topic: str, message: dict) -> bool:
    """
    Produce a JSON message to a Kafka topic.
    
    Args:
        bootstrap_servers: Kafka broker addresses
        topic: Target topic name
        message: Dictionary to be sent as JSON
        
    Returns:
        True if message was sent successfully
    """
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        future = producer.send(topic, message)
        producer.flush()
        return future.is_done
    except Exception as e:
        print(f"Error producing message: {e}")
        return False
    
def produce_string_message(bootstrap_servers: str, topic: str, message: str) -> bool:
    """
    Produce a string message to a Kafka topic.
    
    Args:
        bootstrap_servers: Kafka broker addresses
        topic: Target topic name
        message: String message to be sent
        
    Returns:
        True if message was sent successfully
    """
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: v.encode('utf-8')
        )
        future = producer.send(topic, message)
        producer.flush()
        return future.is_done
    except Exception as e:
        print(f"Error producing message: {e}")
        return False
    
def change_serializer(bootstrap_servers: str, serializer_type: str = 'json') -> KafkaProducer:
    """
    Create a Kafka producer with different serializers.
    
    Args:
        bootstrap_servers: Kafka broker addresses
        serializer_type: Type of serializer ('json', 'string', 'binary', 'pickle')
        
    Returns:
        Configured KafkaProducer instance
        
    Raises:
        ValueError: If invalid serializer_type is provided
    """
    if serializer_type == 'json':
        return KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
    elif serializer_type == 'string':
        return KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: v.encode('utf-8')
        )
    elif serializer_type == 'binary':
        return KafkaProducer(bootstrap_servers=bootstrap_servers)
    elif serializer_type == 'pickle':
        return KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: pickle.dumps(v)
        )
    else:
        raise ValueError(f"Unsupported serializer type: {serializer_type}")
    
def get_acknowledge(bootstrap_servers: str, ack_setting: str = 'all') -> KafkaProducer:
    """
    Create a Kafka producer with different acknowledgement settings.
    
    Args:
        bootstrap_servers: Kafka broker addresses
        ack_setting: Acknowledgement setting ('all', 'leader', 'none')
        
    Returns:
        Configured KafkaProducer instance
        
    Raises:
        ValueError: If invalid ack_setting is provided
    """
    valid_settings = {'all', 'leader', 'none'}
    if ack_setting not in valid_settings:
        raise ValueError(f"Invalid ack setting. Must be one of {valid_settings}")
    
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        acks=ack_setting
    )