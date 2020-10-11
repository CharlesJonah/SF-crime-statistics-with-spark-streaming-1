from kafka import KafkaConsumer
import time
import sys

# Kafka consumer to test data moved to topic
class ConsumerServer(KafkaConsumer):
    
    def __init__(self, topic_name):
        # Configure a Kafka Consumer
        self.consumer = KafkaConsumer(
                        topic_name,
                        bootstrap_servers = "localhost:9092", 
                        request_timeout_ms = 1000, 
                        auto_offset_reset="earliest"
                        )   
    
    def consume(self):
        try:
             while True:
                for metadata,message in self.consumer.poll().items():
                    if message is None:
                        print ("no message received by consumer")
                    #elif message.error() is not None:
                    #    print (f"error from consumer {message.error()}") 
                    else:
                        for record in message:
                            print(record.value)
                            time.sleep(0.1)
                time.sleep(0.01)
        except:
            print("Closing consumer", sys.exc_info()[0])
            self.consumer.close()
            
if __name__ == "__main__":
    consumer = ConsumerServer("sf.crime.statistics.topic")
    consumer.consume()