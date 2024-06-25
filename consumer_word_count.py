from confluent_kafka import Consumer, KafkaError, KafkaException
import sys
import json

KAFKA_TOPIC = 'jokes'
KAFKA_BOOTSTRAP_SERVERS = ':61941'

# Running trackers of jokes & total words
num_jokes = 0
total_words = 0

# Output avergae word count every 5 jokes
def update_inventory(message):
    global num_jokes  # Access global inventory variable
    global total_words

    # Make joke a string
    joke = str(message.value().decode('utf-8'))
    num_jokes += 1
    total_words += len(joke.split())

    if num_jokes % 5 == 0:
        # Makes sense to round the number
        avg_words = round(total_words / num_jokes, 0)
        print(f"Average words per joke: {avg_words}")

def main():
    conf = {'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'group.id': 'joke_inventory',
            'auto.offset.reset': 'earliest'}
    consumer = Consumer(conf)

    try:
      consumer.subscribe([KAFKA_TOPIC])

      while True:
          message = consumer.poll(timeout=1.0)
          if message is None:
              continue

          if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition event
                sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                (msg.topic(), msg.partition(), msg.offset()))
              elif msg.error():
                raise KafkaException(msg.error())
            else: 
              update_inventory(message)
      
    except KeyboardInterrupt:
      sys.stderr.write('%% Aborted by user\n')

    finally:
      consumer.close()

if __name__ == '__main__':
    main()