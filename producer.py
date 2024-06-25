from confluent_kafka import Producer
import socket
import random
import time

import json
import requests


KAFKA_TOPIC = 'jokes'
KAFKA_BOOTSTRAP_SERVERS = ':61941'

url = 'https://icanhazdadjoke.com/'

# Fetch a random dad joke with each API call from the Dad Joke API using the fetch a random dad joke endpoint.
# Check for errors, if no errors, print topic & partition
def delivery_callback(err, msg):
    if err:
        print('%% Message failed delivery: %s' % err)

    else:
        print('%% Message delivered to %s [%d]' % (msg.topic(), msg.partition()))

def get_API(url):
    # Set headers to get response in JSON
    headers = {"Accept": "application/json"}
    r = requests.get(url, headers=headers)
    if r.status_code == 200:
      print("Joke received successfully.")
      return r.json()['joke']
    else:
      print("Failed to get joke.")
      return None


def main():
    # Kafka producer configuration
    conf = {'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'client.id':socket.gethostname()} # Connect ports using socket

    # Create producer instance
    producer = Producer(conf)

    try:
      while True:
        # Get a random dad joke
        random_joke = get_API(url)

        if random_joke:
        # Send the joke to Kafka
          producer.produce(KAFKA_TOPIC, value=random_joke, on_delivery=delivery_callback)
          producer.flush()

        # Wait 5 seconds to loop again
        time.sleep(5)
    except KeyboardInterrupt:
            print('%% Aborted by user')
    finally:
      producer.close()

if __name__ == '__main__':
    main()


