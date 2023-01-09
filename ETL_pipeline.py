import argparse
import configparser
from datetime import datetime
import json
import os
import boto3
import logging
import base64
import psycopg2

logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)

class ETL_pipeline():

    def __init__(self, queue_name, endpoint_url, max_num_messages, wait_time_seconds):
        config = configparser.ConfigParser()
        config.read("postgres.ini")

        self.__queue_name = queue_name
        self.__endpoint_url = endpoint_url
        self.__wait_time_seconds = wait_time_seconds

        self.__postgres_username = config["postgres"]["username"]
        self.__postgres_password = config["postgres"]["password"]
        self.__postgres_host = config["postgres"]["host"]
        self.__postgres_port = config["postgres"]["port"]
        self.__postgres_database = config["postgres"]["database"]

        self.messages = []
    
    def process_pii_data(self, message, func="encode"):
        if func == "encode":
            message_bytes = message.encode("ascii")
            encoded = base64.b64encode(message_bytes)
            return encoded
        elif func == "decode":
            decoded_bytes = base64.b64decode(message)
            decoded = decoded_bytes.decode("utf-8")
            return decoded
    
    def process_message(self, message):
        logger.info(f"Processing message: {message}")
        processed_message = {}
        message_dict = json.loads(message)
        for k, v in message_dict.items():
            if k == "device_id":
                processed_message["masked_device_id"] = self.process_pii_data(v, func="encode")
            elif k == "ip":
                processed_message["masked_ip"] = self.process_pii_data(v, func="encode")
            elif k == "app_version":
                processed_message["app_version"] = int(v.split(".")[0])
            else:
                processed_message[k] = v
        self.messages.append(processed_message)

    def consume_data(self):
        logger.info("SQS Consumer starting ...")
        logger.info(f"Subscribing to queue {self.__queue_name}")
        sqs = boto3.resource("sqs", endpoint_url=self.__endpoint_url)
        queue = sqs.get_queue_by_name(QueueName=self.__queue_name)

        try:
            messages = queue.receive_messages(
                MaxNumberOfMessages=100,
                WaitTimeSeconds=self.__wait_time_seconds
            )
            logging.info("Messages received")
            logging.info("Length of messages: %s", len(messages))
            for message in messages:
                try:
                    self.process_message(message.body)
                except Exception as e:
                    print(f"Exception while processing message: {repr(e)}")
                    continue
                message.delete()
        except Exception as e:
            print(f"Exception while consuming messages: {repr(e)}")
    
    def feed_postgres(self):
        logger.info("Postgres feeder starting ...")
        logger.info(f"Feeding data to Postgres")
        conn = psycopg2.connect(
            host=self.__postgres_host,
            port=self.__postgres_port,
            database=self.__postgres_database,
            user=self.__postgres_username,
            password=self.__postgres_password
        )

        conn.autocommit = True
        cursor = conn.cursor()

        for message in self.messages:
            if "user_id" not in message:
                continue
            message["create_date"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            cursor.execute(
                "INSERT INTO user_logins (user_id, device_type, masked_ip, masked_device_id, locale, app_version, create_date) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                (message["user_id"], message["device_type"], message["masked_ip"], message["masked_device_id"],
                message["locale"], message["app_version"], message["create_date"])
            )
        
        conn.commit()
        conn.close()
    
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--queue_name", type=str, required=True)
    parser.add_argument("--endpoint_url", type=str, required=True)
    parser.add_argument("--max_num_messages", type=int, default=100)
    parser.add_argument("--wait_time_seconds", type=int, default=20)
    args = parser.parse_args()

    queue_name = args.queue_name
    endpoint_url = args.endpoint_url
    max_num_messages = args.max_num_messages
    wait_time_seconds = args.wait_time_seconds
    
    etl_pipeline = ETL_pipeline(
        queue_name=queue_name,
        endpoint_url=endpoint_url,
        max_num_messages=max_num_messages,
        wait_time_seconds=wait_time_seconds
    )
    etl_pipeline.consume_data()
    etl_pipeline.feed_postgres()

if __name__ == "__main__":
    main()
