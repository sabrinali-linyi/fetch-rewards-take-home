## To run the program

1. Clone the repo
2. `cd` into the repo
3. Install the dependencies using \
`pip install -r requirements.txt`
4. Start the server using `docker-compose up -d`
5. Run the program using \
`python ETL_pipeline.py --queue_name login-queue --endpoint_url http://localhost:4566/000000000000/ --wait_time_seconds 20` \
Feel free to customize `wait_time_seconds` as per your requirement.\
The program will perform all the steps mentioned in the problem statement, including reading from an AWS SQS Queue, transforming that data, and then writing to a Postgres database.
5. Use the following command to verify the messages are fed into the database table \
`psql -d postgres -U postgres -p 5432 -h localhost -W`\
`select * from user_logins;`
6. Use `docker-compose down` to stop the server and remove the containers

## Answers to the questions

### 1. How would you deploy this application in production?
We can use a containerization tool like Docker to containerize the application. We can then use a container orchestration tool like Kubernetes to deploy the application. We can use a CI/CD tool like Jenkins to automate the deployment process. We can also use a cloud service like AWS to deploy the application. We could then schedule the ETL application to run at a specific time interval using a cron job.
### 2. What other components would you want to add to make this production ready?
Before shipping the application, we should ensure that the application is fully tested by writing unit tests and integration tests. Besides, we can add a monitoring tool like Prometheus to monitor the application. We could add a logging tool to record log files, performance metrics, and any error messages that are generated by the ETL process. We can also add a caching layer like Redis to cache the data.
### 3. How can this application scale with a growing dataset.
We can use a distributed database like Cassandra to store the data. We can use a distributed message queue like Kafka to store the data. We can use a distributed file system like HDFS to store the data. We can also use a distributed processing framework like Spark to process the data.
### 4. How can PII be recovered later on?
Since we use `base64` to encode the PII data, we can use the same to decode the data.
### 5. What are the assumptions you made?
I assumed that all fields except for `ip` and `device_id` are not confidential and do not require any encryption. I assumed that the data in the `user_logins` table is not sensitive and can be made public.\
I assumed that the total number of messages in the queue is 100 (as mentioned in the problem statement), and that the messages are stored in the queue in the order in which they were sent.\
Since the data type of `app_version` is an integer as defined by `user_logins` DDL, I assumed that it is okay to only keep the number before the first decimal point as the version number.

