# custom-kafka-connector
This repository is intended to serve as a sandbox where I can experiment and learn how to create robust custom Kafka connectors.


## Quickstart
* Build the connector tar file
* Start the test environment:
    * docker compose up -d
* Check if the custom connector is read by the connect cluster:
    * curl --url 'http://localhost:8083/connector' -k
* Push Connector configuration to start the connector:
    * curl -X POST -H "Content-Type:application/json" -d @./custome-connector.json http://localhost:8083/connectors
* Check the status of the connector:
    * curl --url 'http://localhost:8083/connectors?expand=status' -k
* Run Kafka Consumers to read data being written by the connector ( In our code, the topics are source-1, source-2, source-3 )
    * kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic source-1 --from-beginning
    * kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic source-2 --from-beginning
    * kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic source-3 --from-beginning


## Contributing
Pull requests are welcome.
