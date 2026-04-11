# Backward-compatible note: this file kept the original typo in its name.
# On Ubuntu/cloud, prefer:
#   bash server_setup.sh
#
# On Windows, start these from your Kafka installation directory in separate terminals:
#   .\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties
#   .\bin\windows\kafka-server-start.bat .\config\server.properties
#
# Then create the topics used by this project:
#   .\bin\windows\kafka-topics.bat --create --if-not-exists --topic infra.raw.metrics --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
#   .\bin\windows\kafka-topics.bat --create --if-not-exists --topic infra.feature.windows --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
#   .\bin\windows\kafka-topics.bat --create --if-not-exists --topic infra.predictions --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
#   .\bin\windows\kafka-topics.bat --create --if-not-exists --topic infra.dlq --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
