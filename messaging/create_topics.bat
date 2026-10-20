@echo off
REM =============================================================================
REM KHỞI TẠO VÀ CẤU HÌNH KAFKA TOPICS TRÊN WINDOWS (KAFKA-FIRST LAMBDA ARCHITECTURE)
REM =============================================================================
set KAFKA_DIR=%KAFKA_HOME%
if "%KAFKA_DIR%"=="" set KAFKA_DIR=D:\BigData\kafka
set BOOTSTRAP=%KAFKA_BOOTSTRAP%
if "%BOOTSTRAP%"=="" set BOOTSTRAP=localhost:9092

echo === Khoi tao Kafka Topics cho He thong Lambda Architecture (Kafka-First) ===

call "%KAFKA_DIR%\bin\windows\kafka-topics.bat" --create --if-not-exists --bootstrap-server %BOOTSTRAP% --replication-factor 1 --partitions 3 --config cleanup.policy=delete --topic tourism-input
call "%KAFKA_DIR%\bin\windows\kafka-topics.bat" --create --if-not-exists --bootstrap-server %BOOTSTRAP% --replication-factor 1 --partitions 3 --config cleanup.policy=delete --topic tourism-output
call "%KAFKA_DIR%\bin\windows\kafka-topics.bat" --create --if-not-exists --bootstrap-server %BOOTSTRAP% --replication-factor 1 --partitions 3 --config cleanup.policy=compact --config retention.ms=-1 --topic tourism-reviews
call "%KAFKA_DIR%\bin\windows\kafka-topics.bat" --create --if-not-exists --bootstrap-server %BOOTSTRAP% --replication-factor 1 --partitions 1 --config cleanup.policy=compact --config max.message.bytes=10485760 --topic tourism-state-spots
call "%KAFKA_DIR%\bin\windows\kafka-topics.bat" --create --if-not-exists --bootstrap-server %BOOTSTRAP% --replication-factor 1 --partitions 1 --config cleanup.policy=compact --config max.message.bytes=10485760 --topic tourism-state-batch
call "%KAFKA_DIR%\bin\windows\kafka-topics.bat" --create --if-not-exists --bootstrap-server %BOOTSTRAP% --replication-factor 1 --partitions 1 --config cleanup.policy=compact --topic tourism-control
call "%KAFKA_DIR%\bin\windows\kafka-topics.bat" --create --if-not-exists --bootstrap-server %BOOTSTRAP% --replication-factor 1 --partitions 1 --config cleanup.policy=delete --topic tourism-dead-letter

echo === Danh sach cac Topics ===
call "%KAFKA_DIR%\bin\windows\kafka-topics.bat" --list --bootstrap-server %BOOTSTRAP%
