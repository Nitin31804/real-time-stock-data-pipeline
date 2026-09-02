output "kafka_bootstrap_brokers" {
  value = aws_msk_cluster.kafka.bootstrap_brokers
}
output "rds_endpoint" {
  value = aws_db_instance.postgres.endpoint
}
