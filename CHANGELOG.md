# 0.11.0

 * SslApp: use DES encryption instead of RC2 for PKCS#12 files, as RC2
   is obsoleted (and disabled by default) in OpenSSL 3.
 * SslApp: use genpkey instead of deprecated genrsa for generating keys.

# 0.10.0

 * Added Oauthbearer/OIDC ticket server app (by @jliunyu, #13)
 * Fix race condition in Cluster.start() where it would check if the cluster
   was operational after each app.start() rather than after starting all apps.
   This only happened if a timeout was provided to Cluster.start()
 * Clean up app config from None values. This fixes a case where "None" was
   passed to the KafkaBrokerApp deploy script if no kafka_path was specified.
 * Clear JMX_PORT env before calling Kafka scripts to avoid
   'port already in use' when setting up SCRAM credentials.
 * Added tests.

# 0.9.0

 * Initial support for Kafka KRaft (run Kafka without Zookeeper).
   Try it with `python3 -m trivup.clusters.KafkaCluster --kraft 2.8.0`
 * Support for intermediate and self-signed certificates (by @KJTsanaktsidis).

# 0.8.4

 * KafkaCluster: Bump Confluent Platform to 6.1.0
 * KafkaCluster: add --cpversion argument

# 0.8.3

 * Bump Apache Kafka to 2.7.0
 * Bump Confluent Platform to 6.0.0
 * Add `port` alias for `port_base` in KafkaBrokerApp config

# 0.8.2

 * SchemaRegistryApp: Honour 'version' conf (defaults to 'latest' docker image,
   was 5.2.1).
 * Update Kerberos encoding types for newer Debian versions.
 * Newer OpenSSL requires at least 2048 bits in the RSA key.
