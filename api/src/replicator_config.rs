use std::fmt::Debug;

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SourceConfig {
    Postgres {
        /// Host on which Postgres is running
        host: String,

        /// Port on which Postgres is running
        port: u16,

        /// Postgres database name
        name: String,

        /// Postgres database user name
        username: String,

        /// Postgres slot name
        slot_name: String,

        /// Postgres publication name
        publication: String,
    },
}

impl Debug for SourceConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Postgres {
                host,
                port,
                name,
                username,
                slot_name,
                publication,
            } => f
                .debug_struct("Postgres")
                .field("host", host)
                .field("port", port)
                .field("name", name)
                .field("username", username)
                .field("slot_name", slot_name)
                .field("publication", publication)
                .finish(),
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SinkConfig {
    BigQuery {
        /// BigQuery project id
        project_id: String,

        /// BigQuery dataset id
        dataset_id: String,

        /// The max_staleness parameter for BigQuery: https://cloud.google.com/bigquery/docs/change-data-capture#create-max-staleness
        #[serde(skip_serializing_if = "Option::is_none")]
        max_staleness_mins: Option<u16>,
    },
}

impl Debug for SinkConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BigQuery {
                project_id,
                dataset_id,
                max_staleness_mins,
            } => f
                .debug_struct("BigQuery")
                .field("project_id", project_id)
                .field("dataset_id", dataset_id)
                .field("max_staleness_mins", max_staleness_mins)
                .finish(),
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct BatchConfig {
    /// maximum batch size in number of events
    pub max_size: usize,

    /// maximum duration, in seconds, to wait for a batch to fill
    pub max_fill_secs: u64,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct Config {
    pub source: SourceConfig,
    pub sink: SinkConfig,
    pub batch: BatchConfig,
}

#[cfg(test)]
mod tests {
    use crate::replicator_config::{BatchConfig, Config, SinkConfig, SourceConfig};

    #[test]
    pub fn deserialize_settings_test() {
        let settings = r#"{
            "source": {
                "postgres": {
                    "host": "localhost",
                    "port": 5432,
                    "name": "postgres",
                    "username": "postgres",
                    "slot_name": "replicator_slot",
                    "publication": "replicator_publication"
                }
            },
            "sink": {
                "big_query": {
                    "project_id": "project-id",
                    "dataset_id": "dataset-id"
                }
            },
            "batch": {
                "max_size": 1000,
                "max_fill_secs": 10
            }
        }"#;
        let actual = serde_json::from_str::<Config>(settings);
        let expected = Config {
            source: SourceConfig::Postgres {
                host: "localhost".to_string(),
                port: 5432,
                name: "postgres".to_string(),
                username: "postgres".to_string(),
                slot_name: "replicator_slot".to_string(),
                publication: "replicator_publication".to_string(),
            },
            sink: SinkConfig::BigQuery {
                project_id: "project-id".to_string(),
                dataset_id: "dataset-id".to_string(),
                max_staleness_mins: None,
            },
            batch: BatchConfig {
                max_size: 1000,
                max_fill_secs: 10,
            },
        };
        assert!(actual.is_ok());
        assert_eq!(expected, actual.unwrap());
    }

    #[test]
    pub fn serialize_settings_test() {
        let actual = Config {
            source: SourceConfig::Postgres {
                host: "localhost".to_string(),
                port: 5432,
                name: "postgres".to_string(),
                username: "postgres".to_string(),
                slot_name: "replicator_slot".to_string(),
                publication: "replicator_publication".to_string(),
            },
            sink: SinkConfig::BigQuery {
                project_id: "project-id".to_string(),
                dataset_id: "dataset-id".to_string(),
                max_staleness_mins: None,
            },
            batch: BatchConfig {
                max_size: 1000,
                max_fill_secs: 10,
            },
        };
        let expected = r#"{"source":{"postgres":{"host":"localhost","port":5432,"name":"postgres","username":"postgres","slot_name":"replicator_slot","publication":"replicator_publication"}},"sink":{"big_query":{"project_id":"project-id","dataset_id":"dataset-id"}},"batch":{"max_size":1000,"max_fill_secs":10}}"#;
        let actual = serde_json::to_string(&actual);
        assert!(actual.is_ok());
        assert_eq!(expected, actual.unwrap());
    }
}
