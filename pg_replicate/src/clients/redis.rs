use std::collections::HashSet;

use redis::{Client, Commands, Connection, RedisError, RedisResult};
use thiserror::Error;
use tokio_postgres::types::PgLsn;
use tracing::info;

use crate::{
    conversions::table_row::TableRow,
    table::{ColumnSchema, TableId, TableSchema},
};

pub struct RedisClient {
    client: Client,
    connection: Connection,
    stream_prefix: String,
}

#[derive(Debug, Error)]
pub enum RedisClientError {
    #[error("redis error: {0}")]
    Redis(#[from] RedisError),

    #[error("failed to get connection")]
    ConnectionError,
}

impl RedisClient {
    pub fn new(
        redis_url: &str,
        stream_prefix: &str,
    ) -> Result<RedisClient, RedisClientError> {
        info!("connecting to redis at {}", redis_url);
        let client = Client::open(redis_url)?;
        let connection = client.get_connection().map_err(|_| RedisClientError::ConnectionError)?;
        
        Ok(RedisClient { 
            client, 
            connection,
            stream_prefix: stream_prefix.to_string(), 
        })
    }

    pub fn get_stream_name(&self, table_name: &str) -> String {
        format!("{}:{}", self.stream_prefix, table_name)
    }

    pub fn add_to_stream(&mut self, stream_name: &str, fields: &[(&str, &str)]) -> Result<String, RedisClientError> {
        let result: String = self.connection.xadd(stream_name, "*", fields)?;
        Ok(result)
    }

    pub fn create_table_if_missing(
        &mut self,
        table_name: &str,
        column_schemas: &[ColumnSchema],
    ) -> Result<bool, RedisClientError> {
        // Redis streams don't need pre-creation, but we'll track metadata
        // Using a hash to store schema information
        let schema_key = format!("{}:schema:{}", self.stream_prefix, table_name);
        
        // Check if the schema already exists
        let exists: bool = self.connection.exists(&schema_key)?;
        if exists {
            return Ok(false);
        }
        
        // Store column schema information as a hash
        for column in column_schemas {
            let field_name = &column.name;
            let field_type = format!("{:?}", column.typ);
            let _: () = self.connection.hset(&schema_key, field_name, field_type)?;
        }
        
        Ok(true)
    }

    pub fn get_copied_table_ids(&mut self) -> Result<HashSet<TableId>, RedisClientError> {
        let copied_tables_key = format!("{}:copied_tables", self.stream_prefix);
        let members: Vec<i32> = self.connection.smembers(&copied_tables_key)?;
        
        Ok(members.into_iter().map(|id| id as TableId).collect())
    }

    pub fn add_copied_table(&mut self, table_id: TableId) -> Result<(), RedisClientError> {
        let copied_tables_key = format!("{}:copied_tables", self.stream_prefix);
        let _: () = self.connection.sadd(&copied_tables_key, table_id)?;
        
        Ok(())
    }

    pub fn get_last_lsn(&mut self) -> Result<PgLsn, RedisClientError> {
        let last_lsn_key = format!("{}:last_lsn", self.stream_prefix);
        let lsn: Option<u64> = self.connection.get(&last_lsn_key)?;
        
        match lsn {
            Some(lsn) => Ok(lsn.into()),
            None => {
                // Initialize with 0 if it doesn't exist
                let _: () = self.connection.set(&last_lsn_key, 0)?;
                Ok(PgLsn::from(0))
            }
        }
    }

    pub fn set_last_lsn(&mut self, lsn: PgLsn) -> Result<(), RedisClientError> {
        let lsn_u64: u64 = lsn.into();
        let last_lsn_key = format!("{}:last_lsn", self.stream_prefix);
        let _: () = self.connection.set(&last_lsn_key, lsn_u64)?;
        
        Ok(())
    }
} 