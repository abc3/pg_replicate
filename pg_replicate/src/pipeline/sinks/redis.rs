use std::collections::HashMap;

use async_trait::async_trait;
use thiserror::Error;
use tokio_postgres::types::PgLsn;
use tracing::info;

use crate::{
    clients::redis::{RedisClient, RedisClientError},
    conversions::{cdc_event::CdcEvent, table_row::TableRow, Cell},
    pipeline::PipelineResumptionState,
    table::{TableId, TableName, TableSchema},
};

use super::{BatchSink, SinkError};

#[derive(Debug, Error)]
pub enum RedisSinkError {
    #[error("redis error: {0}")]
    Redis(#[from] RedisClientError),

    #[error("missing table schemas")]
    MissingTableSchemas,

    #[error("missing table id: {0}")]
    MissingTableId(TableId),

    #[error("incorrect commit lsn: {0}(expected: {0})")]
    IncorrectCommitLsn(PgLsn, PgLsn),

    #[error("commit message without begin message")]
    CommitWithoutBegin,
}

impl SinkError for RedisSinkError {}

pub struct RedisBatchSink {
    client: RedisClient,
    table_schemas: Option<HashMap<TableId, TableSchema>>,
    committed_lsn: Option<PgLsn>,
    final_lsn: Option<PgLsn>,
}

impl RedisBatchSink {
    pub fn new(redis_url: &str, stream_prefix: &str) -> Result<RedisBatchSink, RedisClientError> {
        let client = RedisClient::new(redis_url, stream_prefix)?;
        
        Ok(RedisBatchSink {
            client,
            table_schemas: None,
            committed_lsn: None,
            final_lsn: None,
        })
    }

    fn get_table_schema(&self, table_id: TableId) -> Result<&TableSchema, RedisSinkError> {
        self.table_schemas
            .as_ref()
            .ok_or(RedisSinkError::MissingTableSchemas)?
            .get(&table_id)
            .ok_or(RedisSinkError::MissingTableId(table_id))
    }

    fn table_name_in_redis(table_name: &TableName) -> String {
        format!("{}_{}", table_name.schema, table_name.name)
    }
    
    fn table_row_to_fields(&self, table_row: &TableRow) -> Vec<(String, String)> {
        let mut fields = Vec::new();
        
        for (i, cell) in table_row.values.iter().enumerate() {
            let field_name = format!("field_{}", i);
            let field_value = self.cell_to_string(cell);
            fields.push((field_name, field_value));
        }
        
        fields
    }
    
    fn cell_to_string(&self, cell: &Cell) -> String {
        match cell {
            Cell::Null => "null".to_string(),
            Cell::Bool(b) => b.to_string(),
            Cell::String(s) => s.clone(),
            Cell::I16(i) => i.to_string(),
            Cell::I32(i) => i.to_string(),
            Cell::I64(i) => i.to_string(),
            Cell::F32(f) => f.to_string(),
            Cell::F64(f) => f.to_string(),
            Cell::Numeric(n) => n.to_string(),
            Cell::Date(d) => d.to_string(),
            Cell::Time(t) => t.to_string(),
            Cell::TimeStamp(ts) => ts.to_string(),
            Cell::TimeStampTz(ts) => ts.to_string(),
            Cell::Uuid(u) => u.to_string(),
            Cell::Json(j) => j.to_string(),
            Cell::U32(u) => u.to_string(),
            Cell::Bytes(b) => format!("{:?}", b),
            Cell::Array(_) => "array_value".to_string(), // Simplified handling
        }
    }
}

#[async_trait]
impl BatchSink for RedisBatchSink {
    type Error = RedisSinkError;

    async fn get_resumption_state(&mut self) -> Result<PipelineResumptionState, Self::Error> {
        info!("getting resumption state from redis");
        
        let copied_tables = self.client.get_copied_table_ids()?;
        let last_lsn = self.client.get_last_lsn()?;
        
        self.committed_lsn = Some(last_lsn);
        
        Ok(PipelineResumptionState {
            copied_tables,
            last_lsn,
        })
    }

    async fn write_table_schemas(
        &mut self,
        table_schemas: HashMap<TableId, TableSchema>,
    ) -> Result<(), Self::Error> {
        for table_schema in table_schemas.values() {
            let table_name = Self::table_name_in_redis(&table_schema.table_name);
            self.client
                .create_table_if_missing(&table_name, &table_schema.column_schemas)?;
        }

        self.table_schemas = Some(table_schemas);

        Ok(())
    }

    async fn write_table_rows(
        &mut self,
        table_rows: Vec<TableRow>,
        table_id: TableId,
    ) -> Result<(), Self::Error> {
        let table_schema = self.get_table_schema(table_id)?;
        let table_name = Self::table_name_in_redis(&table_schema.table_name);
        let stream_name = self.client.get_stream_name(&table_name);

        for table_row in &table_rows {
            let fields = self.table_row_to_fields(table_row);
            // Convert fields to &[(&str, &str)]
            let fields_refs: Vec<(&str, &str)> = fields
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_str()))
                .collect();
                
            // Add operation type as a field
            let op_refs = [("operation", "UPSERT")];
            let all_fields: Vec<(&str, &str)> = fields_refs.iter()
                .map(|(k, v)| (*k, *v))
                .chain(op_refs.iter().copied())
                .collect();
                
            self.client.add_to_stream(&stream_name, &all_fields)?;
        }

        Ok(())
    }

    async fn write_cdc_events(&mut self, events: Vec<CdcEvent>) -> Result<PgLsn, Self::Error> {
        let mut table_id_to_events = HashMap::new();
        let mut new_last_lsn = PgLsn::from(0);

        for event in events {
            match event {
                CdcEvent::Begin(begin_body) => {
                    let final_lsn_u64 = begin_body.final_lsn();
                    self.final_lsn = Some(final_lsn_u64.into());
                }
                CdcEvent::Commit(commit_body) => {
                    let commit_lsn: PgLsn = commit_body.commit_lsn().into();
                    if let Some(final_lsn) = self.final_lsn {
                        if commit_lsn == final_lsn {
                            new_last_lsn = commit_lsn;
                        } else {
                            return Err(RedisSinkError::IncorrectCommitLsn(commit_lsn, final_lsn));
                        }
                    } else {
                        return Err(RedisSinkError::CommitWithoutBegin);
                    }
                }
                CdcEvent::Insert((table_id, table_row)) => {
                    let table_events = table_id_to_events.entry(table_id).or_insert_with(Vec::new);
                    table_events.push((table_row, "UPSERT"));
                }
                CdcEvent::Update((table_id, table_row)) => {
                    let table_events = table_id_to_events.entry(table_id).or_insert_with(Vec::new);
                    table_events.push((table_row, "UPSERT"));
                }
                CdcEvent::Delete((table_id, table_row)) => {
                    let table_events = table_id_to_events.entry(table_id).or_insert_with(Vec::new);
                    table_events.push((table_row, "DELETE"));
                }
                // Other events that we don't handle for Redis
                CdcEvent::Origin(_) => {}
                CdcEvent::Truncate(_) => {}
                CdcEvent::Relation(_) => {}
                CdcEvent::KeepAliveRequested { reply: _ } => {}
                CdcEvent::Type(_) => {}
            }
        }

        for (table_id, table_events) in table_id_to_events {
            let table_schema = self.get_table_schema(table_id)?;
            let table_name = Self::table_name_in_redis(&table_schema.table_name);
            let stream_name = self.client.get_stream_name(&table_name);

            for (table_row, operation) in table_events {
                let fields = self.table_row_to_fields(&table_row);
                // Convert fields to &[(&str, &str)]
                let fields_refs: Vec<(&str, &str)> = fields
                    .iter()
                    .map(|(k, v)| (k.as_str(), v.as_str()))
                    .collect();
                    
                // Add operation type as a field
                let op_refs = [("operation", operation)];
                let all_fields: Vec<(&str, &str)> = fields_refs.iter()
                    .map(|(k, v)| (*k, *v))
                    .chain(op_refs.iter().copied())
                    .collect();
                    
                self.client.add_to_stream(&stream_name, &all_fields)?;
            }
        }

        if new_last_lsn > PgLsn::from(0) {
            self.client.set_last_lsn(new_last_lsn)?;
            self.committed_lsn = Some(new_last_lsn);
        }

        Ok(new_last_lsn)
    }

    async fn table_copied(&mut self, table_id: TableId) -> Result<(), Self::Error> {
        self.client.add_copied_table(table_id)?;
        Ok(())
    }

    async fn truncate_table(&mut self, _table_id: TableId) -> Result<(), Self::Error> {
        // Redis doesn't have a direct TRUNCATE equivalent
        // We would need to implement this differently, possibly by deleting the stream
        // and creating a new one, but for simplicity we'll just ignore it
        Ok(())
    }
} 