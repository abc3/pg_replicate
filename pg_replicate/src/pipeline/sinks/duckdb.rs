use std::{collections::HashMap, path::Path};

use thiserror::Error;
use tokio::sync::mpsc::{channel, Receiver, Sender};

use async_trait::async_trait;
use tokio_postgres::types::Type;
use tracing::error;

use crate::{
    clients::duckdb::DuckDbClient,
    conversions::{cdc_event::CdcEvent, table_row::TableRow},
    pipeline::PipelineResumptionState,
    table::{ColumnSchema, TableId, TableName, TableSchema},
};

use super::{Sink, SinkError};

pub enum DuckDbRequest {
    GetResumptionState,
    CreateTables(HashMap<TableId, TableSchema>),
    InsertRow(TableRow, TableId),
    HandleCdcEvent(CdcEvent),
    TableCopied(TableId),
}

pub enum DuckDbResponse {
    ResumptionState(Result<PipelineResumptionState, DuckDbExecutorError>),
    CreateTablesResponse(Result<(), DuckDbExecutorError>),
    InsertRowResponse(Result<(), DuckDbExecutorError>),
    HandleCdcEventResponse(Result<(), DuckDbExecutorError>),
    TableCopiedResponse(Result<(), DuckDbExecutorError>),
}

#[derive(Debug, Error)]
pub enum DuckDbExecutorError {
    #[error("duckdb error: {0}")]
    DuckDb(#[from] duckdb::Error),

    #[error("missing table schemas")]
    MissingTableSchemas,

    #[error("missing table id: {0}")]
    MissingTableId(TableId),
}

struct DuckDbExecutor {
    client: DuckDbClient,
    req_receiver: Receiver<DuckDbRequest>,
    res_sender: Sender<DuckDbResponse>,
    table_schemas: Option<HashMap<TableId, TableSchema>>,
}

impl DuckDbExecutor {
    pub fn start(mut self) {
        tokio::spawn(async move {
            while let Some(req) = self.req_receiver.recv().await {
                match req {
                    DuckDbRequest::GetResumptionState => {
                        let result = self.get_resumption_state();
                        let response = DuckDbResponse::ResumptionState(result);
                        self.send_response(response).await;
                    }
                    DuckDbRequest::CreateTables(table_schemas) => {
                        let result = self.create_tables(&table_schemas);
                        self.table_schemas = Some(table_schemas);
                        let response = DuckDbResponse::CreateTablesResponse(result);
                        self.send_response(response).await;
                    }
                    DuckDbRequest::InsertRow(row, table_id) => {
                        let result = self.insert_row(table_id, row);
                        let response = DuckDbResponse::InsertRowResponse(result);
                        self.send_response(response).await;
                    }
                    DuckDbRequest::HandleCdcEvent(event) => {
                        let result = match event {
                            CdcEvent::Begin(_) => Ok(()),
                            CdcEvent::Commit(_) => Ok(()),
                            CdcEvent::Insert((table_id, table_row)) => {
                                self.insert_row(table_id, table_row)
                            }
                            CdcEvent::Update((table_id, table_row)) => {
                                self.update_row(table_id, table_row)
                            }
                            CdcEvent::Delete((table_id, table_row)) => {
                                self.delete_row(table_id, table_row)
                            }
                            CdcEvent::Relation(_) => Ok(()),
                            CdcEvent::KeepAliveRequested { reply: _ } => Ok(()),
                        };

                        let response = DuckDbResponse::HandleCdcEventResponse(result);
                        self.send_response(response).await;
                    }
                    DuckDbRequest::TableCopied(table_id) => {
                        let result = self.table_copied(table_id);
                        let response = DuckDbResponse::TableCopiedResponse(result);
                        self.send_response(response).await;
                    }
                }
            }
        });
    }

    async fn send_response(&mut self, response: DuckDbResponse) {
        match self.res_sender.send(response).await {
            Ok(_) => {}
            Err(e) => error!("failed to send response: {e}"),
        }
    }

    fn get_resumption_state(&self) -> Result<PipelineResumptionState, DuckDbExecutorError> {
        let table_name = TableName {
            schema: "pg_replicate".to_string(),
            name: "copied_tables".to_string(),
        };
        let column_schemas = vec![ColumnSchema {
            name: "table_id".to_string(),
            typ: Type::INT4,
            modifier: 0,
            nullable: false,
            identity: true,
        }];
        self.client.create_schema_if_missing(&table_name.schema)?;

        self.client
            .create_table_if_missing(&table_name, &column_schemas)?;

        let copied_tables = self.client.get_copied_table_ids()?;

        Ok(PipelineResumptionState { copied_tables })
    }

    fn create_tables(
        &self,
        table_schemas: &HashMap<u32, TableSchema>,
    ) -> Result<(), DuckDbExecutorError> {
        for table_schema in table_schemas.values() {
            let schema = &table_schema.table_name.schema;

            self.client.create_schema_if_missing(schema)?;
            self.client
                .create_table_if_missing(&table_schema.table_name, &table_schema.column_schemas)?;
        }

        Ok(())
    }

    fn insert_row(
        &self,
        table_id: TableId,
        table_row: TableRow,
    ) -> Result<(), DuckDbExecutorError> {
        let table_schema = self.get_table_schema(table_id)?;
        self.client
            .insert_row(&table_schema.table_name, &table_row)?;
        Ok(())
    }

    fn update_row(
        &self,
        table_id: TableId,
        table_row: TableRow,
    ) -> Result<(), DuckDbExecutorError> {
        let table_schema = self.get_table_schema(table_id)?;
        self.client.update_row(table_schema, &table_row)?;
        Ok(())
    }

    fn delete_row(
        &self,
        table_id: TableId,
        table_row: TableRow,
    ) -> Result<(), DuckDbExecutorError> {
        let table_schema = self.get_table_schema(table_id)?;
        self.client.delete_row(table_schema, &table_row)?;
        Ok(())
    }

    fn get_table_schema(&self, table_id: TableId) -> Result<&TableSchema, DuckDbExecutorError> {
        self.table_schemas
            .as_ref()
            .ok_or(DuckDbExecutorError::MissingTableSchemas)?
            .get(&table_id)
            .ok_or(DuckDbExecutorError::MissingTableId(table_id))
    }

    fn table_copied(&self, table_id: TableId) -> Result<(), DuckDbExecutorError> {
        self.client.insert_into_copied_tables(table_id)?;
        Ok(())
    }
}

pub struct DuckDbSink {
    req_sender: Sender<DuckDbRequest>,
    res_receiver: Receiver<DuckDbResponse>,
}

const CHANNEL_SIZE: usize = 32;

impl DuckDbSink {
    pub async fn file<P: AsRef<Path>>(file_name: P) -> Result<DuckDbSink, duckdb::Error> {
        let (req_sender, req_receiver) = channel(CHANNEL_SIZE);
        let (res_sender, res_receiver) = channel(CHANNEL_SIZE);
        let client = DuckDbClient::open_file(file_name)?;
        let executor = DuckDbExecutor {
            client,
            req_receiver,
            res_sender,
            table_schemas: None,
        };
        executor.start();
        Ok(DuckDbSink {
            req_sender,
            res_receiver,
        })
    }

    pub async fn in_memory() -> Result<DuckDbSink, duckdb::Error> {
        let (req_sender, req_receiver) = channel(CHANNEL_SIZE);
        let (res_sender, res_receiver) = channel(CHANNEL_SIZE);
        let client = DuckDbClient::open_in_memory()?;
        let executor = DuckDbExecutor {
            client,
            req_receiver,
            res_sender,
            table_schemas: None,
        };
        executor.start();
        Ok(DuckDbSink {
            req_sender,
            res_receiver,
        })
    }

    pub async fn execute(&mut self, req: DuckDbRequest) -> Result<DuckDbResponse, SinkError> {
        self.req_sender.send(req).await?;
        if let Some(res) = self.res_receiver.recv().await {
            Ok(res)
        } else {
            Err(SinkError::NoResponseReceived)
        }
    }
}

#[async_trait]
impl Sink for DuckDbSink {
    async fn get_resumption_state(&mut self) -> Result<PipelineResumptionState, SinkError> {
        let req = DuckDbRequest::GetResumptionState;
        match self.execute(req).await? {
            DuckDbResponse::ResumptionState(res) => {
                let resumption_state = res?;
                Ok(resumption_state)
            }
            _ => panic!("invalid response to GetResumptionState request"),
        }
    }

    async fn write_table_schemas(
        &mut self,
        table_schemas: HashMap<TableId, TableSchema>,
    ) -> Result<(), SinkError> {
        let req = DuckDbRequest::CreateTables(table_schemas);
        match self.execute(req).await? {
            DuckDbResponse::CreateTablesResponse(res) => {
                let _ = res?;
            }
            _ => panic!("invalid response to CreateTables request"),
        }

        Ok(())
    }

    async fn write_table_row(&mut self, row: TableRow, table_id: TableId) -> Result<(), SinkError> {
        let req = DuckDbRequest::InsertRow(row, table_id);
        match self.execute(req).await? {
            DuckDbResponse::InsertRowResponse(res) => {
                let _ = res?;
            }
            _ => panic!("invalid response to InsertRow request"),
        }
        Ok(())
    }

    async fn write_cdc_event(&mut self, event: CdcEvent) -> Result<(), SinkError> {
        let req = DuckDbRequest::HandleCdcEvent(event);
        match self.execute(req).await? {
            DuckDbResponse::HandleCdcEventResponse(res) => {
                let _ = res?;
            }
            _ => panic!("invalid response to HandleCdcEvent request"),
        }
        Ok(())
    }

    async fn table_copied(&mut self, table_id: TableId) -> Result<(), SinkError> {
        let req = DuckDbRequest::TableCopied(table_id);
        match self.execute(req).await? {
            DuckDbResponse::TableCopiedResponse(res) => {
                let _ = res?;
            }
            _ => panic!("invalid response to TableCopied request"),
        }
        Ok(())
    }
}
