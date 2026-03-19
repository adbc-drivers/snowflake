// src/statement.rs (stub)
use std::sync::Arc;
use adbc_core::{error::Result, options::{OptionStatement, OptionValue}, Optionable, PartitionedResult};
use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::Schema;
use sf_core::apis::database_driver_v1::Handle;
use crate::driver::Inner;

pub struct Statement {
    pub(crate) inner: Arc<Inner>,
    pub(crate) stmt_handle: Handle,
    pub(crate) query: Option<String>,
    pub(crate) target_table: Option<String>,
    pub(crate) ingest_mode: Option<String>,
    pub(crate) query_tag: Option<String>,
}

impl Optionable for Statement {
    type Option = OptionStatement;
    fn set_option(&mut self, _key: Self::Option, _value: OptionValue) -> Result<()> { todo!() }
    fn get_option_string(&self, _key: Self::Option) -> Result<String> { todo!() }
    fn get_option_bytes(&self, _key: Self::Option) -> Result<Vec<u8>> { todo!() }
    fn get_option_int(&self, _key: Self::Option) -> Result<i64> { todo!() }
    fn get_option_double(&self, _key: Self::Option) -> Result<f64> { todo!() }
}

impl adbc_core::Statement for Statement {
    fn bind(&mut self, _batch: RecordBatch) -> Result<()> { Err(crate::error::not_implemented("bind")) }
    fn bind_stream(&mut self, _reader: Box<dyn RecordBatchReader + Send>) -> Result<()> { Err(crate::error::not_implemented("bind_stream")) }
    fn execute(&mut self) -> Result<Box<dyn RecordBatchReader + Send + 'static>> { todo!() }
    fn execute_update(&mut self) -> Result<Option<i64>> { todo!() }
    fn execute_schema(&mut self) -> Result<Schema> { Err(crate::error::not_implemented("execute_schema")) }
    fn execute_partitions(&mut self) -> Result<PartitionedResult> { Err(crate::error::not_implemented("execute_partitions")) }
    fn get_parameter_schema(&self) -> Result<Schema> { Err(crate::error::not_implemented("get_parameter_schema")) }
    fn prepare(&mut self) -> Result<()> {
        if self.query.is_none() {
            return Err(adbc_core::error::Error::with_message_and_status(
                "cannot prepare statement with no query",
                adbc_core::error::Status::InvalidState,
            ));
        }
        Ok(())
    }
    fn set_sql_query(&mut self, query: impl AsRef<str>) -> Result<()> { todo!() }
    fn set_substrait_plan(&mut self, _plan: impl AsRef<[u8]>) -> Result<()> { Err(crate::error::not_implemented("set_substrait_plan")) }
    fn cancel(&mut self) -> Result<()> { Err(crate::error::not_implemented("cancel")) }
}
