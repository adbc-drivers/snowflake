// src/database.rs (stub)
use std::collections::HashMap;
use std::sync::Arc;
use adbc_core::{error::Result, options::{OptionConnection, OptionDatabase, OptionValue}, Optionable};
use sf_core::apis::database_driver_v1::Handle;
use sf_core::config::settings::Setting;
use crate::connection::Connection;
use crate::driver::Inner;

pub struct Database {
    pub(crate) inner: Arc<Inner>,
    pub(crate) db_handle: Handle,
    pub(crate) sf_settings: HashMap<String, Setting>,
}

impl Optionable for Database {
    type Option = OptionDatabase;
    fn set_option(&mut self, _key: Self::Option, _value: OptionValue) -> Result<()> { todo!() }
    fn get_option_string(&self, _key: Self::Option) -> Result<String> { todo!() }
    fn get_option_bytes(&self, _key: Self::Option) -> Result<Vec<u8>> { todo!() }
    fn get_option_int(&self, _key: Self::Option) -> Result<i64> { todo!() }
    fn get_option_double(&self, _key: Self::Option) -> Result<f64> { todo!() }
}

impl adbc_core::Database for Database {
    type ConnectionType = Connection;
    fn new_connection(&self) -> Result<Self::ConnectionType> { todo!() }
    fn new_connection_with_opts(&self, _opts: impl IntoIterator<Item = (OptionConnection, OptionValue)>) -> Result<Self::ConnectionType> { todo!() }
}
