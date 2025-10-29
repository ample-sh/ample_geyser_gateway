
#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    #[error("missing key path in transport options")]
    MissingKeyPath,

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("pem parse error: {0}")]
    PemParse(#[from] pem::PemError),

    #[error("rustls error: {0}")]
    Rustls(#[from] rustls::Error),

    #[error("quinn error during connection: {0}")]
    QuinnConnection(#[from] quinn::ConnectionError),

    #[error("quinn failed to connect: {0}")]
    QuinnConnectError(#[from] quinn::ConnectError),

    #[error("bincode error: {0}")]
    Bincode(#[from] bincode::Error),

    #[error("invalid stream opcode: {0}")]
    InvalidStreamOp(u8),
}