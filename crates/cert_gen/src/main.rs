use clap::Parser;
use std::path::PathBuf;
use rcgen::CertifiedKey;

#[derive(Parser, Debug)]
#[command(name = "cert-gen")]
#[command(about = "Generate self-signed certificates for QUIC/TLS", long_about = None)]
struct Args {
    #[arg(short, long)]
    fqdn: String,
    #[arg(short, long, value_name = "PATH")]
    cert: PathBuf,
    #[arg(short, long, value_name = "PATH")]
    key: PathBuf,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    println!("{:#?}", args);

    let CertifiedKey { cert, signing_key } = rcgen::generate_simple_self_signed(vec![args.fqdn])?;

    if let Some(cert_dir) = args.cert.parent() {
        let _ = std::fs::create_dir_all(cert_dir);
    }
    std::fs::write(&args.cert, cert.pem())?;

    if let Some(key_dir) = args.key.parent() {
        let _ = std::fs::create_dir_all(key_dir);
    }
    std::fs::write(&args.key, signing_key.serialize_pem())?;

    println!("Certificate and key have been written to specified paths.");

    Ok(())
}
