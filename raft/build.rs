fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure().compile_protos(
        &["proto/client_service.proto", "proto/internal_service.proto"],
        &["proto/"],
    )?;
    Ok(())
}
