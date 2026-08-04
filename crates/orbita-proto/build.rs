use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../proto");
    let protos = [
        "orbita/v1/kv.proto",
        "orbita/v1/admin.proto",
        "orbita/v1/health.proto",
    ];

    // Rebuild when a definition changes, not just when the Rust source does.
    println!("cargo:rerun-if-changed={}", proto_root.display());

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(
            &protos
                .iter()
                .map(|p| proto_root.join(p))
                .collect::<Vec<_>>(),
            &[proto_root],
        )?;

    Ok(())
}
