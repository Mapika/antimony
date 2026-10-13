// Uses protox (pure Rust) so no system `protoc` is required.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto = "../../proto/ant.proto";
    println!("cargo:rerun-if-changed={proto}");
    let fds = protox::compile([proto], ["../../proto"])?;
    tonic_prost_build::configure().compile_fds(fds)?;
    Ok(())
}
