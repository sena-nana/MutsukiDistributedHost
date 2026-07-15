use std::path::Path;
use std::process::Command;

fn main() {
    println!("cargo:rerun-if-env-changed=MUTSUKI_DISTRIBUTED_HOST_REVISION");
    let revision = std::env::var("MUTSUKI_DISTRIBUTED_HOST_REVISION")
        .ok()
        .filter(|value| is_revision(value))
        .or_else(git_revision)
        .unwrap_or_else(|| "unknown".into());
    println!("cargo:rustc-env=MUTSUKI_DISTRIBUTED_HOST_REVISION={revision}");
}

fn git_revision() -> Option<String> {
    let manifest = std::env::var_os("CARGO_MANIFEST_DIR")?;
    let root = Path::new(&manifest).ancestors().nth(2)?;
    let output = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(root)
        .output()
        .ok()?;
    let revision = String::from_utf8(output.stdout).ok()?;
    let revision = revision.trim();
    (output.status.success() && is_revision(revision)).then(|| revision.into())
}

fn is_revision(value: &str) -> bool {
    value.len() == 40 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}
