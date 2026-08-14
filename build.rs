use std::fs;
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

fn main() {
    let mut inputs = vec![
        PathBuf::from("src/engine.rs"),
        PathBuf::from("src/gtm.rs"),
        PathBuf::from("src/outreach.rs"),
        PathBuf::from("src/playbook.rs"),
        PathBuf::from("src/response_design.rs"),
    ];
    collect_policy_files(Path::new("playbooks"), &mut inputs);
    for path in [
        "docs/sales-manual.md",
        "docs/follow-up-doctrine.md",
        "docs/wapahki-sales-manual.md",
        "docs/gnk-sales-manual.md",
        "docs/acquisition-channels.md",
        "docs/plays/telecom-noc.md",
        "docs/plays/msp.md",
        "docs/plays/multi-site-facilities.md",
        "docs/plays/generator-dispatch.md",
        "docs/plays/cold-storage-labs.md",
        "docs/plays/insurance.md",
        "evals/outreach-quality-rubric.md",
    ] {
        inputs.push(PathBuf::from(path));
    }
    inputs.sort();

    let mut hash = Sha256::new();
    for path in inputs {
        println!("cargo:rerun-if-changed={}", path.display());
        hash.update(path.to_string_lossy().as_bytes());
        hash.update([0]);
        hash.update(fs::read(&path).unwrap_or_else(|error| {
            panic!("reading copy-policy input {}: {error}", path.display())
        }));
        hash.update([0xff]);
    }
    println!(
        "cargo:rustc-env=SPRUCE_COMPILED_COPY_POLICY_HASH={:x}",
        hash.finalize()
    );
}

fn collect_policy_files(directory: &Path, output: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(directory) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_policy_files(&path, output);
        } else if matches!(
            path.extension().and_then(|extension| extension.to_str()),
            Some("md" | "toml")
        ) {
            output.push(path);
        }
    }
}
