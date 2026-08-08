//! Small persistence primitives shared by local JSON stores.

use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::Path;

use anyhow::{Context, Result};
use uuid::Uuid;

/// Replace a file atomically by writing and syncing a sibling temporary file,
/// then renaming it over the destination. Keeping the temporary file in the
/// same directory makes the rename atomic on the filesystems Spruce Leaf uses.
pub fn atomic_write(path: &Path, contents: impl AsRef<[u8]>) -> Result<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent).with_context(|| format!("creating {}", parent.display()))?;
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("spruce-data");
    let temporary = parent.join(format!(".{name}.{}.tmp", Uuid::new_v4()));

    let result = (|| -> Result<()> {
        let mut options = OpenOptions::new();
        options.create_new(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options
            .open(&temporary)
            .with_context(|| format!("creating {}", temporary.display()))?;
        file.write_all(contents.as_ref())
            .with_context(|| format!("writing {}", temporary.display()))?;
        file.sync_all()
            .with_context(|| format!("syncing {}", temporary.display()))?;
        fs::rename(&temporary, path).with_context(|| {
            format!(
                "atomically replacing {} with a complete file",
                path.display()
            )
        })?;
        Ok(())
    })();

    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::atomic_write;
    use uuid::Uuid;

    #[test]
    fn atomic_write_replaces_complete_contents_without_temp_debris() {
        let root = std::env::temp_dir().join(format!("spruce-atomic-{}", Uuid::new_v4()));
        let path = root.join("state.json");
        atomic_write(&path, b"old").expect("initial write");
        atomic_write(&path, b"new complete value").expect("replacement write");
        assert_eq!(std::fs::read(&path).unwrap(), b"new complete value");
        assert_eq!(std::fs::read_dir(&root).unwrap().count(), 1);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
        let _ = std::fs::remove_dir_all(root);
    }
}
