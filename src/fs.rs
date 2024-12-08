use anyhow::Result;
use glob::glob;
use std::future::Future;
use std::path::Path;
use std::pin::Pin;

pub fn copy_dir_all(
    src: impl AsRef<Path> + 'static,
    dst: impl AsRef<Path> + 'static,
) -> Pin<Box<dyn Future<Output = Result<()>>>> {
    Box::pin(async move {
        let src = src.as_ref();
        let dst = dst.as_ref();

        if !dst.exists() {
            tokio::fs::create_dir_all(dst).await?;
        }

        let mut entries = tokio::fs::read_dir(src).await?;

        while let Some(entry) = entries.next_entry().await? {
            let ty = entry.file_type().await?;
            let src_path = entry.path();
            let dst_path = dst.join(entry.file_name());

            if ty.is_dir() {
                copy_dir_all(src_path.clone(), dst_path.clone()).await?;
            } else {
                tokio::fs::copy(&src_path, &dst_path).await?;
            }
        }

        Ok(())
    })
}

pub async fn remove_matching_files(dir: impl AsRef<Path>, pattern: &str) -> Result<()> {
    let dir = dir.as_ref();
    let pattern = format!("{}/**/{}", dir.display(), pattern);

    for entry in glob(&pattern)? {
        match entry {
            Ok(path) => {
                if path.is_dir() {
                    tokio::fs::remove_dir_all(&path).await?;
                } else {
                    tokio::fs::remove_file(&path).await?;
                }
            }
            Err(e) => tracing::warn!("Failed to read glob entry: {}", e),
        }
    }

    Ok(())
}
