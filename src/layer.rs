use anyhow::{anyhow, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::io::Write;
use std::path::Path;

#[async_trait]
pub trait LayerBuilder {
    async fn build(&self) -> Result<Layer>;
    async fn compress(&self, data: Vec<u8>) -> Result<Vec<u8>>;
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Layer {
    pub media_type: String,
    pub digest: String,
    pub size: u64,
    pub compressed_size: u64,
    pub data: Vec<u8>,
    pub diff_id: String,
    pub annotations: HashMap<String, String>,
}

impl Layer {
    pub async fn from_dir(path: &Path) -> Result<Self> {
        let mut archive = tar::Builder::new(Vec::new());
        let walker = walkdir::WalkDir::new(path).min_depth(1).follow_links(true);

        for entry in walker {
            let entry = entry.map_err(|e| anyhow!(e.to_string()))?;
            if entry.file_type().is_file() {
                archive.append_path_with_name(
                    entry.path(),
                    entry.path().strip_prefix(path).unwrap(),
                )?;
            }
        }

        let data = archive.into_inner()?;
        let compressed = Self::compress_data(&data).await?;

        let mut hasher = Sha256::new();
        hasher.update(&compressed);
        let digest = format!("sha256:{:x}", hasher.finalize());

        let mut diff_hasher = Sha256::new();
        diff_hasher.update(&data);
        let diff_id = format!("sha256:{:x}", diff_hasher.finalize());

        Ok(Self {
            media_type: "application/vnd.oci.image.layer.v1.tar+gzip".to_string(),
            digest,
            size: data.len() as u64,
            compressed_size: compressed.len() as u64,
            data: compressed,
            diff_id,
            annotations: HashMap::new(),
        })
    }

    async fn compress_data(data: &[u8]) -> Result<Vec<u8>> {
        let mut encoder = zstd::Encoder::new(Vec::new(), 3)?;
        encoder.write_all(data)?;
        Ok(encoder.finish()?)
    }
}
