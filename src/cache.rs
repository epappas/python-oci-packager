use anyhow::Result;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tokio::fs;

use crate::image::ImageConfig;
use crate::layer::Layer;

#[derive(Debug, Serialize, Deserialize)]
pub struct Cache {
    cache_dir: PathBuf,
    layer_index: HashMap<String, LayerCacheEntry>,
    dependency_index: HashMap<String, String>,
    config_index: HashMap<String, ConfigCacheEntry>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ConfigCacheEntry {
    path: PathBuf,
    timestamp: std::time::SystemTime,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LayerCacheEntry {
    digest: String,
    path: PathBuf,
    timestamp: std::time::SystemTime,
    metadata: LayerMetadata,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LayerMetadata {
    pub layer_type: LayerType,
    pub source_hash: String,
    pub dependencies: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum LayerType {
    VirtualEnv,
    Dependencies,
    Application,
}

impl Cache {
    pub async fn new(cache_dir: PathBuf) -> Result<Self> {
        fs::create_dir_all(&cache_dir).await?;

        let index_path = cache_dir.join("index.json");
        let cache = if index_path.exists() {
            let data = fs::read(&index_path).await?;
            serde_json::from_slice(&data)?
        } else {
            Self {
                cache_dir,
                layer_index: HashMap::new(),
                dependency_index: HashMap::new(),
                config_index: HashMap::new(), // Initialize the new field
            }
        };

        Ok(cache)
    }

    pub async fn store_config(&mut self, key: &str, config: &ImageConfig) -> Result<()> {
        let config_path = self.cache_dir.join(format!("config_{}.json", key));

        // Serialize and store config data
        let config_data = serde_json::to_string_pretty(config)?;
        fs::write(&config_path, config_data).await?;

        // Update index
        self.config_index.insert(
            key.to_string(),
            ConfigCacheEntry {
                path: config_path,
                timestamp: std::time::SystemTime::now(),
            },
        );

        // Save updated index
        self.save_index().await?;

        Ok(())
    }

    pub async fn get_config(&self, key: &str) -> Option<ImageConfig> {
        let entry = self.config_index.get(key)?;

        // Check if cached config still exists
        if !entry.path.exists() {
            return None;
        }

        // Load and deserialize config data
        match fs::read(&entry.path).await {
            Ok(data) => serde_json::from_slice(&data).ok(),
            Err(_) => None,
        }
    }

    pub async fn get_layer(&self, key: &str) -> Option<Layer> {
        let entry = self.layer_index.get(key)?;

        // Check if cached layer still exists
        if !entry.path.exists() {
            return None;
        }

        // Load layer data
        match fs::read(&entry.path).await {
            Ok(data) => {
                // Verify layer integrity
                let mut hasher = Sha256::new();
                hasher.update(&data);
                let digest = format!("sha256:{:x}", hasher.finalize());
                let data_u8: &[u8] = data.as_slice();

                if digest == entry.digest {
                    // Deserialize and return layer
                    bincode::deserialize(data_u8).ok()
                } else {
                    None
                }
            }
            Err(_) => None,
        }
    }

    pub async fn store_layer(
        &mut self,
        key: &str,
        layer: &Layer,
        metadata: LayerMetadata,
    ) -> Result<()> {
        // Generate path for layer file
        let layer_path = self.cache_dir.join(format!("layer_{}.bin", layer.digest));

        // Serialize and store layer data
        let layer_data = bincode::serialize(layer)?;
        fs::write(&layer_path, &layer_data).await?;

        // Update index
        self.layer_index.insert(
            key.to_string(),
            LayerCacheEntry {
                digest: layer.digest.clone(),
                path: layer_path,
                timestamp: std::time::SystemTime::now(),
                metadata,
            },
        );

        // Save updated index
        self.save_index().await?;

        Ok(())
    }

    pub async fn get_dependency_layer(&self, requirements: &Path) -> Option<Layer> {
        // Calculate hash of requirements.txt
        let req_content = fs::read(requirements).await.ok()?;
        let mut hasher = Sha256::new();
        hasher.update(&req_content);
        let req_hash = format!("sha256:{:x}", hasher.finalize());

        // Look up layer digest
        let layer_digest = self.dependency_index.get(&req_hash)?;

        // Get layer from cache
        self.get_layer(layer_digest).await
    }

    async fn save_index(&self) -> Result<()> {
        let index_path = self.cache_dir.join("index.json");
        let index_data = serde_json::to_string_pretty(&self)?;
        fs::write(index_path, index_data).await?;
        Ok(())
    }

    pub async fn cleanup(&mut self, max_age: std::time::Duration) -> Result<()> {
        let now = std::time::SystemTime::now();

        // Remove old entries from indexes
        self.layer_index
            .retain(|_, entry| match now.duration_since(entry.timestamp) {
                Ok(age) => age <= max_age,
                Err(_) => false,
            });

        self.config_index
            .retain(|_, entry| match now.duration_since(entry.timestamp) {
                Ok(age) => age <= max_age,
                Err(_) => false,
            });

        // Remove orphaned files
        let mut entries = fs::read_dir(&self.cache_dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if let Some(ext) = path.extension().and_then(|ext| ext.to_str()) {
                match ext {
                    "bin" => {
                        let is_referenced =
                            self.layer_index.values().any(|entry| entry.path == path);
                        if !is_referenced {
                            fs::remove_file(path).await?;
                        }
                    }
                    "json" => {
                        let is_referenced =
                            self.config_index.values().any(|entry| entry.path == path);
                        if !is_referenced {
                            fs::remove_file(path).await?;
                        }
                    }
                    _ => {}
                }
            }
        }

        self.save_index().await?;
        Ok(())
    }
}
