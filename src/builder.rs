use anyhow::{format_err, Result};
use oci_spec::image::Config as OCIConfig;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::cache::Cache;
use crate::fs::{copy_dir_all, remove_matching_files};
use crate::image::ImageConfig;
use crate::layer::Layer;
use crate::manifest::Manifest;
use std::path::PathBuf;

#[derive(Debug)]
struct BuildOutput {
    layer: Layer,
    config: ImageConfig,
}

#[derive(Debug, Serialize, Deserialize)]
struct BaseImage {
    layer: Layer,
    config: ImageConfig,
}

pub struct PythonImageBuilder {
    project_path: PathBuf,
    output_path: PathBuf,
    base_image: String,
    config: ImageConfig,
    cache: Cache,
}

impl PythonImageBuilder {
    pub fn new(
        project_path: PathBuf,
        output_path: PathBuf,
        base_image: String,
        config: ImageConfig,
        cache: Cache,
    ) -> Result<Self> {
        // Validate project path
        if !project_path.exists() {
            return Err(format_err!("Project path does not exist"));
        }
        if !project_path.is_dir() {
            return Err(format_err!("Project path is not a directory"));
        }

        Ok(Self {
            project_path,
            output_path,
            base_image,
            config,
            cache,
        })
    }

    pub async fn build(&self) -> Result<()> {
        tracing::info!("Starting build process for Python project");

        // Rest of your async code...
        let base_image = self.pull_base_image().await?;
        let venv_layer = self.create_venv_layer().await?;
        let deps_layer = self.create_deps_layer().await?;
        let app_layer = self.create_app_layer().await?;

        // Generate final config
        let config = self.generate_config(&[
            &base_image.config,
            &venv_layer.config,
            &deps_layer.config,
            &app_layer.config,
        ])?;

        // Create manifest
        let manifest = self.create_manifest(
            config.clone(),
            vec![
                base_image.layer,
                venv_layer.layer,
                deps_layer.layer,
                app_layer.layer,
            ],
        )?;

        // Write image
        self.write_image(config.clone(), manifest).await?;

        tracing::info!("Build completed successfully");
        Ok(())
    }

    async fn create_venv_layer(&self) -> Result<BuildOutput> {
        tracing::debug!("Creating virtual environment layer");

        let temp_dir = tempfile::tempdir()?;
        let venv_path = temp_dir.path().join("venv");

        // Create virtual environment with system packages
        let output = tokio::process::Command::new("python")
            .args([
                "-m",
                "venv",
                "--system-site-packages",
                venv_path.to_str().unwrap(),
            ])
            .output()
            .await?;

        if !output.status.success() {
            let error = String::from_utf8_lossy(&output.stderr);
            return Err(format_err!(
                "Failed to create virtual environment: {}",
                error
            ));
        }

        // Activate venv and upgrade pip
        let pip_upgrade = tokio::process::Command::new("bash")
            .args([
                "-c",
                &format!(
                    "source {}/bin/activate && pip install --upgrade pip",
                    venv_path.to_str().unwrap()
                ),
            ])
            .output()
            .await?;

        if !pip_upgrade.status.success() {
            let error = String::from_utf8_lossy(&pip_upgrade.stderr);
            return Err(format_err!("Failed to upgrade pip: {}", error));
        }

        let layer = Layer::from_dir(&venv_path).await?;

        // Cleanup temp directory
        if let Err(e) = temp_dir.close() {
            tracing::warn!("Failed to cleanup temporary directory: {}", e);
        }

        Ok(BuildOutput {
            layer,
            config: self.venv_config()?,
        })
    }

    async fn create_deps_layer(&self) -> Result<BuildOutput> {
        tracing::debug!("Creating dependencies layer");

        let requirements = self.project_path.join("requirements.txt");
        if !requirements.exists() {
            return Err(format_err!("requirements.txt not found"));
        }

        let temp_dir = tempfile::tempdir()?;
        let deps_path = temp_dir.path().join("deps");

        // Install dependencies
        let output = tokio::process::Command::new("pip")
            .args([
                "install",
                "--target",
                deps_path.to_str().unwrap(),
                "-r",
                requirements.to_str().unwrap(),
            ])
            .output()
            .await?;

        if !output.status.success() {
            return Err(format_err!("Failed to install dependencies"));
        }

        // Create layer from dependencies
        let layer = Layer::from_dir(&deps_path).await?;

        Ok(BuildOutput {
            layer,
            config: self.deps_config()?,
        })
    }

    async fn create_app_layer(&self) -> Result<BuildOutput> {
        tracing::debug!("Creating application layer");

        let temp_dir = tempfile::tempdir()?;
        let app_path = temp_dir.path().join("app");

        // Copy application files
        tokio::fs::create_dir(&app_path).await?;

        copy_dir_all(self.project_path.clone(), app_path.clone()).await?;

        // Remove unnecessary files
        for pattern in &[
            "venv",
            "__pycache__",
            "*.pyc",
            "*.pyo",
            ".git",
            ".pytest_cache",
        ] {
            remove_matching_files(&app_path, pattern).await?;
        }

        // Create layer from application code
        let layer = Layer::from_dir(&app_path).await?;

        Ok(BuildOutput {
            layer,
            config: self.app_config()?,
        })
    }

    fn generate_config(&self, configs: &[&ImageConfig]) -> Result<OCIConfig> {
        // Start with default configuration
        let mut final_config = OCIConfig::default();
        let mut env = Vec::new();

        // Merge configurations manually
        for config in configs {
            let config_env = &config.env;

            if !config_env.is_empty() {
                env.extend(config_env.clone());
            }

            if !config.working_dir.is_empty() {
                final_config.set_working_dir(Some(config.working_dir.clone()));
            }

            if !config.cmd.is_empty() {
                final_config.set_cmd(Some(config.cmd.clone()));
            }
        }

        // Add Python-specific environment variables
        env.push("PYTHONUNBUFFERED=1".to_string());
        env.push("PYTHONDONTWRITEBYTECODE=1".to_string());
        env.push("PYTHONPATH=/app/deps:/app".to_string());

        final_config.set_env(Some(env));

        final_config.set_entrypoint(Some(vec![
            "/bin/sh".to_string(),
            "-c".to_string(),
            format!(
                "source /venv/bin/activate && python /app/{}",
                self.config.entrypoint.join(" "),
            ),
        ]));

        Ok(final_config)
    }

    async fn pull_base_image(&self) -> Result<BaseImage> {
        tracing::info!("Pulling base image: {}", self.base_image);

        // Check cache first
        if let Some(cached) = self.cache.get_layer(&self.base_image).await {
            return Ok(BaseImage {
                layer: cached,
                config: ImageConfig::default(), // Load from cache in production
            });
        }

        // TODO: Implement actual base image pulling
        // For now, return a minimal base layer
        Ok(BaseImage {
            layer: Layer {
                media_type: "application/vnd.oci.image.layer.v1.tar+gzip".to_string(),
                digest: "sha256:empty".to_string(),
                size: 0,
                compressed_size: 0,
                data: vec![],
                diff_id: "sha256:empty".to_string(),
                annotations: Default::default(),
            },
            config: ImageConfig::default(),
        })
    }

    fn venv_config(&self) -> Result<ImageConfig> {
        Ok(ImageConfig {
            env: vec![
                "VIRTUAL_ENV=/venv".to_string(),
                "PATH=/venv/bin:$PATH".to_string(),
            ],
            ..ImageConfig::default()
        })
    }

    fn deps_config(&self) -> Result<ImageConfig> {
        Ok(ImageConfig {
            env: vec!["PYTHONPATH=/app/deps:$PYTHONPATH".to_string()],
            ..ImageConfig::default()
        })
    }

    fn app_config(&self) -> Result<ImageConfig> {
        Ok(ImageConfig {
            working_dir: "/app".to_string(),
            cmd: self.config.cmd.clone(),
            env: self.config.env.clone(),
            ..ImageConfig::default()
        })
    }

    fn create_manifest(&self, config: OCIConfig, layers: Vec<Layer>) -> Result<Manifest> {
        // Calculate config JSON size and digest
        let config_json = serde_json::to_vec(&config)?;
        let mut hasher = Sha256::new();
        hasher.update(&config_json);
        let config_digest = format!("sha256:{:x}", hasher.finalize());

        Manifest::new(config, layers, config_json.len() as u64, config_digest)
    }

    async fn write_image(&self, config: OCIConfig, manifest: Manifest) -> Result<()> {
        // Create output directory structure
        let blobs_dir = self.output_path.join("blobs/sha256");
        tokio::fs::create_dir_all(&blobs_dir).await?;

        // Write config
        let config_json = serde_json::to_vec_pretty(&config)?;
        let mut hasher = Sha256::new();
        hasher.update(&config_json);
        let config_digest = format!("{:x}", hasher.finalize());
        tokio::fs::write(blobs_dir.join(&config_digest), config_json).await?;

        // Write layers
        for layer in &manifest.layers {
            let digest = layer.digest.trim_start_matches("sha256:");
            if let Some(data) = &layer.data {
                tokio::fs::write(blobs_dir.join(digest), data).await?;
            } else {
                return Err(format_err!("Layer data is missing"));
            }
        }

        // Write manifest
        let manifest_json = serde_json::to_vec_pretty(&manifest)?;
        tokio::fs::write(self.output_path.join("manifest.json"), manifest_json).await?;

        // Write OCI layout file
        let layout = serde_json::json!({
            "imageLayoutVersion": "1.0.0"
        });
        tokio::fs::write(
            self.output_path.join("oci-layout"),
            serde_json::to_vec_pretty(&layout)?,
        )
        .await?;

        Ok(())
    }
}
