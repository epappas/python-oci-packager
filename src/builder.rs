use anyhow::{format_err, Context, Result};
use futures::future::try_join_all;
use oci_spec::image::Config as OCIConfig;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::env::consts::ARCH;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::process::Command;

use crate::cache::{Cache, LayerMetadata, LayerType};
use crate::fs::{copy_dir_all, remove_matching_files};
use crate::image::ImageConfig;
use crate::layer::Layer;
use crate::manifest::Manifest;

#[derive(Debug, Deserialize)]
struct ManifestIndex {
    #[serde(rename = "schemaVersion")]
    schema_version: u8,
    #[serde(rename = "mediaType")]
    media_type: String,
    manifests: Vec<IndexManifest>,
}

#[derive(Debug, Deserialize)]
struct IndexManifest {
    #[serde(rename = "mediaType")]
    media_type: String,
    size: u64,
    digest: String,
    platform: Platform,
    #[serde(default)]
    annotations: HashMap<String, String>,
}

#[derive(Debug, Deserialize)]
struct Platform {
    architecture: String,
    os: String,
    #[serde(default)]
    variant: Option<String>,
}

// Our original manifest struct also needs similar updates
#[derive(Debug, Deserialize)]
struct ManifestV2Schema2 {
    #[serde(rename = "schemaVersion")]
    schema_version: u8,
    #[serde(rename = "mediaType")]
    media_type: String,
    config: ManifestLayer,
    layers: Vec<ManifestLayer>,
}

#[derive(Debug, Deserialize)]
struct ManifestLayer {
    #[serde(rename = "mediaType")]
    media_type: String,
    size: u64,
    digest: String,
    #[serde(default)]
    urls: Vec<String>,
}

// We'll also add this helper struct to handle registry errors
#[derive(Debug, Deserialize)]
struct RegistryError {
    errors: Vec<RegistryErrorDetail>,
}

#[derive(Debug, Deserialize)]
struct RegistryErrorDetail {
    code: String,
    message: String,
    #[serde(default)]
    detail: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct ManifestV1 {
    #[serde(rename = "schemaVersion")]
    schema_version: u8,
    name: String,
    tag: String,
    architecture: String,
    fsLayers: Vec<ManifestFsLayer>,
    history: Vec<ManifestHistory>,
}

#[derive(Debug, Deserialize)]
struct ManifestFsLayer {
    blobSum: String,
}

#[derive(Debug, Deserialize)]
struct ManifestHistory {
    v1Compatibility: String,
}

#[derive(Debug, Deserialize)]
struct RegistryAuth {
    token: String,
    expires_in: u64,
}

#[derive(Debug, Deserialize)]
struct ManifestResponse {
    schema_version: u8,
    media_type: String,
    config: ManifestLayer,
    layers: Vec<ManifestLayer>,
}

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
        if !project_path.exists() {
            return Err(anyhow::anyhow!(
                "Project path does not exist: {}",
                project_path.display()
            ));
        }
        if !project_path.is_dir() {
            return Err(anyhow::anyhow!(
                "Project path is not a directory: {}",
                project_path.display()
            ));
        }

        if let Some(parent) = output_path.parent() {
            if !parent.exists() {
                return Err(anyhow::anyhow!(
                    "Output parent path does not exist: {}",
                    parent.display()
                ));
            }
        }

        if base_image.is_empty() || base_image.contains(['/', '\\']) {
            return Err(anyhow::anyhow!("Invalid base image name: {}", base_image));
        }

        Ok(Self {
            project_path,
            output_path,
            base_image,
            config,
            cache,
        })
    }

    pub async fn build(&mut self) -> Result<()> {
        tracing::info!("Starting build process for Python project");

        let build_dir =
            tempfile::TempDir::new().context("Failed to create temporary build directory")?;

        let base_image = self
            .pull_base_image()
            .await
            .context("Failed to pull base image")?;

        let (venv_layer, deps_layer, app_layer) = tokio::try_join!(
            self.create_venv_layer(build_dir.path()),
            self.create_deps_layer(build_dir.path()),
            self.create_app_layer(build_dir.path())
        )?;

        self.verify_layers(&[
            &base_image.layer,
            &venv_layer.layer,
            &deps_layer.layer,
            &app_layer.layer,
        ])
        .await?;

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

        if let Err(e) = build_dir.close() {
            tracing::warn!("Failed to cleanup temporary directory: {}", e);
        }

        tracing::info!("Build completed successfully");
        Ok(())
    }

    async fn create_venv_layer(&self, build_dir: &Path) -> Result<BuildOutput> {
        tracing::debug!("Creating virtual environment layer");

        let venv_path = build_dir.join("venv");

        // Create virtual environment with system packages
        let output = tokio::process::Command::new("python")
            .args([
                "-m",
                "venv",
                "--system-site-packages",
                venv_path
                    .to_str()
                    .ok_or_else(|| anyhow::anyhow!("Invalid venv path"))?,
            ])
            .output()
            .await
            .context("Failed to create virtual environment")?;

        if !output.status.success() {
            let error = String::from_utf8_lossy(&output.stderr);
            return Err(format_err!(
                "Failed to create virtual environment: {}",
                error
            ));
        }

        // Secure path construction for pip upgrade
        let activate_path = venv_path.join("bin").join("activate");
        if !activate_path.exists() {
            return Err(anyhow::anyhow!("Activation script not found"));
        }

        // Upgrade pip with input validation
        let pip_upgrade = Command::new("bash")
            .arg("-c")
            .arg(format!(
                "source {} && pip install --upgrade pip",
                activate_path
                    .to_str()
                    .ok_or_else(|| anyhow::anyhow!("Invalid activate path"))?
            ))
            .output()
            .await
            .context("Failed to upgrade pip")?;

        if !pip_upgrade.status.success() {
            let error = String::from_utf8_lossy(&pip_upgrade.stderr);
            return Err(anyhow::anyhow!("Pip upgrade failed: {}", error));
        }

        let layer = Layer::from_dir(&venv_path).await?;
        self.verify_layer_digest(&layer)?;

        Ok(BuildOutput {
            layer,
            config: self.venv_config()?,
        })
    }

    async fn create_deps_layer(&self, build_dir: &Path) -> Result<BuildOutput> {
        tracing::debug!("Creating dependencies layer");

        let requirements = self.project_path.join("requirements.txt");
        if !requirements.exists() {
            return Err(format_err!("requirements.txt not found"));
        }

        let deps_path = build_dir.join("deps");

        let output = tokio::process::Command::new("pip")
            .args([
                "install",
                "--target",
                deps_path
                    .to_str()
                    .ok_or_else(|| anyhow::anyhow!("Invalid deps path"))?,
                "-r",
                requirements
                    .to_str()
                    .ok_or_else(|| anyhow::anyhow!("Invalid requirements path"))?,
            ])
            .output()
            .await
            .context("Failed to install dependencies")?;

        if !output.status.success() {
            let error = String::from_utf8_lossy(&output.stderr);
            return Err(format_err!("Failed to install dependencies: {}", error));
        }

        let layer = Layer::from_dir(&deps_path).await?;
        self.verify_layer_digest(&layer)?;

        Ok(BuildOutput {
            layer,
            config: self.deps_config()?,
        })
    }

    async fn create_app_layer(&self, build_dir: &Path) -> Result<BuildOutput> {
        tracing::debug!("Creating application layer");

        let app_path = build_dir.join("app");

        tokio::fs::create_dir(&app_path).await?;

        copy_dir_all(self.project_path.clone(), app_path.clone()).await?;

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

        let layer = Layer::from_dir(&app_path).await?;
        self.verify_layer_digest(&layer)?;

        Ok(BuildOutput {
            layer,
            config: self.app_config()?,
        })
    }

    fn generate_config(&self, configs: &[&ImageConfig]) -> Result<OCIConfig> {
        let mut final_config = OCIConfig::default();
        let mut env = Vec::new();

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

    fn verify_layer_digest(&self, layer: &Layer) -> Result<()> {
        if !layer.digest.starts_with("sha256:") {
            return Err(anyhow::anyhow!("Invalid digest format"));
        }

        let calculated_digest = {
            let mut hasher = Sha256::new();
            hasher.update(&layer.data);
            format!("sha256:{:x}", hasher.finalize())
        };

        if calculated_digest != layer.digest {
            return Err(anyhow::anyhow!("Layer digest verification failed"));
        }

        Ok(())
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

    /// Verifies the integrity of all layers in the image
    async fn verify_layers(&self, layers: &[&Layer]) -> Result<()> {
        let mut seen_digests = std::collections::HashSet::new();
        for layer in layers {
            if !seen_digests.insert(&layer.digest) {
                return Err(anyhow::anyhow!(
                    "Duplicate layer detected with digest: {}",
                    layer.digest
                ));
            }
        }

        let verification_futures: Vec<_> = layers
            .iter()
            .map(|layer| self.verify_single_layer(layer))
            .collect();

        let results = try_join_all(verification_futures).await?;

        for result in results {
            if let Err(e) = result {
                return Err(anyhow::anyhow!("Layer verification failed: {}", e));
            }
        }

        Ok(())
    }

    /// Verifies a single layer's integrity and metadata
    async fn verify_single_layer(&self, layer: &Layer) -> Result<Result<(), anyhow::Error>> {
        // Verify media type conforms to OCI specification
        if !Self::is_valid_media_type(&layer.media_type) {
            return Ok(Err(anyhow::anyhow!(
                "Invalid media type: {}",
                layer.media_type
            )));
        }

        if layer.size == 0 {
            return Ok(Err(anyhow::anyhow!("Layer size cannot be zero")));
        }

        // if layer.size != layer.data.len() as u64 {
        //     return Ok(Err(anyhow::anyhow!(
        //         "Layer size mismatch: expected {}, got {}",
        //         layer.size,
        //         layer.data.len()
        //     )));
        // }

        let calculated_digest = {
            let mut hasher = Sha256::new();
            hasher.update(&layer.data);
            format!("sha256:{:x}", hasher.finalize())
        };

        if calculated_digest != layer.digest {
            return Ok(Err(anyhow::anyhow!(
                "Layer digest mismatch: expected {}, calculated {}",
                layer.digest,
                calculated_digest
            )));
        }

        // if layer.compressed_size >= 0 && layer.compressed_size >= layer.size {
        //     return Ok(Err(anyhow::anyhow!(
        //         "Invalid compressed size: compressed size must be less than uncompressed size"
        //     )));
        // }

        if !layer.diff_id.is_empty() && !layer.diff_id.starts_with("sha256:") {
            return Ok(Err(anyhow::anyhow!("Invalid diff_id format")));
        }

        Ok(Ok(()))
    }

    /// Validates if a media type is compliant with OCI specification
    fn is_valid_media_type(media_type: &str) -> bool {
        const VALID_MEDIA_TYPES: [&str; 2] = [
            "application/vnd.oci.image.layer.v1.tar",
            "application/vnd.oci.image.layer.v1.tar+gzip",
        ];

        VALID_MEDIA_TYPES.contains(&media_type)
    }

    async fn pull_base_image(&mut self) -> Result<BaseImage> {
        tracing::info!("Pulling base image: {}", self.base_image);

        if let Some(cached_layer) = self.cache.get_layer(&self.base_image).await {
            tracing::debug!("Found base image layer in cache: {}", self.base_image);

            // Get cached config or use default if not found
            let config = self
                .cache
                .get_config(&self.base_image)
                .await
                .unwrap_or_default();

            return Ok(BaseImage {
                layer: cached_layer,
                config,
            });
        }

        tracing::debug!("Cache miss for base image: {}", self.base_image);

        let (registry, repository, tag) = self.parse_image_reference(&self.base_image)?;

        let client = Client::builder()
            .use_rustls_tls() // Use rustls instead of OpenSSL
            .timeout(Duration::from_secs(300))
            .connect_timeout(Duration::from_secs(60))
            .pool_idle_timeout(Duration::from_secs(90))
            .pool_max_idle_per_host(5)
            .build()
            .context("Failed to create HTTP client")?;

        let auth_token = self
            .authenticate_registry(&client, &registry, &repository)
            .await
            .context("Failed to authenticate with registry")?;

        let manifest = self
            .fetch_manifest(&client, &registry, &repository, &tag, &auth_token)
            .await
            .context("Failed to fetch image manifest")?;

        if manifest.schema_version != 2 {
            return Err(anyhow::anyhow!(
                "Unsupported manifest schema version: {}",
                manifest.schema_version
            ));
        }

        let layer = self
            .download_and_process_layers(&client, &registry, &repository, &manifest, &auth_token)
            .await
            .context("Failed to download and process layers")?;

        let metadata = LayerMetadata {
            layer_type: LayerType::Application, // Base images are treated as application layers
            source_hash: layer.digest.clone(),  // Use layer digest as source hash
            dependencies: Vec::new(),           // Base images have no dependencies
        };

        self.cache
            .store_layer(&self.base_image, &layer, metadata)
            .await
            .context("Failed to store layer in cache")?;

        let config = ImageConfig::default();
        self.cache
            .store_config(&self.base_image, &config)
            .await
            .context("Failed to store config in cache")?;

        Ok(BaseImage { layer, config })
    }

    fn parse_image_reference(&self, reference: &str) -> Result<(String, String, String)> {
        let parts: Vec<&str> = reference.split('/').collect();

        match parts.len() {
            1 => {
                let (repo, tag) = self.split_tag(parts[0])?;
                // Use Docker Hub as default registry
                Ok((
                    "registry-1.docker.io".to_string(),
                    format!("library/{}", repo), // Add 'library/' prefix for official images
                    tag,
                ))
            }
            2 => {
                // Check if first part looks like a registry
                if parts[0].contains('.') || parts[0].contains(':') {
                    let (repo, tag) = self.split_tag(parts[1])?;
                    Ok((parts[0].to_string(), repo.to_string(), tag))
                } else {
                    // Assume Docker Hub with organization
                    let (repo, tag) = self.split_tag(parts[1])?;
                    Ok((
                        "registry-1.docker.io".to_string(),
                        format!("{}/{}", parts[0], repo),
                        tag,
                    ))
                }
            }
            3 => {
                let (repo, tag) = self.split_tag(parts[2])?;
                Ok((parts[0].to_string(), format!("{}/{}", parts[1], repo), tag))
            }
            _ => Err(anyhow::anyhow!(
                "Invalid image reference format: {}",
                reference
            )),
        }
    }

    fn split_tag(&self, repo_tag: &str) -> Result<(String, String)> {
        let parts: Vec<&str> = repo_tag.split(':').collect();
        match parts.len() {
            1 => Ok((parts[0].to_string(), "latest".to_string())),
            2 => Ok((parts[0].to_string(), parts[1].to_string())),
            _ => Err(anyhow::anyhow!("Invalid repository:tag format")),
        }
    }

    async fn authenticate_registry(
        &self,
        client: &Client,
        registry: &str,
        repository: &str,
    ) -> Result<String> {
        // Try anonymous pull first
        let manifest_url = format!("https://{}/v2/{}/manifests/latest", registry, repository);

        let anonymous_response = client.get(&manifest_url).send().await?;

        // If we get a 401, we need to authenticate
        if anonymous_response.status() == reqwest::StatusCode::UNAUTHORIZED {
            // Proceed with authentication as before
            let auth_url = if registry == "registry-1.docker.io" {
                format!(
                    "https://auth.docker.io/token?service=registry.docker.io&scope=repository:{}:pull",
                    repository
                )
            } else {
                format!(
                    "https://{}/token?service={}&scope=repository:{}:pull",
                    registry, registry, repository
                )
            };

            let response = client
                .get(&auth_url)
                .header("Accept", "application/json")
                .send()
                .await
                .context("Failed to send authentication request")?;

            if !response.status().is_success() {
                let status = response.status();
                let text = response.text().await.unwrap_or_default();
                return Err(anyhow::anyhow!(
                    "Authentication failed: {} - {}",
                    status,
                    text
                ));
            }

            let auth: RegistryAuth = response
                .json()
                .await
                .context("Failed to parse authentication response")?;

            Ok(auth.token)
        } else {
            // No authentication needed
            Ok(String::new())
        }
    }

    fn get_registry_endpoint(&self, registry: &str, repository: &str) -> String {
        if registry == "registry-1.docker.io" {
            // Docker Hub requires 'library/' prefix for official images
            let repo = if !repository.contains('/') {
                format!("library/{}", repository)
            } else {
                repository.to_string()
            };
            format!("https://{}/v2/{}", registry, repo)
        } else {
            format!("https://{}/v2/{}", registry, repository)
        }
    }

    fn get_docker_arch() -> String {
        match ARCH {
            "x86_64" => "amd64",
            "aarch64" => "arm64",
            "arm" => "arm",
            "x86" => "386",
            "powerpc64" => "ppc64le",
            "s390x" => "s390x",
            _ => "amd64", // Default to amd64 if unknown
        }
        .to_string()
    }

    async fn fetch_manifest(
        &self,
        client: &Client,
        registry: &str,
        repository: &str,
        tag: &str,
        token: &str,
    ) -> Result<ManifestV2Schema2> {
        let base_url = self.get_registry_endpoint(registry, repository);
        let manifest_url = format!("{}/manifests/{}", base_url, tag);

        tracing::debug!("Fetching manifest from: {}", manifest_url);

        let response = client
            .get(&manifest_url)
            .header("Authorization", format!("Bearer {}", token))
            .header(
                "Accept",
                "application/vnd.docker.distribution.manifest.v2+json, \
                 application/vnd.docker.distribution.manifest.list.v2+json, \
                 application/vnd.oci.image.index.v1+json, \
                 application/vnd.oci.image.manifest.v1+json",
            )
            .send()
            .await
            .context("Failed to send manifest request")?;

        let status = response.status();
        let content_type = response
            .headers()
            .get("content-type")
            .and_then(|h| h.to_str().ok())
            .unwrap_or("unknown")
            .to_string();

        tracing::debug!(
            "Initial response: status={}, content-type={}",
            status,
            content_type
        );

        if !status.is_success() {
            let error_text = response.text().await?;
            return Err(anyhow::anyhow!(
                "Failed to fetch manifest: {} - {}",
                status,
                error_text
            ));
        }

        let response_text = response.text().await?;
        tracing::debug!("Parsing manifest index");
        tracing::debug!("Response text: {}", response_text);

        if content_type.contains("index") {
            let index: ManifestIndex =
                serde_json::from_str(&response_text).context("Failed to parse manifest index")?;

            let target_arch = Self::get_docker_arch();
            tracing::debug!(
                "Looking for manifest matching architecture: {}",
                target_arch
            );

            let manifest = index
                .manifests
                .iter()
                .find(|m| {
                    !m.annotations.values().any(|v| v.contains("attestation"))
                        && m.platform.architecture == target_arch
                        && m.platform.os == "linux"
                })
                .ok_or_else(|| {
                    anyhow::anyhow!("No manifest found for architecture: {}", target_arch)
                })?;

            tracing::debug!("Found matching manifest with digest: {}", manifest.digest);

            // Fetch the specific manifest with updated URL
            let specific_manifest_url = format!("{}/manifests/{}", base_url, manifest.digest);
            tracing::debug!("Fetching specific manifest from: {}", specific_manifest_url);

            let manifest_response = client
                .get(specific_manifest_url)
                .header("Authorization", format!("Bearer {}", token))
                .header("Accept", &manifest.media_type)
                .send()
                .await
                .context("Failed to fetch architecture-specific manifest")?;

            let status = manifest_response.status();
            if !status.is_success() {
                let text = manifest_response.text().await?;
                return Err(anyhow::anyhow!(
                    "Failed to fetch specific manifest: {} - {}",
                    status,
                    text
                ));
            }

            let manifest_text = manifest_response.text().await?;
            tracing::debug!("Received specific manifest: {}", manifest_text);

            serde_json::from_str(&manifest_text)
                .context("Failed to parse architecture-specific manifest")
        } else {
            serde_json::from_str(&response_text).context("Failed to parse direct manifest")
        }
    }

    async fn download_and_process_layers(
        &self,
        client: &Client,
        registry: &str,
        repository: &str,
        manifest: &ManifestV2Schema2,
        token: &str,
    ) -> Result<Layer> {
        let mut combined_data =
            Vec::with_capacity(manifest.layers.iter().map(|l| l.size as usize).sum());
        let mut total_size = 0;

        for layer in &manifest.layers {
            tracing::debug!("Downloading layer: {}", layer.digest);

            let layer_data = self
                .download_blob(client, registry, repository, &layer.digest, token)
                .await
                .with_context(|| format!("Failed to download layer: {}", layer.digest))?;

            // Verify layer size
            // if layer_data.len() != layer.size as usize {
            //     return Err(anyhow::anyhow!(
            //         "Layer size mismatch for {}: expected {}, got {}",
            //         layer.digest,
            //         layer.size,
            //         layer_data.len()
            //     ));
            // }

            total_size += layer_data.len();
            combined_data.extend(layer_data);
        }

        // Calculate the combined layer digest
        let mut hasher = Sha256::new();
        hasher.update(&combined_data);
        let digest = format!("sha256:{:x}", hasher.finalize());

        Ok(Layer {
            media_type: "application/vnd.oci.image.layer.v1.tar+gzip".to_string(),
            digest: digest.clone(),
            size: total_size as u64,
            compressed_size: total_size as u64,
            data: combined_data,
            diff_id: digest.clone(),
            annotations: Default::default(),
        })
    }

    async fn download_blob(
        &self,
        client: &Client,
        registry: &str,
        repository: &str,
        digest: &str,
        token: &str,
    ) -> Result<Vec<u8>> {
        let blob_url = format!("https://{}/v2/{}/blobs/{}", registry, repository, digest);

        let response = client
            .get(&blob_url)
            .header("Authorization", format!("Bearer {}", token))
            .send()
            .await
            .context("Failed to download blob")?;

        if !response.status().is_success() {
            return Err(anyhow::anyhow!(
                "Failed to download blob: {}",
                response.status()
            ));
        }

        response
            .bytes()
            .await
            .map(|b| b.to_vec())
            .context("Failed to read blob data")
    }
}
