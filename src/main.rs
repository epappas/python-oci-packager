mod builder;
mod cache;
mod fs;
mod image;
mod layer;
mod manifest;

use crate::builder::PythonImageBuilder;
use crate::cache::Cache;
use crate::image::ImageConfig;
use anyhow::{format_err, Result};
use clap::Parser;
use std::path::PathBuf;

#[derive(Parser)]
struct Cli {
    project_path: String,
    output: String,
    base_image: String,
    cache_dir: String,
}

#[tokio::main]

async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();

    // let output_path = PathBuf::from(&cli.output);
    let project_path: PathBuf = PathBuf::from(&cli.project_path);
    let cache_dir: PathBuf = PathBuf::from(&cli.cache_dir);

    println!("Building image for project: {}", project_path.display());
    println!("Output image: {}", cli.output);
    println!("Base image: {}", cli.base_image);

    let cache: Cache = Cache::new(cache_dir).await?;
    let image_config: ImageConfig = ImageConfig::from_project(&project_path)?;
    let base_image: String = match cli.base_image {
        s if s.is_empty() => "python:3.9-slim".to_string(),
        s => s,
    };

    let mut builder = PythonImageBuilder::new(
        PathBuf::from(cli.project_path),
        PathBuf::from(cli.output),
        base_image,
        image_config,
        cache,
    )
    .map_err(|e| format_err!("Failed to create image builder: {}", e))?;

    // TODO use cache
    // cache.get_layer()
    // cache.store_layer()
    // cache.get_dependency_layer()

    match builder.build().await {
        Ok(_) => {
            tracing::info!("Successfully built OCI image");
            Ok(())
        }
        Err(e) => {
            tracing::error!("Failed to build image: {}", e);
            Err(e)
        }
    }
}
