use anyhow::Result;
use oci_spec::image::Config as OCIConfig;
use std::collections::HashMap;

use crate::layer::Layer;
use serde::Serialize;

#[derive(Serialize, Clone)]
pub struct ConfigDescriptor {
    pub config: OCIConfig,
    pub media_type: String,
    pub size: u64,
    pub digest: String,
}

#[derive(Serialize, Clone)]
pub struct LayerDescriptor {
    pub media_type: String,
    pub size: u64,
    pub digest: String,
    pub annotations: Option<HashMap<String, String>>,
    pub data: Option<Vec<u8>>,
}

#[derive(Serialize, Clone)]
pub struct Manifest {
    pub schema_version: i32,
    pub media_type: String,
    pub config: ConfigDescriptor,
    pub layers: Vec<LayerDescriptor>,
    pub annotations: Option<HashMap<String, String>>,
}

impl Manifest {
    pub fn new(config: OCIConfig, layers: Vec<Layer>, size: u64, digest: String) -> Result<Self> {
        let config_descriptor = ConfigDescriptor {
            config,
            media_type: "application/vnd.oci.image.config.v1+json".to_string(),
            size,
            digest,
        };

        let layer_descriptors = layers
            .iter()
            .map(|layer| LayerDescriptor {
                media_type: layer.media_type.clone(),
                size: layer.compressed_size,
                digest: layer.digest.clone(),
                annotations: Some(layer.annotations.clone()),
                data: Some(layer.data.clone()),
            })
            .collect();

        Ok(Self {
            schema_version: 2,
            media_type: "application/vnd.oci.image.manifest.v1+json".to_string(),
            config: config_descriptor,
            layers: layer_descriptors,
            annotations: None,
        })
    }

    pub fn to_json(&self) -> Result<String> {
        serde_json::to_string(self).map_err(Into::into)
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        serde_json::to_vec(self).map_err(Into::into)
    }

    pub fn to_string(&self) -> Result<String> {
        serde_json::to_string(self).map_err(Into::into)
    }

    pub fn to_writer<W: std::io::Write>(&self, writer: W) -> Result<()> {
        serde_json::to_writer(writer, self).map_err(Into::into)
    }
}
