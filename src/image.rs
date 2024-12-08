use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::fs;
use std::{collections::HashMap, path::Path};

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct ImageConfig {
    pub env: Vec<String>,
    pub cmd: Vec<String>,
    pub working_dir: String,
    pub entrypoint: Vec<String>,
    pub labels: HashMap<String, String>,
    pub exposed_ports: HashMap<String, HashMap<(), ()>>,
    pub volumes: HashMap<String, HashMap<(), ()>>,
}

impl ImageConfig {
    pub fn from_project(project_path: &Path) -> Result<Self> {
        let pyproject = project_path.join("pyproject.toml");
        if pyproject.exists() {
            Self::from_pyproject(&pyproject)
        } else {
            Self::default_config()
        }
    }

    fn default_config() -> Result<Self> {
        Ok(Self {
            env: vec![
                "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin".to_string(),
                "PYTHONUNBUFFERED=1".to_string(),
            ],
            cmd: vec!["python".to_string(), "main.py".to_string()],
            working_dir: "/app".to_string(),
            entrypoint: vec![],
            labels: HashMap::new(),
            exposed_ports: HashMap::new(),
            volumes: HashMap::new(),
        })
    }

    fn from_pyproject(path: &Path) -> Result<Self> {
        let content = fs::read_to_string(path)?;
        let pyproject: toml::Value = toml::from_str(&content)?;

        // Return default config if tool.spacejar section is missing
        pyproject
            .get("tool")
            .and_then(|t| t.get("spacejar"))
            .map_or_else(
                Self::default_config,
                |tool| {
                    let mut config = Self::default_config()?;

                    if let Some(env) = tool.get("env").and_then(|e| e.as_array()) {
                        config
                            .env
                            .extend(env.iter().filter_map(|v| v.as_str()).map(String::from));
                    }

                    if let Some(cmd) = tool.get("cmd").and_then(|c| c.as_array()) {
                        config.cmd = cmd
                            .iter()
                            .filter_map(|v| v.as_str())
                            .map(String::from)
                            .collect();
                    }

                    if let Some(working_dir) = tool.get("working_dir").and_then(|w| w.as_str()) {
                        config.working_dir = working_dir.to_string();
                    }

                    if let Some(entrypoint) = tool.get("entrypoint").and_then(|e| e.as_array()) {
                        config.entrypoint = entrypoint
                            .iter()
                            .filter_map(|v| v.as_str())
                            .map(String::from)
                            .collect();
                    }

                    if let Some(ports) = tool.get("ports").and_then(|p| p.as_array()) {
                        for port in ports.iter().filter_map(|v| v.as_str()) {
                            config
                                .exposed_ports
                                .insert(port.to_string(), HashMap::new());
                        }
                    }

                    if let Some(volumes) = tool.get("volumes").and_then(|v| v.as_array()) {
                        for volume in volumes.iter().filter_map(|v| v.as_str()) {
                            config.volumes.insert(volume.to_string(), HashMap::new());
                        }
                    }

                    Ok(config)
                },
            )
    }
}
