use base64::Engine as _;
use serde::{Deserialize, Deserializer, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ImageValidationConfig {
    pub default: String,
    pub allow: Vec<String>,
    pub deny: Vec<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct ConfigFile {
    #[serde(deserialize_with = "de_unwrap_or_default")]
    pub registry_proxies: RegistryProxiesConfig,
    pub image_validation: Option<ImageValidationConfig>,
}

fn de_unwrap_or_default<'de, T, D>(d: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de> + Default,
{
    Deserialize::deserialize(d).map(|x: Option<_>| x.unwrap_or_default())
}

#[derive(Default, Serialize, Deserialize, Clone, Debug)]
pub struct RegistryProxyConfigs(Vec<SingleRegistryProxyConfig>);

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct RegistryProxiesConfig {
    #[serde(default)]
    pub registries: RegistryProxyConfigs,
    #[serde(default)]
    pub offline: bool,
    #[serde(default)]
    pub max_size: Option<size::Size>,
    /// When enabled, proxied manifests are served as soon as the manifest
    /// itself is fetched, and layers are downloaded on demand — each blob GET
    /// streams bytes to the client while the download into the cache is still
    /// in progress (concurrent requests for the same blob share one upstream
    /// download). When disabled (default, upstream behavior), a manifest GET
    /// blocks until every layer is fully cached.
    #[serde(default)]
    pub stream: bool,
}

fn normalize_path_prefix<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let opt = Option::<String>::deserialize(deserializer)?;
    Ok(opt
        .map(|s| s.trim_matches('/').to_string())
        .filter(|s| !s.is_empty()))
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct SingleRegistryProxyConfig {
    /// What containerd calls "namespace" (ghcr.io, docker.io, ...)
    /// This can be empty !!
    pub host: String,
    /// Optional path prefix for scoped credential matching.
    /// Allows different credentials for different projects on the same registry host.
    /// Example: "system" matches repos like "system/app", "system/worker".
    /// When multiple entries match the same host, the longest matching prefix wins.
    #[serde(default, deserialize_with = "normalize_path_prefix")]
    pub path_prefix: Option<String>,
    /// TODO: insecure currently means "use HTTP", we should also support self-signed TLS
    #[serde(default)]
    pub insecure: bool,
    pub username: Option<String>,
    pub password: Option<String>,
    /// Read the password from this file instead of embedding it here.
    /// Lets a licence token / registry password reach trow through a mounted
    /// secret while the config itself stays plain enough to keep in git:
    /// trow has no env interpolation and its config is rendered into a
    /// ConfigMap, so an inline `password` would put the secret in cleartext
    /// in the repo AND in the ConfigMap. Takes precedence over `password`.
    /// The file is read at every credential fetch, so rotating the mounted
    /// secret takes effect without restarting the pod.
    pub password_file: Option<String>,
    /// Read the password out of a mounted Kubernetes `.dockerconfigjson`
    /// (a `kubernetes.io/dockerconfigjson` Secret), picking the entry that
    /// matches this proxy's `host`.
    ///
    /// This is the preferred form when the credential ALREADY exists in the
    /// cluster as a pull secret: the file is cloned to the namespace as-is,
    /// so nothing has to re-type, transform or re-store the password, and it
    /// keeps tracking the source of truth on rotation. Takes precedence over
    /// both `password_file` and `password`.
    pub password_docker_config_file: Option<String>,
}

/// Extract the password for `host` from a `.dockerconfigjson` document.
/// Handles both shapes docker writes: an explicit `password`, and the
/// base64 `auth` blob holding `user:password`.
fn password_from_docker_config(doc: &str, host: &str) -> Option<String> {
    let parsed: serde_json::Value = serde_json::from_str(doc).ok()?;
    let auths = parsed.get("auths")?.as_object()?;
    // Exact host first; fall back to the sole entry, which is what a
    // single-registry pull secret looks like.
    let entry = auths.get(host).or_else(|| {
        if auths.len() == 1 {
            auths.values().next()
        } else {
            None
        }
    })?;
    if let Some(p) = entry.get("password").and_then(|v| v.as_str()) {
        return Some(p.to_string());
    }
    let auth = entry.get("auth").and_then(|v| v.as_str())?;
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(auth)
        .ok()?;
    let decoded = String::from_utf8(decoded).ok()?;
    decoded.split_once(':').map(|(_, p)| p.to_string())
}

impl SingleRegistryProxyConfig {
    /// Resolve the password: `password_file` wins over the inline `password`.
    /// A missing or unreadable file is logged and treated as "no password" —
    /// the pull then fails with the registry's own 401, which is a far
    /// clearer signal than trow refusing to start.
    pub fn resolve_password(&self) -> Option<String> {
        if let Some(path) = self.password_docker_config_file.as_deref() {
            match std::fs::read_to_string(path) {
                Ok(doc) => match password_from_docker_config(&doc, &self.host) {
                    Some(p) => return Some(p),
                    None => {
                        tracing::error!(
                            path = path,
                            host = %self.host,
                            "No credentials for this host in the mounted dockerconfigjson"
                        );
                        return None;
                    }
                },
                Err(e) => {
                    tracing::error!(
                        path = path,
                        "Could not read proxy password_docker_config_file: {e} — proceeding without a password"
                    );
                    return None;
                }
            }
        }
        if let Some(path) = self.password_file.as_deref() {
            match std::fs::read_to_string(path) {
                Ok(s) => return Some(s.trim().to_string()),
                Err(e) => {
                    tracing::error!(
                        path = path,
                        "Could not read proxy password_file: {e} — proceeding without a password"
                    );
                    return None;
                }
            }
        }
        self.password.clone()
    }
}

impl Default for RegistryProxiesConfig {
    fn default() -> Self {
        RegistryProxiesConfig {
            registries: RegistryProxyConfigs(Vec::new()),
            offline: true,
            max_size: None,
            stream: false,
        }
    }
}

impl RegistryProxyConfigs {
    pub fn get_for<'a>(
        &'a self,
        registry: &str,
        repo: &str,
    ) -> Option<&'a SingleRegistryProxyConfig> {
        let matches = self.0.iter().filter_map(|proxy| {
            if proxy.host == registry {
                if let Some(proxy_prefix) = proxy.path_prefix.as_deref() {
                    // for prefix "org" match org/toto, not org_b/toto
                    if repo == proxy_prefix
                        || (repo.starts_with(proxy_prefix)
                            && repo.as_bytes().get(proxy_prefix.len()) == Some(&b'/'))
                    {
                        return Some((proxy_prefix.len(), proxy));
                    }
                } else {
                    return Some((0, proxy));
                }
            }
            None
        });
        matches
            .max_by_key(|(prefix_len, _)| *prefix_len)
            .map(|(_, registry)| registry)
    }
}

impl From<Vec<SingleRegistryProxyConfig>> for RegistryProxyConfigs {
    fn from(vec: Vec<SingleRegistryProxyConfig>) -> Self {
        RegistryProxyConfigs(vec)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_registry_proxy_deserialize_path_prefix() {
        let proxy_config: SingleRegistryProxyConfig =
            serde_json::from_str(r#"{"host": "registry.example.com", "path_prefix": "/org/sub/"}"#)
                .unwrap();
        assert_eq!(proxy_config.path_prefix.unwrap(), "org/sub");

        let proxy_config: SingleRegistryProxyConfig =
            serde_json::from_str(r#"{"host": "registry.example.com", "path_prefix": "/"}"#)
                .unwrap();
        assert_eq!(proxy_config.path_prefix, None);
    }

    #[test]
    fn test_registry_proxy_configs_path_prefix_longest_match_wins() {
        let config = RegistryProxiesConfig {
            registries: vec![
                SingleRegistryProxyConfig {
                    host: "registry.example.com".to_string(),
                    username: Some("default".to_string()),
                    ..Default::default()
                },
                SingleRegistryProxyConfig {
                    host: "registry.example.com".to_string(),
                    path_prefix: Some("org".to_string()),
                    username: Some("org-token".to_string()),
                    ..Default::default()
                },
                SingleRegistryProxyConfig {
                    host: "registry.example.com".to_string(),
                    path_prefix: Some("org/sub".to_string()),
                    username: Some("org-sub-token".to_string()),
                    ..Default::default()
                },
            ]
            .into(),
            ..Default::default()
        };
        // "org/sub/app" matches both, but "org/sub" is longer
        let proxy = config
            .registries
            .get_for("registry.example.com", "org/sub/app");
        assert_eq!(proxy.unwrap().username, Some("org-sub-token".to_string()));

        // "org/other" matches only "org"
        let proxy = config
            .registries
            .get_for("registry.example.com", "org/other");
        assert_eq!(proxy.unwrap().username, Some("org-token".to_string()));

        // no path_prefix match
        let proxy = config
            .registries
            .get_for("registry.example.com", "outta-this-world");
        assert_eq!(proxy.unwrap().username, Some("default".to_string()));

        // doesn't match path prefix across '/' boundary
        let proxy = config
            .registries
            .get_for("registry.example.com", "org_b/app");
        assert_eq!(proxy.unwrap().username, Some("default".to_string()));
    }
}

#[cfg(test)]
mod docker_config_tests {
    use super::*;

    fn cfg_with(path: &str, host: &str) -> SingleRegistryProxyConfig {
        SingleRegistryProxyConfig {
            host: host.to_string(),
            password_docker_config_file: Some(path.to_string()),
            ..Default::default()
        }
    }

    #[test]
    fn reads_explicit_password_for_the_matching_host() {
        let dir = test_temp_dir::test_temp_dir!();
        let p = dir.as_path_untracked().join("cfg.json");
        std::fs::write(
            &p,
            r#"{"auths":{"registry.deckhouse.io":{"username":"license-token","password":"tok-1"},
                         "other.io":{"password":"nope"}}}"#,
        )
        .unwrap();
        let cfg = cfg_with(p.to_str().unwrap(), "registry.deckhouse.io");
        assert_eq!(cfg.resolve_password().as_deref(), Some("tok-1"));
    }

    #[test]
    fn decodes_the_base64_auth_blob() {
        // docker login writes `auth: base64(user:password)` and often no
        // explicit password field at all.
        let dir = test_temp_dir::test_temp_dir!();
        let p = dir.as_path_untracked().join("cfg.json");
        let blob = base64::engine::general_purpose::STANDARD.encode("license-token:tok-2");
        std::fs::write(
            &p,
            format!(r#"{{"auths":{{"registry.deckhouse.io":{{"auth":"{blob}"}}}}}}"#),
        )
        .unwrap();
        let cfg = cfg_with(p.to_str().unwrap(), "registry.deckhouse.io");
        assert_eq!(cfg.resolve_password().as_deref(), Some("tok-2"));
    }

    #[test]
    fn falls_back_to_the_sole_entry() {
        // A single-registry pull secret may key the entry by a URL form that
        // does not string-match our `host`; with exactly one entry there is
        // no ambiguity.
        let dir = test_temp_dir::test_temp_dir!();
        let p = dir.as_path_untracked().join("cfg.json");
        std::fs::write(
            &p,
            r#"{"auths":{"https://registry.deckhouse.io/v2/":{"password":"tok-3"}}}"#,
        )
        .unwrap();
        let cfg = cfg_with(p.to_str().unwrap(), "registry.deckhouse.io");
        assert_eq!(cfg.resolve_password().as_deref(), Some("tok-3"));
    }

    #[test]
    fn no_match_among_several_entries_yields_none() {
        let dir = test_temp_dir::test_temp_dir!();
        let p = dir.as_path_untracked().join("cfg.json");
        std::fs::write(
            &p,
            r#"{"auths":{"a.io":{"password":"x"},"b.io":{"password":"y"}}}"#,
        )
        .unwrap();
        let cfg = cfg_with(p.to_str().unwrap(), "registry.deckhouse.io");
        assert_eq!(cfg.resolve_password(), None);
    }

    #[test]
    fn missing_file_is_not_fatal() {
        let cfg = cfg_with("/nonexistent/dockerconfig.json", "registry.deckhouse.io");
        assert_eq!(cfg.resolve_password(), None);
    }

    #[test]
    fn docker_config_wins_over_password_file_and_inline() {
        let dir = test_temp_dir::test_temp_dir!();
        let dc = dir.as_path_untracked().join("cfg.json");
        let pf = dir.as_path_untracked().join("plain");
        std::fs::write(&dc, r#"{"auths":{"r.io":{"password":"from-dockercfg"}}}"#).unwrap();
        std::fs::write(&pf, "from-password-file").unwrap();
        let cfg = SingleRegistryProxyConfig {
            host: "r.io".to_string(),
            password: Some("inline".to_string()),
            password_file: Some(pf.to_string_lossy().to_string()),
            password_docker_config_file: Some(dc.to_string_lossy().to_string()),
            ..Default::default()
        };
        assert_eq!(cfg.resolve_password().as_deref(), Some("from-dockercfg"));
    }
}
