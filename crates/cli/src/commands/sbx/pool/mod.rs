pub mod claim;
pub mod create;
pub mod get;
pub mod ls;
pub mod rm;
pub mod update;

use crate::commands::sbx::apply_proxy_access_settings;

pub const DEFAULT_POOL_CPUS: f64 = 1.0;
pub const DEFAULT_POOL_MEMORY_MB: i64 = 1024;
pub const DEFAULT_POOL_DISK_MB: u64 = 1024;

/// Shared resource arguments that flow into create / update bodies.
///
/// Fields are `Option<_>` so the same builder can serve `create` (where `None`
/// means "use default") and `update` (where `None` means "don't change"). The
/// caller decides how to interpret `None`.
#[derive(Default, Clone)]
pub struct PoolBodyArgs<'a> {
    pub image: Option<&'a str>,
    pub cpus: Option<f64>,
    pub memory_mb: Option<i64>,
    pub disk_mb: Option<u64>,
    pub timeout: Option<i64>,
    pub entrypoint: &'a [String],
    pub max_containers: Option<i64>,
    pub warm_containers: Option<i64>,
    pub ports: &'a [u16],
    pub allow_unauthenticated_access: bool,
    pub no_internet: bool,
    pub network_allow: &'a [String],
    pub network_deny: &'a [String],
}

/// Build a fresh pool create body, falling back to library defaults for
/// resource fields. Used by `tl sbx pool create`.
pub fn build_pool_create_body(args: &PoolBodyArgs<'_>) -> serde_json::Value {
    let mut body = serde_json::json!({
        "resources": {
            "cpus": args.cpus.unwrap_or(DEFAULT_POOL_CPUS),
            "memory_mb": args.memory_mb.unwrap_or(DEFAULT_POOL_MEMORY_MB),
            "ephemeral_disk_mb": args.disk_mb.unwrap_or(DEFAULT_POOL_DISK_MB),
        },
    });

    if let Some(image) = args.image {
        body["image"] = serde_json::Value::String(image.to_string());
    }
    if let Some(t) = args.timeout {
        body["timeout_secs"] = serde_json::Value::Number(t.into());
    }
    if !args.entrypoint.is_empty() {
        body["entrypoint"] = serde_json::json!(args.entrypoint);
    }
    if let Some(m) = args.max_containers {
        body["max_containers"] = serde_json::Value::Number(m.into());
    }
    if let Some(w) = args.warm_containers {
        body["warm_containers"] = serde_json::Value::Number(w.into());
    }

    apply_proxy_access_settings(&mut body, args.ports, args.allow_unauthenticated_access);

    let has_network = args.no_internet || !args.network_allow.is_empty() || !args.network_deny.is_empty();
    if has_network {
        let mut network = serde_json::json!({});
        if args.no_internet {
            network["allow_internet_access"] = serde_json::Value::Bool(false);
        }
        if !args.network_allow.is_empty() {
            network["allow_out"] = serde_json::json!(args.network_allow);
        }
        if !args.network_deny.is_empty() {
            network["deny_out"] = serde_json::json!(args.network_deny);
        }
        body["network"] = network;
    }

    body
}

/// Apply the user's update overrides on top of a body parsed from the current
/// pool's `GET` response, producing a full `PUT` body. Only scalar/list
/// fields are exposed in `tl sbx pool update`; network/proxy access are
/// preserved as-is from the current state. The current state's
/// `network_policy` (response field) is renamed to `network` (request field).
pub fn merge_pool_update_body(
    current: &serde_json::Value,
    overrides: &PoolBodyArgs<'_>,
) -> serde_json::Value {
    let mut body = serde_json::json!({});

    let image = overrides
        .image
        .map(str::to_string)
        .or_else(|| {
            current
                .get("image")
                .and_then(|v| v.as_str())
                .map(str::to_string)
        });
    if let Some(image) = image {
        body["image"] = serde_json::Value::String(image);
    }

    let current_resources = current
        .get("resources")
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));
    let mut resources = serde_json::json!({
        "cpus": overrides
            .cpus
            .or_else(|| current_resources.get("cpus").and_then(|v| v.as_f64()))
            .unwrap_or(DEFAULT_POOL_CPUS),
        "memory_mb": overrides
            .memory_mb
            .or_else(|| current_resources.get("memory_mb").and_then(|v| v.as_i64()))
            .unwrap_or(DEFAULT_POOL_MEMORY_MB),
    });
    let disk = overrides
        .disk_mb
        .map(|v| v as i64)
        .or_else(|| {
            current_resources
                .get("ephemeral_disk_mb")
                .and_then(|v| v.as_i64())
        });
    if let Some(d) = disk {
        resources["ephemeral_disk_mb"] = serde_json::Value::Number(d.into());
    }
    body["resources"] = resources;

    let timeout = overrides.timeout.or_else(|| {
        current.get("timeout_secs").and_then(|v| v.as_i64())
    });
    if let Some(t) = timeout {
        body["timeout_secs"] = serde_json::Value::Number(t.into());
    }

    let entrypoint: Vec<String> = if !overrides.entrypoint.is_empty() {
        overrides.entrypoint.to_vec()
    } else {
        current
            .get("entrypoint")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default()
    };
    if !entrypoint.is_empty() {
        body["entrypoint"] = serde_json::json!(entrypoint);
    }

    let max_containers = overrides.max_containers.or_else(|| {
        current.get("max_containers").and_then(|v| v.as_i64())
    });
    if let Some(m) = max_containers {
        body["max_containers"] = serde_json::Value::Number(m.into());
    }

    let warm_containers = overrides.warm_containers.or_else(|| {
        current.get("warm_containers").and_then(|v| v.as_i64())
    });
    if let Some(w) = warm_containers {
        body["warm_containers"] = serde_json::Value::Number(w.into());
    }

    if let Some(secret_names) = current.get("secret_names").cloned() {
        body["secret_names"] = secret_names;
    }
    if let Some(true) = current
        .get("allow_unauthenticated_access")
        .and_then(|v| v.as_bool())
    {
        body["allow_unauthenticated_access"] = serde_json::Value::Bool(true);
    }
    if let Some(ports) = current.get("exposed_ports").cloned() {
        body["exposed_ports"] = ports;
    }
    if let Some(network) = current.get("network_policy").cloned() {
        body["network"] = network;
    }

    body
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_body_uses_defaults_when_unspecified() {
        let body = build_pool_create_body(&PoolBodyArgs {
            image: Some("alpine"),
            entrypoint: &[],
            ports: &[],
            network_allow: &[],
            network_deny: &[],
            ..Default::default()
        });
        assert_eq!(body["image"], "alpine");
        assert_eq!(body["resources"]["cpus"], 1.0);
        assert_eq!(body["resources"]["memory_mb"], 1024);
        assert_eq!(body["resources"]["ephemeral_disk_mb"], 1024);
        assert!(body.get("max_containers").is_none());
        assert!(body.get("network").is_none());
    }

    #[test]
    fn create_body_includes_proxy_and_network_when_set() {
        let body = build_pool_create_body(&PoolBodyArgs {
            image: Some("alpine"),
            entrypoint: &[],
            ports: &[8080],
            allow_unauthenticated_access: true,
            no_internet: true,
            network_allow: &["10.0.0.0/8".to_string()],
            network_deny: &[],
            ..Default::default()
        });
        assert_eq!(body["exposed_ports"], serde_json::json!([8080]));
        assert_eq!(body["allow_unauthenticated_access"], true);
        assert_eq!(body["network"]["allow_internet_access"], false);
        assert_eq!(body["network"]["allow_out"], serde_json::json!(["10.0.0.0/8"]));
    }

    #[test]
    fn merge_update_body_preserves_unspecified_fields() {
        let current = serde_json::json!({
            "image": "alpine",
            "resources": {"cpus": 0.5, "memory_mb": 256, "ephemeral_disk_mb": 512},
            "timeout_secs": 60,
            "entrypoint": ["bash"],
            "max_containers": 10,
            "warm_containers": 3,
            "allow_unauthenticated_access": true,
            "exposed_ports": [8080],
            "network_policy": {"allow_internet_access": false, "allow_out": [], "deny_out": []},
            "secret_names": ["A"],
        });
        let overrides = PoolBodyArgs {
            warm_containers: Some(5),
            entrypoint: &[],
            ports: &[],
            network_allow: &[],
            network_deny: &[],
            ..Default::default()
        };
        let body = merge_pool_update_body(&current, &overrides);
        assert_eq!(body["image"], "alpine");
        assert_eq!(body["resources"]["cpus"], 0.5);
        assert_eq!(body["resources"]["memory_mb"], 256);
        assert_eq!(body["resources"]["ephemeral_disk_mb"], 512);
        assert_eq!(body["timeout_secs"], 60);
        assert_eq!(body["entrypoint"], serde_json::json!(["bash"]));
        assert_eq!(body["max_containers"], 10);
        assert_eq!(body["warm_containers"], 5);
        assert_eq!(body["allow_unauthenticated_access"], true);
        assert_eq!(body["exposed_ports"], serde_json::json!([8080]));
        assert_eq!(body["network"]["allow_internet_access"], false);
        assert_eq!(body["secret_names"], serde_json::json!(["A"]));
    }

    #[test]
    fn create_body_setting_port_implicitly_allows_unauthenticated() {
        // apply_proxy_access_settings forces allow_unauthenticated_access=true
        // whenever any port is exposed, so ports work end-to-end without an
        // extra flag.
        let body = build_pool_create_body(&PoolBodyArgs {
            image: Some("alpine"),
            entrypoint: &[],
            ports: &[8080],
            allow_unauthenticated_access: false,
            network_allow: &[],
            network_deny: &[],
            ..Default::default()
        });
        assert_eq!(body["exposed_ports"], serde_json::json!([8080]));
        assert_eq!(body["allow_unauthenticated_access"], true);
    }

    #[test]
    fn merge_update_body_handles_pool_without_optional_fields() {
        // Minimal current state — pool was created without entrypoint,
        // network policy, ports, etc. Update should still succeed.
        let current = serde_json::json!({
            "image": "alpine",
            "resources": {"cpus": 1.0, "memory_mb": 1024, "ephemeral_disk_mb": 1024},
        });
        let overrides = PoolBodyArgs {
            warm_containers: Some(2),
            entrypoint: &[],
            ports: &[],
            network_allow: &[],
            network_deny: &[],
            ..Default::default()
        };
        let body = merge_pool_update_body(&current, &overrides);
        assert_eq!(body["image"], "alpine");
        assert_eq!(body["warm_containers"], 2);
        assert!(body.get("entrypoint").is_none());
        assert!(body.get("network").is_none());
        assert!(body.get("exposed_ports").is_none());
    }

    #[test]
    fn merge_update_body_renames_network_policy_to_network() {
        let current = serde_json::json!({
            "image": "alpine",
            "resources": {"cpus": 0.5, "memory_mb": 256},
            "network_policy": {"allow_internet_access": false, "allow_out": ["1.2.3.4/32"], "deny_out": []},
        });
        let overrides = PoolBodyArgs {
            entrypoint: &[],
            ports: &[],
            network_allow: &[],
            network_deny: &[],
            ..Default::default()
        };
        let body = merge_pool_update_body(&current, &overrides);
        assert_eq!(body["network"]["allow_out"], serde_json::json!(["1.2.3.4/32"]));
        assert!(body.get("network_policy").is_none());
    }
}
