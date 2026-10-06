//! Live resource resize: admission is distinct from VMM-confirmed completion.
use super::{
    SandboxesClient,
    models::{ResizeSandboxResources, SandboxInfo, UpdateSandboxRequest},
};
use crate::{Traced, error::SdkError, retry::is_transient};
use reqwest::Method;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::time::Instant;

#[derive(Clone, Copy, Debug)]
pub struct ResizeOptions {
    pub wait: bool,
    pub timeout: Duration,
    pub poll_interval: Duration,
}
impl Default for ResizeOptions {
    fn default() -> Self {
        Self {
            wait: true,
            timeout: Duration::from_secs(300),
            poll_interval: Duration::from_secs(1),
        }
    }
}

/// Update observation and the generation admitted by this call, if any.
/// A no-op retains prior resize metadata in `info`, but admits no generation.
#[derive(Debug, Clone)]
pub struct SandboxUpdateResult {
    pub info: Traced<SandboxInfo>,
    pub resize_generation: Option<u64>,
}

/// Carries the last observation so failure/timeout never hides actual allocation.
#[derive(Debug, Clone, Serialize, Deserialize, thiserror::Error)]
#[error("Sandbox {sandbox_id} resize generation {generation}: {message}; last confirmed allocation: {allocation}", allocation = allocation_text(info))]
pub struct SandboxResizeError {
    pub sandbox_id: String,
    pub generation: u64,
    /// failed, timeout, superseded, interrupted, or incompatible_response.
    pub reason: String,
    pub message: String,
    pub info: Option<Box<SandboxInfo>>,
}
fn allocation_text(info: &Option<Box<SandboxInfo>>) -> String {
    info.as_ref()
        .map(|i| {
            format!(
                "{} CPUs, {} MiB memory, {} MiB disk",
                i.resources.cpus, i.resources.memory_mb, i.resources.disk_mb
            )
        })
        .unwrap_or_else(|| "unavailable".into())
}
fn failure(
    id: &str,
    generation: u64,
    reason: &str,
    message: impl Into<String>,
    info: Option<&SandboxInfo>,
) -> SdkError {
    Box::new(SandboxResizeError {
        sandbox_id: id.into(),
        generation,
        reason: reason.into(),
        message: message.into(),
        info: info.cloned().map(Box::new),
    })
    .into()
}
impl ResizeSandboxResources {
    pub fn is_empty(&self) -> bool {
        self.cpus.is_none() && self.memory_mb.is_none() && self.disk_mb.is_none()
    }
    pub fn validate(&self) -> Result<(), SdkError> {
        if self.is_empty() {
            return Err(SdkError::ClientError(
                "resources must set at least one of cpus, memory_mb, or disk_mb".into(),
            ));
        }
        if let Some(cpus) = self.cpus
            && (!cpus.is_finite() || cpus <= 0.0 || cpus.fract() != 0.0)
        {
            return Err(SdkError::ClientError(format!(
                "resources.cpus {cpus} must be a finite positive whole-vCPU count"
            )));
        }
        if let Some(memory) = self.memory_mb
            && (memory <= 0)
        {
            return Err(SdkError::ClientError(format!(
                "resources.memory_mb {memory} must be a positive integer number of MiB"
            )));
        }
        if self.disk_mb == Some(0) {
            return Err(SdkError::ClientError(
                "resources.disk_mb 0 must be a positive integer number of MiB".into(),
            ));
        }
        Ok(())
    }
    /// Validate observable live constraints and omit unchanged dimensions.
    pub fn against(&self, current: &SandboxInfo) -> Result<Self, SdkError> {
        self.validate()?;
        if current.status != "running" {
            return Err(SdkError::ClientError(format!(
                "resource resize requires a Running sandbox; current status is {}",
                current.status
            )));
        }
        if let Some(runtime) = &current.runtime
            && (matches!(runtime.as_str(), "firecracker" | "gvisor"))
        {
            return Err(SdkError::ClientError(format!(
                "resource resize requires Cloud Hypervisor; current runtime is {runtime}"
            )));
        }
        let mut target = self.clone();
        if let Some(disk) = target.disk_mb
            && (current.resources.disk_mb >= 0 && disk < current.resources.disk_mb as u64)
        {
            return Err(SdkError::ClientError(format!(
                "resources.disk_mb {disk} is below current {} MiB; live disk resize can only grow",
                current.resources.disk_mb
            )));
        }
        if target.cpus == Some(current.resources.cpus) {
            target.cpus = None;
        }
        if target.memory_mb == Some(current.resources.memory_mb) {
            target.memory_mb = None;
        }
        if current.resources.disk_mb >= 0
            && target.disk_mb == Some(current.resources.disk_mb as u64)
        {
            target.disk_mb = None;
        }
        Ok(target)
    }
}
impl SandboxesClient {
    /// Resize waits by default; `wait: false` returns the admitted generation.
    /// Network/proxy updates retain their existing behavior.
    pub async fn update_with_options(
        &self,
        sandbox_id: &str,
        request: &UpdateSandboxRequest,
        options: ResizeOptions,
    ) -> Result<Traced<SandboxInfo>, SdkError> {
        Ok(self
            .update_with_result(sandbox_id, request, options)
            .await?
            .info)
    }

    /// Like `update_with_options`, with an explicit admission outcome so callers
    /// can distinguish a no-op from a new resize without another preflight GET.
    pub async fn update_with_result(
        &self,
        sandbox_id: &str,
        request: &UpdateSandboxRequest,
        options: ResizeOptions,
    ) -> Result<SandboxUpdateResult, SdkError> {
        let mut request = request.clone();
        if let Some(resources) = &request.resources {
            if request.name.is_some()
                || request.allow_unauthenticated_access.is_some()
                || request.exposed_ports.is_some()
                || !request.network.is_keep()
            {
                return Err(SdkError::ClientError(
                    "resources cannot be combined with other sandbox update fields".into(),
                ));
            }
            resources.validate()?;
            if options.poll_interval.is_zero() {
                return Err(SdkError::ClientError(
                    "poll_interval must be positive".into(),
                ));
            }
            let current = self.get(sandbox_id).await?;
            let resources = resources.against(&current)?;
            if resources.is_empty() {
                return Ok(SandboxUpdateResult {
                    info: current,
                    resize_generation: None,
                });
            }
            request.resources = Some(resources);
        }
        let uri = self.endpoint(&format!("sandboxes/{sandbox_id}"));
        let req = self
            .client
            .build_post_json_request(Method::PATCH, &uri, &request)?;
        // Never replay admission when a later poll fails.
        let admitted: Traced<SandboxInfo> = self.client.execute_json(req).await?;
        if request.resources.is_none() {
            return Ok(SandboxUpdateResult {
                info: admitted,
                resize_generation: None,
            });
        }
        let generation = admitted
            .resource_resize
            .as_ref()
            .ok_or_else(|| {
                failure(
                    sandbox_id,
                    0,
                    "incompatible_response",
                    "server omitted resource_resize after admission; completion is unknown",
                    Some(&admitted),
                )
            })?
            .generation;
        if resize_complete(sandbox_id, generation, &admitted)? || !options.wait {
            return Ok(SandboxUpdateResult {
                info: admitted,
                resize_generation: Some(generation),
            });
        }
        let info = self
            .poll_resource_resize(
                &admitted.sandbox_id,
                generation,
                options.timeout,
                options.poll_interval,
                Some(admitted.clone()),
            )
            .await?;
        Ok(SandboxUpdateResult {
            info,
            resize_generation: Some(generation),
        })
    }

    /// Wait for this exact generation. Timeout does not cancel the resize.
    /// A zero timeout checks once without polling again.
    pub async fn wait_for_resource_resize(
        &self,
        sandbox_id: &str,
        generation: u64,
        timeout: Duration,
        poll_interval: Duration,
    ) -> Result<Traced<SandboxInfo>, SdkError> {
        if generation == 0 {
            return Err(SdkError::ClientError("generation must be positive".into()));
        }
        self.poll_resource_resize(sandbox_id, generation, timeout, poll_interval, None)
            .await
    }
    async fn poll_resource_resize(
        &self,
        sandbox_id: &str,
        generation: u64,
        timeout: Duration,
        poll_interval: Duration,
        mut last: Option<Traced<SandboxInfo>>,
    ) -> Result<Traced<SandboxInfo>, SdkError> {
        if poll_interval.is_zero() {
            return Err(SdkError::ClientError(
                "poll_interval must be positive".into(),
            ));
        }
        let deadline = Instant::now().checked_add(timeout).ok_or_else(|| {
            SdkError::ClientError("timeout is outside the supported duration range".into())
        })?;
        let mut check_once = timeout.is_zero();
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            let first_check = std::mem::take(&mut check_once);
            if remaining.is_zero() && !first_check {
                return Err(failure(
                    sandbox_id,
                    generation,
                    "timeout",
                    "wait timed out; resize is not cancelled; inspect or wait for this generation again",
                    last.as_deref(),
                ));
            }
            let observation = if first_check {
                self.get(sandbox_id).await
            } else {
                self.get_within(sandbox_id, remaining).await
            };
            match observation {
                Ok(info) => {
                    if resize_complete(sandbox_id, generation, &info)? {
                        return Ok(info);
                    }
                    last = Some(info);
                }
                Err(error) if is_transient(&error) || error.transport_failure().is_some() => {}
                Err(error) => return Err(error),
            }
            tokio::time::sleep(
                poll_interval.min(deadline.saturating_duration_since(Instant::now())),
            )
            .await;
        }
    }
}
fn resize_complete(id: &str, generation: u64, info: &SandboxInfo) -> Result<bool, SdkError> {
    if let Some(resize) = &info.resource_resize {
        if resize.generation > generation {
            return Err(failure(
                id,
                generation,
                "superseded",
                format!(
                    "generation {} superseded this resize; its result is no longer available",
                    resize.generation
                ),
                Some(info),
            ));
        }
        if resize.generation == generation {
            match resize.status.as_str() {
                "succeeded" => return Ok(true),
                "failed" => {
                    return Err(failure(
                        id,
                        generation,
                        "failed",
                        resize
                            .error_message
                            .as_deref()
                            .unwrap_or("resize failed without a server diagnostic"),
                        Some(info),
                    ));
                }
                "pending" => {}
                status => {
                    return Err(failure(
                        id,
                        generation,
                        "incompatible_response",
                        format!("unknown resize status {status}"),
                        Some(info),
                    ));
                }
            }
        }
    }
    if info.status != "running" {
        return Err(failure(
            id,
            generation,
            "interrupted",
            format!("sandbox is {}; resize completion is unknown", info.status),
            Some(info),
        ));
    }
    Ok(false)
}
