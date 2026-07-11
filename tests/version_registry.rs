//! Application version registry: `launch` records this process's version in
//! `application_versions`; the engine can list them, read the latest, and
//! promote one to latest.

use durare::{DurableEngine, InMemoryProvider, Result};
use std::sync::Arc;
use std::time::Duration;

/// Launching registers the engine's version; a later launch of a higher version
/// becomes the latest; re-launching an existing version is idempotent.
#[tokio::test]
async fn launch_registers_version_and_tracks_latest() -> Result<()> {
    let provider = Arc::new(InMemoryProvider::new());

    // Nothing registered before any launch.
    let probe = DurableEngine::new(provider.clone()).await?;
    assert!(probe.get_latest_application_version().await?.is_none());
    assert!(probe.list_application_versions().await?.is_empty());

    // Launching engine A records version 1.0.0.
    let a = DurableEngine::new_with_version(provider.clone(), "1.0.0").await?;
    a.launch().await?;
    // A later launch of 2.0.0 becomes the latest (more recent timestamp).
    tokio::time::sleep(Duration::from_millis(5)).await;
    let b = DurableEngine::new_with_version(provider.clone(), "2.0.0").await?;
    b.launch().await?;
    a.shutdown(Duration::from_secs(1)).await?;
    b.shutdown(Duration::from_secs(1)).await?;

    let versions = probe.list_application_versions().await?;
    assert_eq!(versions.len(), 2, "both versions registered");
    assert_eq!(versions[0].version_name, "2.0.0", "newest first");
    assert_eq!(versions[1].version_name, "1.0.0");
    assert_eq!(
        probe
            .get_latest_application_version()
            .await?
            .unwrap()
            .version_name,
        "2.0.0"
    );

    // Re-launching an already-registered version adds no duplicate row.
    let a2 = DurableEngine::new_with_version(provider.clone(), "1.0.0").await?;
    a2.launch().await?;
    a2.shutdown(Duration::from_secs(1)).await?;
    assert_eq!(probe.list_application_versions().await?.len(), 2);
    Ok(())
}

/// `set_latest_application_version` promotes a registered version to the top;
/// an unknown version is a no-op and an empty name is rejected.
#[tokio::test]
async fn set_latest_promotes_a_version() -> Result<()> {
    let provider = Arc::new(InMemoryProvider::new());
    let a = DurableEngine::new_with_version(provider.clone(), "1.0.0").await?;
    a.launch().await?;
    tokio::time::sleep(Duration::from_millis(5)).await;
    let b = DurableEngine::new_with_version(provider.clone(), "2.0.0").await?;
    b.launch().await?;
    a.shutdown(Duration::from_secs(1)).await?;
    b.shutdown(Duration::from_secs(1)).await?;

    assert_eq!(
        a.get_latest_application_version()
            .await?
            .unwrap()
            .version_name,
        "2.0.0"
    );

    // Promote 1.0.0 — it now sorts to the top.
    tokio::time::sleep(Duration::from_millis(5)).await;
    assert!(a.set_latest_application_version("1.0.0").await?);
    assert_eq!(
        a.get_latest_application_version()
            .await?
            .unwrap()
            .version_name,
        "1.0.0"
    );
    assert_eq!(
        a.list_application_versions().await?[0].version_name,
        "1.0.0"
    );

    // Unknown version: no row matched. Empty name: rejected.
    assert!(!a.set_latest_application_version("9.9.9").await?);
    assert!(a.set_latest_application_version("").await.is_err());
    Ok(())
}
