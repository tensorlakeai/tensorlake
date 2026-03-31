use crate::auth::context::CliContext;
use crate::commands::sbx::sandbox_endpoint;
use crate::error::{CliError, Result};

pub async fn run(ctx: &CliContext, sandbox_id: &str, new_name: &str) -> Result<()> {
    let client = ctx.client()?;
    let url = sandbox_endpoint(ctx, &format!("sandboxes/{sandbox_id}"));

    let body = serde_json::json!({"name": new_name});
    let resp = client
        .patch(&url)
        .json(&body)
        .send()
        .await
        .map_err(CliError::Http)?;

    if !resp.status().is_success() {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        return Err(CliError::Other(anyhow::anyhow!(
            "failed to update sandbox name (HTTP {}): {}",
            status,
            text
        )));
    }

    let is_tty = std::io::IsTerminal::is_terminal(&std::io::stdout());
    if is_tty {
        eprintln!("Sandbox {} is now named {}.", sandbox_id, new_name);
        print_post_name_tip(new_name);
    } else {
        println!("{new_name}");
    }
    Ok(())
}

fn print_post_name_tip(name: &str) {
    eprintln!();
    eprintln!("Since your sandbox has a name, you can access it with commands like:");
    eprintln!("  tl sbx ssh {name}");
    eprintln!("  tl sbx exec {name} -- bash -c \"echo Hello, World!\"");
    eprintln!("  tl sbx cp ./myfile.py {name}:/tmp/myfile.py");
    eprintln!("  tl sbx suspend {name}");
    eprintln!();
}
