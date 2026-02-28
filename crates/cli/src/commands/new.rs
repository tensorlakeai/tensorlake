use std::fs;
use std::path::Path;

use crate::error::{CliError, Result};
use crate::project::templates::{PYTHON_TEMPLATE, README_TEMPLATE};

/// Sanitize name to snake_case.
fn sanitize(name: &str) -> String {
    let mut result = name.replace(['-', ' '], "_");

    // Insert underscores before uppercase letters (camelCase/PascalCase)
    let mut snake = String::new();
    for (i, ch) in result.chars().enumerate() {
        if i > 0 && ch.is_uppercase() {
            snake.push('_');
        }
        snake.push(ch);
    }
    result = snake.to_lowercase();

    // Remove consecutive underscores
    while result.contains("__") {
        result = result.replace("__", "_");
    }
    result.trim_matches('_').to_string()
}

/// Validate that the name can be used as a Python identifier.
fn validate_app_name(name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(CliError::usage("application name cannot be empty"));
    }

    // Check for invalid characters
    let valid = name
        .chars()
        .all(|c| c.is_alphanumeric() || c == '_' || c == '-' || c == ' ');
    if !valid {
        return Err(CliError::usage(
            "application name can only contain letters, numbers, hyphens, underscores, and spaces",
        ));
    }

    let snake_name = sanitize(name);

    // Check if it's a valid Python identifier (starts with letter or underscore, rest alphanumeric/underscore)
    if snake_name.is_empty() {
        return Err(CliError::usage("application name resolves to empty string"));
    }
    let first = snake_name.chars().next().unwrap();
    if !first.is_alphabetic() && first != '_' {
        return Err(CliError::usage(format!(
            "'{}' is not a valid Python identifier. names must start with a letter or underscore.",
            snake_name
        )));
    }
    if !snake_name.chars().all(|c| c.is_alphanumeric() || c == '_') {
        return Err(CliError::usage(format!(
            "'{}' is not a valid Python identifier.",
            snake_name
        )));
    }

    // Check Python keywords
    const PYTHON_KEYWORDS: &[&str] = &[
        "False", "None", "True", "and", "as", "assert", "async", "await", "break", "class",
        "continue", "def", "del", "elif", "else", "except", "finally", "for", "from", "global",
        "if", "import", "in", "is", "lambda", "nonlocal", "not", "or", "pass", "raise", "return",
        "try", "while", "with", "yield",
    ];
    if PYTHON_KEYWORDS.contains(&snake_name.as_str()) {
        return Err(CliError::usage(format!(
            "'{}' is a Python keyword and cannot be used as an application name",
            snake_name
        )));
    }

    Ok(())
}

pub fn run(name: &str, force: bool) -> Result<()> {
    validate_app_name(name)?;

    let module_name = sanitize(name);
    let filename = format!("{}.py", module_name);
    let target_dir = Path::new(&module_name).canonicalize().unwrap_or_else(|_| {
        std::env::current_dir()
            .unwrap_or_default()
            .join(&module_name)
    });

    let python_file = target_dir.join(&filename);
    let readme_file = target_dir.join("README.md");

    if !force && python_file.exists() {
        return Err(CliError::usage(format!(
            "'{}' already exists. use --force to overwrite, or choose a different name.",
            filename
        )));
    }

    let python_content = PYTHON_TEMPLATE
        .replace("{function_name}", &module_name)
        .replace("{filename}", &filename);

    let readme_content = README_TEMPLATE
        .replace("{app_name}", name)
        .replace("{function_name}", &module_name)
        .replace("{filename}", &filename)
        .replace("{module_name}", &module_name);

    eprintln!(
        "\nCreating new Tensorlake application in '{}'...\n",
        module_name
    );

    if force && target_dir.exists() {
        fs::remove_dir_all(&target_dir)?;
    }
    fs::create_dir_all(&target_dir)?;

    fs::write(&python_file, &python_content)?;
    eprintln!("  + {}", filename);

    fs::write(&readme_file, &readme_content)?;
    eprintln!("  + README.md");

    eprintln!("\n==================================================");
    eprintln!("application created successfully!");
    eprintln!("==================================================");
    eprintln!("\nNext steps:");
    eprintln!("  Deploy: tensorlake deploy {}", filename);
    eprintln!("\nLearn more: https://docs.tensorlake.ai/quickstart");

    Ok(())
}
