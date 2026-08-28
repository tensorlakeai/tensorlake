use std::fs;
use std::path::Path;

use clap::ValueEnum;

use crate::error::{CliError, Result};
use crate::project::templates::{
    PYTHON_TEMPLATE, README_TEMPLATE, TYPESCRIPT_CONFIG_TEMPLATE, TYPESCRIPT_PACKAGE_TEMPLATE,
    TYPESCRIPT_README_TEMPLATE, TYPESCRIPT_TEMPLATE,
};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, ValueEnum)]
pub enum Language {
    #[default]
    #[value(alias = "py")]
    Python,
    #[value(alias = "ts")]
    Typescript,
}

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

/// Validate and normalize the stable application name used by both templates.
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

pub fn run(name: &str, force: bool, language: Language) -> Result<()> {
    validate_app_name(name)?;

    let module_name = sanitize(name);
    let files = scaffold_files(name, &module_name, language);
    let application_filename = files[0].0.clone();
    let target_dir = Path::new(&module_name).canonicalize().unwrap_or_else(|_| {
        std::env::current_dir()
            .unwrap_or_default()
            .join(&module_name)
    });

    if !force
        && let Some((filename, _)) = files
            .iter()
            .find(|(filename, _)| target_dir.join(filename).exists())
    {
        return Err(CliError::usage(format!(
            "'{}' already exists. use --force to overwrite, or choose a different name.",
            filename
        )));
    }

    eprintln!(
        "\nCreating new Tensorlake application in '{}'...\n",
        module_name
    );

    if force && target_dir.exists() {
        fs::remove_dir_all(&target_dir)?;
    }
    fs::create_dir_all(&target_dir)?;

    for (filename, content) in files {
        fs::write(target_dir.join(&filename), content)?;
        eprintln!("  + {}", filename);
    }

    eprintln!("\n==================================================");
    eprintln!("application created successfully!");
    eprintln!("==================================================");
    eprintln!("\nNext steps:");
    eprintln!("  cd {}", module_name);
    if language == Language::Typescript {
        eprintln!("  npm install");
    }
    eprintln!("  Deploy: tl app deploy {}", application_filename);
    eprintln!("\nLearn more: https://docs.tensorlake.ai/applications/introduction");

    Ok(())
}

fn scaffold_files(name: &str, module_name: &str, language: Language) -> Vec<(String, String)> {
    match language {
        Language::Python => {
            let filename = format!("{module_name}.py");
            vec![
                (
                    filename.clone(),
                    PYTHON_TEMPLATE
                        .replace("{function_name}", module_name)
                        .replace("{filename}", &filename),
                ),
                (
                    "README.md".to_string(),
                    README_TEMPLATE
                        .replace("{app_name}", name)
                        .replace("{function_name}", module_name)
                        .replace("{filename}", &filename)
                        .replace("{module_name}", module_name),
                ),
            ]
        }
        Language::Typescript => vec![
            (
                "application.ts".to_string(),
                TYPESCRIPT_TEMPLATE.replace("{function_name}", module_name),
            ),
            (
                "package.json".to_string(),
                TYPESCRIPT_PACKAGE_TEMPLATE
                    .replace("{package_name}", &npm_package_name(module_name))
                    .replace("{sdk_version}", env!("CARGO_PKG_VERSION")),
            ),
            (
                "tsconfig.json".to_string(),
                TYPESCRIPT_CONFIG_TEMPLATE.to_string(),
            ),
            (
                "README.md".to_string(),
                TYPESCRIPT_README_TEMPLATE
                    .replace("{app_name}", name)
                    .replace("{function_name}", module_name),
            ),
        ],
    }
}

fn npm_package_name(module_name: &str) -> String {
    let mut stem = String::with_capacity(module_name.len());
    for character in module_name.chars() {
        if character.is_ascii_alphanumeric() {
            stem.push(character);
        } else if !stem.ends_with('-') {
            stem.push('-');
        }
    }
    let stem = stem.trim_matches('-');
    format!("tensorlake-{}", if stem.is_empty() { "app" } else { stem })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn python_scaffold_remains_the_default_shape() {
        let files = scaffold_files("Hello World", "hello_world", Language::Python);
        assert_eq!(
            files
                .iter()
                .map(|(name, _)| name.as_str())
                .collect::<Vec<_>>(),
            ["hello_world.py", "README.md"]
        );
        assert!(files[0].1.contains("def hello_world(name: str)"));
    }

    #[test]
    fn typescript_scaffold_is_installable_and_deployable() {
        let files = scaffold_files("Hello World", "hello_world", Language::Typescript);
        assert_eq!(
            files
                .iter()
                .map(|(name, _)| name.as_str())
                .collect::<Vec<_>>(),
            [
                "application.ts",
                "package.json",
                "tsconfig.json",
                "README.md"
            ]
        );
        assert!(files[0].1.contains("registerApplication"));
        assert!(files[0].1.contains("\"hello_world\""));
        assert!(files[1].1.contains("\"tensorlake\""));
        assert!(files[1].1.contains("\"name\": \"tensorlake-hello-world\""));
        assert!(
            !files
                .iter()
                .any(|(_, content)| content.contains("{sdk_version}"))
        );
    }

    #[test]
    fn npm_package_name_is_valid_for_python_compatible_unicode_names() {
        assert_eq!(npm_package_name("_déjà_vu"), "tensorlake-d-j-vu");
    }
}
