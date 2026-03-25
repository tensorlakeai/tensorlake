use std::collections::HashMap;

use rustpython_parser::{Parse, ast};

/// Default base image when `base_image` is not specified in `Image(...)`.
pub const DEFAULT_BASE_IMAGE: &str = "python:3.12-slim-bookworm";
const DEFAULT_IMAGE_NAME: &str = "default";
const DEFAULT_IMAGE_TAG: &str = "latest";

/// One build operation extracted from an Image method chain.
#[derive(Debug, Clone, PartialEq)]
pub struct OpDef {
    pub op_type: String,
    pub args: Vec<String>,
    pub options: HashMap<String, String>,
}

/// An Image definition extracted from a Python source file.
#[derive(Debug, Clone, PartialEq)]
pub struct ImageDef {
    pub name: String,
    pub tag: String,
    pub base_image: String,
    pub operations: Vec<OpDef>,
}

/// Extract all `Image(...)` definitions from the top-level statements of `source`.
///
/// Returns an empty `Vec` on parse failure rather than propagating the error,
/// because the caller (import resolver) may encounter non-application Python files.
pub fn extract_images_from_source(source: &str) -> Vec<ImageDef> {
    let stmts = match ast::Suite::parse(source, "<unknown>") {
        Ok(s) => s,
        Err(_) => return Vec::new(),
    };

    let mut images = Vec::new();
    for stmt in &stmts {
        collect_from_stmt(stmt, &mut images);
    }
    images
}

fn collect_from_stmt(stmt: &ast::Stmt, images: &mut Vec<ImageDef>) {
    match stmt {
        ast::Stmt::Assign(a) => {
            if let Some(img) = try_extract_image(a.value.as_ref()) {
                images.push(img);
            }
        }
        ast::Stmt::AnnAssign(a) => {
            if let Some(ref val) = a.value
                && let Some(img) = try_extract_image(val.as_ref())
            {
                images.push(img);
            }
        }
        _ => {}
    }
}

/// Try to parse `expr` as an `Image(...)` call or method chain ending at `Image(...)`.
///
/// Method chains like `Image(...).run(...).env(...)` are unwound from the outside in.
fn try_extract_image(expr: &ast::Expr) -> Option<ImageDef> {
    // Collect method calls in reverse (outermost → innermost).
    let mut ops_reversed: Vec<OpDef> = Vec::new();
    let mut cur = expr;

    loop {
        match cur {
            ast::Expr::Call(call) => {
                match call.func.as_ref() {
                    ast::Expr::Name(name) if name.id.as_str() == "Image" => {
                        // Reached the Image() constructor.
                        let (img_name, img_tag, img_base) =
                            extract_constructor_args(&call.args, &call.keywords);
                        ops_reversed.reverse();
                        return Some(ImageDef {
                            name: img_name,
                            tag: img_tag,
                            base_image: img_base,
                            operations: ops_reversed,
                        });
                    }
                    ast::Expr::Attribute(attr) => {
                        let method = attr.attr.as_str();
                        if let Some(op) = extract_method_op(method, &call.args, &call.keywords) {
                            ops_reversed.push(op);
                        } else if !matches!(method, "run" | "copy" | "add" | "env") {
                            // Unknown method — this isn't an Image chain.
                            return None;
                        }
                        // Continue unwinding the chain.
                        cur = attr.value.as_ref();
                    }
                    _ => return None,
                }
            }
            _ => return None,
        }
    }
}

fn extract_constructor_args(
    args: &[ast::Expr],
    keywords: &[ast::Keyword],
) -> (String, String, String) {
    let mut name = DEFAULT_IMAGE_NAME.to_string();
    let mut tag = DEFAULT_IMAGE_TAG.to_string();
    let mut base_image = DEFAULT_BASE_IMAGE.to_string();

    // Positional arguments: Image(name, tag, base_image)
    if let Some(s) = args.first().and_then(expr_str) {
        name = s;
    }
    if let Some(s) = args.get(1).and_then(expr_str) {
        tag = s;
    }
    if let Some(s) = args.get(2).and_then(expr_str) {
        base_image = s;
    }

    // Keyword arguments override positional.
    for kw in keywords {
        let Some(ref kw_name) = kw.arg else {
            continue;
        };
        let Some(val) = expr_str(&kw.value) else {
            continue;
        };
        match kw_name.as_str() {
            "name" => name = val,
            "tag" => tag = val,
            "base_image" => base_image = val,
            _ => {}
        }
    }

    (name, tag, base_image)
}

fn extract_method_op(method: &str, args: &[ast::Expr], keywords: &[ast::Keyword]) -> Option<OpDef> {
    match method {
        "run" => {
            // run(commands: str | List[str], options: Dict | None = None)
            let cmd_expr = args.first().or_else(|| find_kwarg(keywords, "commands"))?;
            let run_args = match cmd_expr {
                ast::Expr::List(list) => list.elts.iter().filter_map(expr_str).collect(),
                _ => {
                    let s = expr_str(cmd_expr)?;
                    vec![s]
                }
            };
            let options = extract_options(keywords);
            Some(OpDef {
                op_type: "RUN".to_string(),
                args: run_args,
                options,
            })
        }
        "copy" => {
            let src = positional_or_kwarg_str(args, keywords, 0, "src")?;
            let dest = positional_or_kwarg_str(args, keywords, 1, "dest")?;
            Some(OpDef {
                op_type: "COPY".to_string(),
                args: vec![src, dest],
                options: extract_options(keywords),
            })
        }
        "add" => {
            let src = positional_or_kwarg_str(args, keywords, 0, "src")?;
            let dest = positional_or_kwarg_str(args, keywords, 1, "dest")?;
            Some(OpDef {
                op_type: "ADD".to_string(),
                args: vec![src, dest],
                options: extract_options(keywords),
            })
        }
        "env" => {
            let key = positional_or_kwarg_str(args, keywords, 0, "key")?;
            let val = positional_or_kwarg_str(args, keywords, 1, "value")?;
            Some(OpDef {
                op_type: "ENV".to_string(),
                args: vec![key, val],
                options: HashMap::new(),
            })
        }
        _ => None,
    }
}

fn positional_or_kwarg_str(
    args: &[ast::Expr],
    keywords: &[ast::Keyword],
    pos: usize,
    kwarg: &str,
) -> Option<String> {
    args.get(pos)
        .and_then(expr_str)
        .or_else(|| find_kwarg(keywords, kwarg).and_then(expr_str))
}

fn find_kwarg<'a>(keywords: &'a [ast::Keyword], name: &str) -> Option<&'a ast::Expr> {
    keywords.iter().find_map(|kw| {
        let kw_name = kw.arg.as_ref()?;
        if kw_name.as_str() == name {
            Some(&kw.value)
        } else {
            None
        }
    })
}

fn extract_options(keywords: &[ast::Keyword]) -> HashMap<String, String> {
    let mut options = HashMap::new();
    for kw in keywords {
        let Some(ref kw_name) = kw.arg else {
            continue;
        };
        if kw_name.as_str() != "options" {
            continue;
        }
        if let ast::Expr::Dict(dict) = &kw.value {
            for (key_opt, val_expr) in dict.keys.iter().zip(dict.values.iter()) {
                if let Some(key_expr) = key_opt
                    && let (Some(k), Some(v)) = (expr_str(key_expr), expr_str(val_expr))
                {
                    options.insert(k, v);
                }
            }
        }
    }
    options
}

/// Extract a string value from a `Constant(Str(...))` expression.
fn expr_str(expr: &ast::Expr) -> Option<String> {
    if let ast::Expr::Constant(c) = expr
        && let ast::Constant::Str(s) = &c.value
    {
        return Some(s.clone());
    }
    None
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn extract(src: &str) -> Vec<ImageDef> {
        extract_images_from_source(src)
    }

    fn single(src: &str) -> ImageDef {
        let mut v = extract(src);
        assert_eq!(v.len(), 1, "expected exactly 1 image, got {}", v.len());
        v.remove(0)
    }

    // ── Constructor arg parsing ───────────────────────────────────────────────

    #[test]
    fn test_defaults_when_no_args() {
        let img = single("IMG = Image()");
        assert_eq!(img.name, "default");
        assert_eq!(img.tag, "latest");
        assert_eq!(img.base_image, DEFAULT_BASE_IMAGE);
        assert!(img.operations.is_empty());
    }

    #[test]
    fn test_positional_args() {
        let img = single(r#"IMG = Image("myapp", "v1", "ubuntu:22.04")"#);
        assert_eq!(img.name, "myapp");
        assert_eq!(img.tag, "v1");
        assert_eq!(img.base_image, "ubuntu:22.04");
    }

    #[test]
    fn test_keyword_args() {
        let img = single(r#"IMG = Image(name="svc", tag="prod", base_image="python:3.11")"#);
        assert_eq!(img.name, "svc");
        assert_eq!(img.tag, "prod");
        assert_eq!(img.base_image, "python:3.11");
    }

    #[test]
    fn test_partial_kwargs_use_defaults() {
        let img = single(r#"IMG = Image(name="partial")"#);
        assert_eq!(img.name, "partial");
        assert_eq!(img.tag, DEFAULT_IMAGE_TAG);
        assert_eq!(img.base_image, DEFAULT_BASE_IMAGE);
    }

    #[test]
    fn test_kwargs_override_positional() {
        // name positional, base_image kwarg
        let img = single(r#"IMG = Image("positional", base_image="python:3.12")"#);
        assert_eq!(img.name, "positional");
        assert_eq!(img.base_image, "python:3.12");
    }

    // ── Method chain extraction ───────────────────────────────────────────────

    #[test]
    fn test_run_string() {
        let img = single(r#"IMG = Image(name="x").run("pip install foo")"#);
        assert_eq!(
            img.operations,
            vec![OpDef {
                op_type: "RUN".to_string(),
                args: vec!["pip install foo".to_string()],
                options: HashMap::new(),
            }]
        );
    }

    #[test]
    fn test_run_list() {
        let img = single(r#"IMG = Image(name="x").run(["cmd1", "cmd2"])"#);
        assert_eq!(img.operations[0].args, vec!["cmd1", "cmd2"]);
    }

    #[test]
    fn test_env_op() {
        let img = single(r#"IMG = Image().env("MY_KEY", "my_value")"#);
        assert_eq!(
            img.operations,
            vec![OpDef {
                op_type: "ENV".to_string(),
                args: vec!["MY_KEY".to_string(), "my_value".to_string()],
                options: HashMap::new(),
            }]
        );
    }

    #[test]
    fn test_copy_op() {
        let img = single(r#"IMG = Image().copy("src/", "/app/src")"#);
        assert_eq!(img.operations[0].op_type, "COPY");
        assert_eq!(img.operations[0].args, vec!["src/", "/app/src"]);
    }

    #[test]
    fn test_add_op() {
        let img = single(r#"IMG = Image().add("data.tar.gz", "/data/")"#);
        assert_eq!(img.operations[0].op_type, "ADD");
    }

    #[test]
    fn test_run_with_options() {
        let img = single(r#"IMG = Image().run("make", options={"from": "builder"})"#);
        let op = &img.operations[0];
        assert_eq!(op.op_type, "RUN");
        assert_eq!(op.options.get("from").map(String::as_str), Some("builder"));
    }

    #[test]
    fn test_copy_with_options() {
        let img = single(r#"IMG = Image().copy("src", "dest", options={"chown": "1000:1000"})"#);
        let op = &img.operations[0];
        assert_eq!(
            op.options.get("chown").map(String::as_str),
            Some("1000:1000")
        );
    }

    #[test]
    fn test_long_method_chain() {
        let src = r#"
IMG = (
    Image(name="chain")
    .run("pip install a")
    .env("FOO", "bar")
    .copy("src", "/app")
    .run("echo done")
)
"#;
        let img = single(src);
        assert_eq!(img.name, "chain");
        assert_eq!(img.operations.len(), 4);
        assert_eq!(img.operations[0].op_type, "RUN");
        assert_eq!(img.operations[1].op_type, "ENV");
        assert_eq!(img.operations[2].op_type, "COPY");
        assert_eq!(img.operations[3].op_type, "RUN");
    }

    // ── Multiple images ───────────────────────────────────────────────────────

    #[test]
    fn test_multiple_images_in_file() {
        let src = r#"
A = Image(name="alpha")
B = Image(name="beta")
"#;
        let imgs = extract(src);
        assert_eq!(imgs.len(), 2);
        let names: Vec<&str> = imgs.iter().map(|i| i.name.as_str()).collect();
        assert!(names.contains(&"alpha"));
        assert!(names.contains(&"beta"));
    }

    // ── Edge cases ────────────────────────────────────────────────────────────

    #[test]
    fn test_annotated_assignment() {
        let img = single(r#"IMG: object = Image(name="annotated")"#);
        assert_eq!(img.name, "annotated");
    }

    #[test]
    fn test_non_image_calls_are_skipped() {
        let imgs = extract(
            r#"
x = SomeOtherClass(name="foo")
y = Image(name="real")
"#,
        );
        assert_eq!(imgs.len(), 1);
        assert_eq!(imgs[0].name, "real");
    }

    #[test]
    fn test_syntax_error_returns_empty() {
        let imgs = extract("this is not valid python @@@ !!!");
        assert!(imgs.is_empty());
    }

    #[test]
    fn test_empty_source() {
        let imgs = extract("");
        assert!(imgs.is_empty());
    }

    #[test]
    fn test_run_no_args_does_not_crash() {
        // Unusual but should not panic.
        let imgs = extract(r#"IMG = Image().run()"#);
        // .run() with no args → skipped, not panicked; img extracted without ops
        assert_eq!(imgs.len(), 1);
        assert!(imgs[0].operations.is_empty());
    }

    #[test]
    fn test_image_keyword_only_run() {
        let img = single(r#"IMG = Image().run(commands="apt-get update")"#);
        assert_eq!(img.operations[0].args, vec!["apt-get update"]);
    }

    #[test]
    fn test_image_at_module_scope_standalone() {
        // Module-level Image assignment (SANDBOX_IMAGE pattern).
        let img =
            single(r#"SANDBOX_IMAGE = Image(name="data-tools", base_image="python:3.11-slim")"#);
        assert_eq!(img.name, "data-tools");
        assert_eq!(img.base_image, "python:3.11-slim");
    }
}
