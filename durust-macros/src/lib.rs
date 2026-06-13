//! Procedural macros for `durust`.
//!
//! The only macro is [`macro@workflow`], the Rust analog of Python's
//! `@DBOS.workflow()` decorator: it leaves your async fn untouched and emits a
//! compile-time registration so the engine discovers it automatically — no
//! manual `engine.register(...)` call, and the workflow name defaults to the
//! function name.

use proc_macro::TokenStream;
use quote::quote;
use syn::parse::{Parse, ParseStream};
use syn::{parse_macro_input, Ident, ItemFn, LitStr, Token};

/// Parsed `#[workflow(...)]` arguments. Supports a bare name literal
/// (`#[workflow("orders.process")]`) and/or keyed args
/// (`#[workflow(name = "...", schedule = "* * * * * *")]`).
struct WorkflowArgs {
    name: Option<String>,
    schedule: Option<String>,
}

impl Parse for WorkflowArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut name = None;
        let mut schedule = None;
        while !input.is_empty() {
            if input.peek(LitStr) {
                // Bare string literal: the registered name.
                name = Some(input.parse::<LitStr>()?.value());
            } else {
                let key: Ident = input.parse()?;
                input.parse::<Token![=]>()?;
                let val: LitStr = input.parse()?;
                match key.to_string().as_str() {
                    "name" => name = Some(val.value()),
                    "schedule" => schedule = Some(val.value()),
                    other => {
                        return Err(syn::Error::new(
                            key.span(),
                            format!("unknown `#[workflow]` argument `{other}` (expected `name` or `schedule`)"),
                        ))
                    }
                }
            }
            if input.peek(Token![,]) {
                input.parse::<Token![,]>()?;
            } else {
                break;
            }
        }
        Ok(WorkflowArgs { name, schedule })
    }
}

/// Register an `async fn(DurableContext, Input) -> Result<Output>` as a durable
/// workflow.
///
/// ```ignore
/// #[durust::workflow]
/// async fn process_order(ctx: DurableContext, order: Order) -> Result<Receipt> { ... }
///
/// // Override the registered name:
/// #[durust::workflow("orders.process")]
/// async fn process_order(ctx: DurableContext, order: Order) -> Result<Receipt> { ... }
///
/// // Run on a cron schedule (6-field cron, second precision). The workflow
/// // receives the scheduled tick time (RFC 3339) as its input:
/// #[durust::workflow(schedule = "0 0 * * * *")] // top of every hour
/// async fn hourly(ctx: DurableContext, scheduled_at: String) -> Result<()> { ... }
/// ```
///
/// The function is left as-is; the macro additionally submits an `inventory`
/// registration. `DurableEngine::new` collects every such registration in the
/// binary, so annotated workflows need no manual `register` call; scheduled
/// ones additionally start firing once [`DurableEngine::launch`] is called.
#[proc_macro_attribute]
pub fn workflow(attr: TokenStream, item: TokenStream) -> TokenStream {
    let func = parse_macro_input!(item as ItemFn);
    let args = parse_macro_input!(attr as WorkflowArgs);

    // Name defaults to the function's identifier.
    let name = args.name.unwrap_or_else(|| func.sig.ident.to_string());
    let schedule = match args.schedule {
        Some(s) => quote! { Some(#s) },
        None => quote! { None },
    };

    let ident = &func.sig.ident;

    let expanded = quote! {
        #func

        durust::inventory::submit! {
            durust::WorkflowRegistration {
                name: #name,
                // A non-capturing closure coerces to `fn() -> WorkflowFn`.
                // `erase` infers the Input/Output types from the fn signature.
                builder: || durust::erase(#ident),
                schedule: #schedule,
            }
        }
    };

    expanded.into()
}
