//! Procedural macros for `durust`.
//!
//! - [`macro@workflow`] leaves your async fn untouched and, alongside it, emits
//!   a compile-time registration (so the engine auto-discovers the workflow —
//!   no manual `engine.register(...)`) plus a typed `UpperCamelCase` marker
//!   implementing `durust::WorkflowDef`, so the workflow can be started by a
//!   type-checked reference rather than a string.
//! - [`macro@step`] wraps an async fn's body in a durable
//!   `ctx.step(...)` checkpoint, so a step reads like an ordinary `async fn`
//!   call — no closure, no `Box::pin`, no `Ok::<_, Error>` annotation.

use heck::ToUpperCamelCase;
use proc_macro::TokenStream;
use quote::quote;
use syn::parse::{Parse, ParseStream};
use syn::{parse_macro_input, FnArg, Ident, ItemFn, LitStr, ReturnType, Token};

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
/// The function is left as-is. The macro additionally emits:
/// - an `inventory` registration — `DurableEngine::new`/`builder` collect every
///   one in the binary, so annotated workflows need no manual `register` call;
///   scheduled ones start firing once [`DurableEngine::launch`] is called;
/// - a typed marker — an `UpperCamelCase` zero-sized struct named after the
///   function (`process_order` → `ProcessOrder`) implementing
///   `durust::WorkflowDef`, so `engine.start_with(ProcessOrder, order, opts)`
///   is checked on input and output without a turbofish.
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
    let vis = &func.vis;

    // Input type: the second parameter, after `DurableContext`.
    let input_ty = match func.sig.inputs.iter().nth(1) {
        Some(FnArg::Typed(pt)) => (*pt.ty).clone(),
        _ => {
            return syn::Error::new_spanned(
                &func.sig,
                "a `#[workflow]` fn must take `(DurableContext, Input)`",
            )
            .to_compile_error()
            .into()
        }
    };
    // Return type: `Result<Output>`. The macro does not parse `Output` out of
    // the tokens — it projects `<ReturnType as WorkflowResult>::Ok` below and
    // lets the compiler extract it (through any `Result` alias).
    let return_ty = match &func.sig.output {
        ReturnType::Type(_, ty) => &**ty,
        ReturnType::Default => {
            return syn::Error::new_spanned(
                &func.sig,
                "a `#[workflow]` fn must return `Result<Output>`",
            )
            .to_compile_error()
            .into()
        }
    };
    // Marker type name: `UpperCamelCase` of the function identifier.
    let marker = Ident::new(&ident.to_string().to_upper_camel_case(), ident.span());

    let expanded = quote! {
        #func

        /// Typed reference to this workflow, emitted by `#[durust::workflow]`.
        /// Pass it to `DurableEngine::start_with`.
        #[derive(Clone, Copy, Debug)]
        #vis struct #marker;

        impl durust::WorkflowDef for #marker {
            type Input = #input_ty;
            type Output = <#return_ty as durust::WorkflowResult>::Ok;
            const NAME: &'static str = #name;
        }

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

/// Parsed `#[step(...)]` arguments: an optional name override — a bare literal
/// (`#[step("charge")]`) or `name = "..."` — defaulting to the function name.
struct StepArgs {
    name: Option<String>,
}

impl Parse for StepArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        if input.is_empty() {
            return Ok(StepArgs { name: None });
        }
        if input.peek(LitStr) {
            return Ok(StepArgs {
                name: Some(input.parse::<LitStr>()?.value()),
            });
        }
        let key: Ident = input.parse()?;
        input.parse::<Token![=]>()?;
        let val: LitStr = input.parse()?;
        if key != "name" {
            return Err(syn::Error::new(
                key.span(),
                format!("unknown `#[step]` argument `{key}` (expected `name`)"),
            ));
        }
        Ok(StepArgs {
            name: Some(val.value()),
        })
    }
}

/// Turn an `async fn(&DurableContext, args..) -> Result<T>` into a durable
/// [`step`](durust::DurableContext::step): the body is checkpointed on first run
/// and served from the checkpoint on replay — exactly like calling
/// `ctx.step("name", || async move { ... })` by hand, but without the closure,
/// the `Box::pin`, or the `Ok::<_, Error>` annotation.
///
/// ```ignore
/// #[durust::step]
/// async fn charge(ctx: &DurableContext, cents: i64) -> Result<Receipt> {
///     // ordinary async work — runs at most once per logical step
///     Ok(gateway::charge(cents).await?)
/// }
///
/// // inside a workflow, call it like any async fn:
/// let receipt = charge(&ctx, 1299).await?;
/// ```
///
/// The step name defaults to the function name; override with `#[step("name")]`
/// or `#[step(name = "...")]`. The first parameter is the context the step
/// checkpoints into (usually `ctx: &DurableContext`); the rest are the step's
/// arguments.
#[proc_macro_attribute]
pub fn step(attr: TokenStream, item: TokenStream) -> TokenStream {
    let func = parse_macro_input!(item as ItemFn);
    let args = parse_macro_input!(attr as StepArgs);

    let name = args.name.unwrap_or_else(|| func.sig.ident.to_string());

    // The first parameter is the context the step checkpoints into.
    let ctx_ident = match func.sig.inputs.first() {
        Some(FnArg::Typed(pt)) => match &*pt.pat {
            syn::Pat::Ident(pi) => &pi.ident,
            _ => {
                return syn::Error::new_spanned(
                    &pt.pat,
                    "the first parameter of a `#[step]` fn must be a plain `ctx` binding",
                )
                .to_compile_error()
                .into()
            }
        },
        _ => {
            return syn::Error::new_spanned(
                &func.sig,
                "a `#[step]` fn must take `(&DurableContext, ..)`",
            )
            .to_compile_error()
            .into()
        }
    };

    let attrs = &func.attrs;
    let vis = &func.vis;
    let sig = &func.sig;
    let block = &func.block;

    let expanded = quote! {
        #(#attrs)*
        #vis #sig {
            #ctx_ident.step(#name, || async move #block).await
        }
    };

    expanded.into()
}
