//! Procedural macros for `durare`.
//!
//! - [`macro@workflow`] leaves your async fn untouched and, alongside it, emits
//!   a compile-time registration (so the engine auto-discovers the workflow —
//!   no manual `engine.register(...)`) plus a typed `UpperCamelCase` marker
//!   implementing `durare::WorkflowDef`, so the workflow can be started by a
//!   type-checked reference rather than a string.
//! - [`macro@step`] wraps an async fn's body in a durable
//!   `ctx.step(...)` checkpoint, so a step reads like an ordinary `async fn`
//!   call — no closure, no `Box::pin`, no `Ok::<_, Error>` annotation.
//! - [`macro@transaction`] does the same for `ctx.transaction(...)`: the body's
//!   SQL writes and the checkpoint commit together, without the
//!   `|tx| Box::pin(async move { ... })` wrapper.

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
/// #[durare::workflow]
/// async fn process_order(ctx: DurableContext, order: Order) -> Result<Receipt> { ... }
///
/// // Override the registered name:
/// #[durare::workflow("orders.process")]
/// async fn process_order(ctx: DurableContext, order: Order) -> Result<Receipt> { ... }
///
/// // Run on a cron schedule (6-field cron, second precision). The workflow
/// // receives the scheduled tick time (RFC 3339) as its input:
/// #[durare::workflow(schedule = "0 0 * * * *")] // top of every hour
/// async fn hourly(ctx: DurableContext, scheduled_at: String) -> Result<()> { ... }
/// ```
///
/// The function is left as-is. The macro additionally emits:
/// - an `inventory` registration — `DurableEngine::new`/`builder` collect every
///   one in the binary, so annotated workflows need no manual `register` call;
///   scheduled ones start firing once [`DurableEngine::launch`] is called;
/// - a typed marker — an `UpperCamelCase` zero-sized struct named after the
///   function (`process_order` → `ProcessOrder`) implementing
///   `durare::WorkflowDef`, so `engine.start_with(ProcessOrder, order, opts)`
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

        /// Typed reference to this workflow, emitted by `#[durare::workflow]`.
        /// Pass it to `DurableEngine::start_with`.
        #[derive(Clone, Copy, Debug)]
        #vis struct #marker;

        impl durare::WorkflowDef for #marker {
            type Input = #input_ty;
            type Output = <#return_ty as durare::WorkflowResult>::Ok;
            const NAME: &'static str = #name;
        }

        durare::inventory::submit! {
            durare::WorkflowRegistration {
                name: #name,
                // A non-capturing closure coerces to `fn() -> WorkflowFn`.
                // `erase` infers the Input/Output types from the fn signature.
                builder: || durare::erase(#ident),
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
/// [`step`](durare::DurableContext::step): the body is checkpointed on first run
/// and served from the checkpoint on replay — exactly like calling
/// `ctx.step("name", || async move { ... })` by hand, but without the closure,
/// the `Box::pin`, or the `Ok::<_, Error>` annotation.
///
/// ```ignore
/// #[durare::step]
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

/// Parsed `#[transaction(...)]` arguments: an optional name override, like
/// [`StepArgs`].
struct TransactionArgs {
    name: Option<String>,
}

impl Parse for TransactionArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        if input.is_empty() {
            return Ok(TransactionArgs { name: None });
        }
        if input.peek(LitStr) {
            return Ok(TransactionArgs {
                name: Some(input.parse::<LitStr>()?.value()),
            });
        }
        let key: Ident = input.parse()?;
        input.parse::<Token![=]>()?;
        let val: LitStr = input.parse()?;
        if key != "name" {
            return Err(syn::Error::new(
                key.span(),
                format!("unknown `#[transaction]` argument `{key}` (expected `name`)"),
            ));
        }
        Ok(TransactionArgs {
            name: Some(val.value()),
        })
    }
}

/// The simple identifier a parameter binds, if it is a plain `name: Ty` pattern.
fn param_ident(arg: &FnArg) -> Option<&Ident> {
    match arg {
        FnArg::Typed(pt) => match &*pt.pat {
            syn::Pat::Ident(pi) => Some(&pi.ident),
            _ => None,
        },
        _ => None,
    }
}

/// Turn an `async fn(&DurableContext, &mut Tx, args..) -> Result<T>` into a
/// durable [`transaction`](durare::DurableContext::transaction): the body's SQL
/// writes and the step checkpoint commit in one database transaction, without
/// the `|tx| Box::pin(async move { ... })` wrapper.
///
/// ```ignore
/// #[durare::transaction]
/// async fn debit(ctx: &DurableContext, tx: &mut Tx<'_>, cents: i64, id: i64) -> Result<i64> {
///     tx.execute("UPDATE acct SET bal = bal - ? WHERE id = ?", &params![cents, id]).await?;
///     let row = tx.query_one("SELECT bal FROM acct WHERE id = ?", &params![id]).await?;
///     Ok(row.get::<i64>("bal"))
/// }
///
/// // the injected `tx` is not a caller argument:
/// let bal = debit(&ctx, 10, 1).await?;
/// ```
///
/// The first parameter is the context, the **second** is the transaction handle
/// (dropped from the public signature — it is supplied by the runtime), and the
/// rest are the caller's arguments. Because a transaction may retry on a
/// serialization conflict, its body runs more than once, so each argument is
/// re-`clone`d per attempt — arguments must be [`Clone`]. The step name defaults
/// to the fn name; override with `#[transaction("name")]`.
#[proc_macro_attribute]
pub fn transaction(attr: TokenStream, item: TokenStream) -> TokenStream {
    let func = parse_macro_input!(item as ItemFn);
    let args = parse_macro_input!(attr as TransactionArgs);

    let name = args.name.unwrap_or_else(|| func.sig.ident.to_string());

    let inputs: Vec<&FnArg> = func.sig.inputs.iter().collect();
    if inputs.len() < 2 {
        return syn::Error::new_spanned(
            &func.sig,
            "a `#[transaction]` fn must take `(&DurableContext, &mut Tx, ..)`",
        )
        .to_compile_error()
        .into();
    }
    let Some(ctx_ident) = param_ident(inputs[0]) else {
        return syn::Error::new_spanned(
            inputs[0],
            "the first parameter of a `#[transaction]` fn must be a plain `ctx` binding",
        )
        .to_compile_error()
        .into();
    };
    // `tx`: its ident names the generated closure parameter, and its written
    // type is referenced below so a `use ..::Tx` import stays live even though
    // the parameter is dropped from the public signature.
    let (FnArg::Typed(tx_pt), Some(tx_ident)) = (inputs[1], param_ident(inputs[1])) else {
        return syn::Error::new_spanned(
            inputs[1],
            "the second parameter of a `#[transaction]` fn must be a plain `tx: &mut Tx` binding",
        )
        .to_compile_error()
        .into();
    };
    let tx_ty = &*tx_pt.ty;
    // The caller's arguments are everything after `ctx` and `tx`. Each must be a
    // plain `name: Ty` binding so it can be re-cloned per attempt; a destructured
    // arg couldn't be, and would silently make the body `FnOnce` — a confusing
    // "expected `Fn`" error far from the cause — so reject it cleanly here.
    let mut arg_idents: Vec<&Ident> = Vec::new();
    for &arg in &inputs[2..] {
        let Some(id) = param_ident(arg) else {
            return syn::Error::new_spanned(
                arg,
                "arguments of a `#[transaction]` fn must be plain `name: Ty` bindings",
            )
            .to_compile_error()
            .into();
        };
        arg_idents.push(id);
    }

    // The public signature drops `tx` (index 1) — the runtime supplies it.
    let mut sig = func.sig.clone();
    sig.inputs = func
        .sig
        .inputs
        .iter()
        .enumerate()
        .filter_map(|(i, a)| (i != 1).then(|| a.clone()))
        .collect();

    let attrs = &func.attrs;
    let vis = &func.vis;
    let block = &func.block;

    let expanded = quote! {
        #(#attrs)*
        #[allow(clippy::clone_on_copy)]
        #vis #sig {
            #ctx_ident.transaction(#name, move |#tx_ident| {
                #( let #arg_idents = #arg_idents.clone(); )*
                ::std::boxed::Box::pin(async move #block)
            }).await
        }

        // The `tx` parameter is dropped from the signature above (the runtime
        // supplies it), so reference its written type here to keep a
        // `use ..::Tx` import from being reported as unused.
        const _: () = {
            #[allow(dead_code)]
            fn _tx_type_referenced(_: #tx_ty) {}
        };
    };

    expanded.into()
}
