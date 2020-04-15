use darling::FromDeriveInput;
use syn::{parse_macro_input, AttributeArgs, DeriveInput, Item};
mod persistent;
use persistent::PersistentInfo;
mod queries;
use queries::persistent_queries;

#[proc_macro_attribute]
pub fn queries(args: proc_macro::TokenStream, original: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let parsed: Item = syn::parse(original).unwrap();
    let args: AttributeArgs = parse_macro_input!(args as AttributeArgs);
    let gen = persistent_queries(parsed, args);
    gen.into()
}
#[proc_macro_derive(PersistentEmbedded, attributes(index))]
pub fn persistent_embedded(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let parsed: DeriveInput = syn::parse(input).unwrap();

    let gen = PersistentInfo::from_derive_input(&parsed).unwrap().to_embedded_tokens();
    gen.into()
}

#[proc_macro_derive(Persistent, attributes(index))]
pub fn persistent(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let parsed: DeriveInput = syn::parse(input).unwrap();

    let gen = PersistentInfo::from_derive_input(&parsed).unwrap().to_tokens();
    gen.into()
}
