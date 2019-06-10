extern crate proc_macro;
extern crate syn;
#[macro_use]
extern crate quote;
extern crate proc_macro2;
use proc_macro2::TokenStream;
use syn::punctuated::Punctuated;
use syn::token::Comma;
use syn::Attribute;
use syn::Data::Struct;
use syn::Type::Path;
use syn::{DataStruct, DeriveInput, Field, Ident, TypePath};

#[proc_macro_derive(Persistent)]
pub fn persistent(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let parsed: DeriveInput = syn::parse(input).unwrap();

    let struct_name = &parsed.ident;
    let gen = match parsed.data {
        Struct(DataStruct {
            fields: syn::Fields::Named(ref fields),
            ..
        }) => impl_persistent_for_struct(struct_name, &fields.named, &parsed.attrs),
        _ => quote! {
            other shit
        },
    };
    gen.into()
}

fn impl_persistent_for_struct(
    name: &Ident,
    fields: &Punctuated<Field, Comma>,
    attrs: &[Attribute],
) -> TokenStream {
    let v: Vec<TokenStream> = fields
        .iter()
        .filter_map(|f| {
            let field = f.ident.clone().unwrap();
            let field_name = f.ident.clone().unwrap().to_string();
            if let Path(TypePath {
                path: syn::Path { ref segments, .. },
                ..
            }) = f.ty
            {
                let ty = segments.iter().last().unwrap().ident.to_string();
                Some(quote! {
                    declare(s.#field,  #field_name ,#ty );
                })
            } else {
                Some(quote! {
                    fn fr(){
                    }
                })
            }
        })
        .collect();
    quote! {
        fn save(s: #name) {
           #( #v )*
        }
    }
}
