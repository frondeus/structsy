extern crate proc_macro;
extern crate syn;
#[macro_use]
extern crate quote;
extern crate proc_macro2;
use proc_macro2::{Span, TokenStream};
use syn::punctuated::Punctuated;
use syn::token::Comma;
use syn::Attribute;
use syn::Data::Struct;
use syn::Type::Path;
use syn::{
    AngleBracketedGenericArguments, DataStruct, DeriveInput, Field, GenericArgument, Ident, PathArguments, PathSegment,
    Type, TypePath,
};

#[proc_macro_derive(Persistent, attributes(index))]
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

fn sub_type(t: &Type) -> Option<&Type> {
    let segs = match *t {
        syn::Type::Path(TypePath {
            path: syn::Path { ref segments, .. },
            ..
        }) => segments,
        _ => return None,
    };
    match *segs.iter().last().unwrap() {
        PathSegment {
            arguments: PathArguments::AngleBracketed(AngleBracketedGenericArguments { ref args, .. }),
            ..
        } if args.len() == 1 => {
            if let GenericArgument::Type(ref ty) = args[0] {
                Some(ty)
            } else {
                None
            }
        }
        _ => None,
    }
}

fn impl_persistent_for_struct(name: &Ident, fields: &Punctuated<Field, Comma>, attrs: &[Attribute]) -> TokenStream {
    let fields: Vec<(Ident, Ident, Option<Ident>, Option<Ident>)> = fields
        .iter()
        .filter_map(|f| {
            let field = f.ident.clone().unwrap();
            let st = sub_type(&f.ty);
            let sst = st.iter().filter_map(|x| sub_type(&x)).next();
            if let Path(ref path) = f.ty {
                let ty = path.clone().path.segments.iter().last().unwrap().ident.clone();
                let sub = st
                    .iter()
                    .filter_map(|x| {
                        if let Path(ref path) = x {
                            Some(path.clone().path.segments.iter().last().unwrap().ident.clone())
                        } else {
                            None
                        }
                    })
                    .next();
                let subsub = sst
                    .iter()
                    .filter_map(|x| {
                        if let Path(ref path) = x {
                            Some(path.clone().path.segments.iter().last().unwrap().ident.clone())
                        } else {
                            None
                        }
                    })
                    .next();
                Some((field, ty, sub, subsub))
            } else {
                None
            }
        })
        .collect();
    let fields_meta: Vec<TokenStream> = fields
        .iter()
        .map(|(field, ty, sub, subsub)| {
            let field_name = field.to_string();
            match (sub, subsub) {
                (Some(s), Some(s1)) => {
                    quote! {
                        fields.push(tsdb::FieldDescription::new(#field_name,tsdb::FieldType::resolve::<#ty<#s<#s1>>>(),true));
                    }
                }

            (Some(s), None) => {
            quote! {
                fields.push(tsdb::FieldDescription::new(#field_name,tsdb::FieldType::resolve::<#ty<#s>>(),true));
            }
            }

            (None, None) => {
            quote! {
                fields.push(tsdb::FieldDescription::new(#field_name,tsdb::FieldType::resolve::<#ty>(),true));
            }
            }
            _ => panic!("can't happen"),
        }
        })
        .collect();

    let fields_write: Vec<TokenStream> = fields
        .iter()
        .map(|(field, ty, sub, subsub)| match (sub, subsub) {
            (Some(s), Some(s1)) => {
                let base_write = Ident::new(
                    &format!(
                        "write_{}_{}",
                        &ty.to_string().to_lowercase(),
                        &s.to_string().to_lowercase()
                    ),
                    Span::call_site(),
                );
                let add_write = Ident::new(&format!("write_{}", &s1.to_string().to_lowercase()), Span::call_site());

                quote! {
                    write.#base_write(&self.#field,TWrite::#add_write)?;
                }
            }

            (Some(s), None) => {
                let base_write = Ident::new(&format!("write_{}", &ty.to_string().to_lowercase()), Span::call_site());
                let add_write = Ident::new(&format!("write_{}", &s.to_string().to_lowercase()), Span::call_site());
                quote! {
                    write.#base_write(&self.#field,TWrite::#add_write)?;
                }
            }

            (None, None) => {
                let base_write = Ident::new(&format!("write_{}", &ty.to_string().to_lowercase()), Span::call_site());
                quote! {
                    write.#base_write(&self.#field)?;
                }
            }
            _ => panic!("can't happen"),
        })
        .collect();

    let fields_read: Vec<TokenStream> = fields
        .iter()
        .map(|(field, ty, sub, subsub)| match (sub, subsub) {
            (Some(s), Some(s1)) => {
                let base_read = Ident::new(
                    &format!(
                        "read_{}_{}",
                        &ty.to_string().to_lowercase(),
                        &s.to_string().to_lowercase()
                    ),
                    Span::call_site(),
                );
                let add_read = Ident::new(&format!("read_{}", &s1.to_string().to_lowercase()), Span::call_site());

                quote! {
                    let #field = read.#base_read(TRead::#add_read)?;
                }
            }

            (Some(s), None) => {
                let base_read = Ident::new(&format!("read_{}", &ty.to_string().to_lowercase()), Span::call_site());
                let add_read = Ident::new(&format!("read_{}", &s.to_string().to_lowercase()), Span::call_site());
                quote! {
                    let #field = read.#base_read(TRead::#add_read)?;
                }
            }

            (None, None) => {
                let base_read = Ident::new(&format!("read_{}", &ty.to_string().to_lowercase()), Span::call_site());
                quote! {
                    let #field = read.#base_read()?;
                }
            }
            _ => panic!("can't happen"),
        })
        .collect();
    let fields_construct: Vec<TokenStream> = fields
        .iter()
        .map(|(field, _ty, _sub, _subsub)| {
            quote! {
                #field,
            }
        })
        .collect();
    let struct_name = name.to_string();
    let hash_id = "".to_string();
    quote! {

        impl tsdb::Persistent for #name {
            fn get_description() -> tsdb::StructDescription {
                let mut fields = Vec::new();
                #( #fields_meta )*
                tsdb::StructDescription::new(#struct_name,#hash_id,fields)
            }

            fn write(&self,write:&mut std::io::Write) -> tsdb::TRes<()> {
                use tsdb::TWrite;
                #( #fields_write )*
                Ok(())
            }

            fn read(read:&mut std::io::Read) -> tsdb::TRes<#name> {
                use tsdb::TRead;
                #( #fields_read )*
                Ok(#name {
                #( #fields_construct )*
                })
            }
        }
    }
}
