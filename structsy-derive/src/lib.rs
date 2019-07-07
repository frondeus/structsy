extern crate proc_macro;
extern crate syn;
#[macro_use]
extern crate quote;
#[macro_use]
extern crate darling;
extern crate proc_macro2;
use darling::ast::Data;
use darling::{FromDeriveInput, FromField};
use proc_macro2::{Span, TokenStream};
use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;
use syn::Type::Path;
use syn::{
    AngleBracketedGenericArguments, DeriveInput, GenericArgument, Ident, PathArguments, PathSegment, Type, TypePath,
};

#[derive(FromDeriveInput, Debug)]
#[darling(attributes(index))]
struct PersistentInfo {
    ident: Ident,
    data: Data<(), PersistentAttr>,
}

#[derive(FromMeta, Debug, Clone, PartialEq)]
//#[darling(default)]
enum IndexMode {
    Exclusive,
    Cluster,
    Replace,
}
impl Default for IndexMode {
    fn default() -> IndexMode {
        IndexMode::Cluster
    }
}

#[derive(FromField, Debug)]
#[darling(attributes(index))]
struct PersistentAttr {
    ident: Option<Ident>,
    ty: syn::Type,
    #[darling(default)]
    mode: Option<IndexMode>,
}

#[proc_macro_derive(Persistent, attributes(index))]
pub fn persistent(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let parsed: DeriveInput = syn::parse(input).unwrap();

    let gen = PersistentInfo::from_derive_input(&parsed).unwrap().to_tokens();
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

fn translate_option_mode(mode: &Option<IndexMode>) -> TokenStream {
    match mode {
        None => quote! {
            None
        },
        Some(x) => {
            let mode = translate_mode(x);
            quote! {
                Some(#mode)
            }
        }
    }
}

fn translate_mode(mode: &IndexMode) -> TokenStream {
    match mode {
        IndexMode::Cluster => quote! {
            structsy::ValueMode::CLUSTER
        },
        IndexMode::Exclusive => quote! {
            structsy::ValueMode::EXCLUSIVE
        },
        IndexMode::Replace => quote! {
            structsy::ValueMode::REPLACE
        },
    }
}
fn get_type_ident(ty: &syn::Type) -> Option<Ident> {
    if let Path(ref path) = ty {
        Some(path.clone().path.segments.iter().last().unwrap().ident.clone())
    } else {
        None
    }
}

impl PersistentInfo {
    fn to_tokens(&self) -> TokenStream {
        let name = &self.ident;
        let fields: Vec<(Ident, Ident, Option<Ident>, Option<Ident>, Option<IndexMode>)> = self
            .data
            .as_ref()
            .take_struct()
            .unwrap()
            .fields
            .iter()
            .filter_map(|f| {
                let field = f.ident.clone().unwrap();
                let st = sub_type(&f.ty);
                let sub = st.iter().filter_map(|x| get_type_ident(*x)).next();
                let subsub = st.iter().filter_map(|x| sub_type(&x)).filter_map(get_type_ident).next();
                if let Some(ty) = get_type_ident(&f.ty) {
                    Some((field, ty, sub, subsub, f.mode.clone()))
                } else {
                    None
                }
            })
            .collect();
        let mut identity = fields
            .iter()
            .map(|(field, ty, sub, subsub, _index_mode)| {
                let mut fs = format!(":{}:{}", field.to_string(), ty.to_string());
                match (sub, subsub) {
                    (Some(x), Some(z)) => fs.push_str(&format!("<{}<{}>>", x.to_string(), z.to_string())),
                    (Some(x), None) => fs.push_str(&format!("<{}>", x.to_string())),
                    _ => {}
                };
                fs
            })
            .collect::<Vec<String>>();
        identity.sort();
        let mut hasher = DefaultHasher::new();
        hasher.write(format!("{}{}", name, identity.into_iter().collect::<String>()).as_bytes());
        let hash_id = hasher.finish();
        let fields_info: Vec<((TokenStream, TokenStream), (TokenStream, TokenStream))> = fields
            .iter()
            .map(|(field, ty, sub, subsub, index_mode)| {
                let indexed = translate_option_mode(index_mode);
                let field_name = field.to_string();
                let read_fill = quote! {
                    #field,
                };
                let desc =match (sub,subsub) {
                        (Some(x),Some(z)) => {
                            quote! {
                                fields.push(structsy::FieldDescription::new(#field_name,structsy::FieldType::resolve::<#ty<#x<#z>>>(),#indexed));
                            }
                        }
                        (Some(x),None) => {
                            quote! {
                                fields.push(structsy::FieldDescription::new(#field_name,structsy::FieldType::resolve::<#ty<#x>>(),#indexed));
                            }
                        }
                        (None,None) => {
                            quote! {
                                fields.push(structsy::FieldDescription::new(#field_name,structsy::FieldType::resolve::<#ty>(),#indexed));
                            }
                        }
                        (None,Some(_x)) => panic!(""),
                    };

                let write = quote! {
                    self.#field.write(write)?;
                };

                let read =quote! {
                    let #field = PersistentEmbedded::read(read)?;
                };
                ((desc, write), (read, read_fill))
            })
            .collect();

        let (fields_meta_write, fields_read_fill): (Vec<(TokenStream, TokenStream)>, Vec<(TokenStream, TokenStream)>) =
            fields_info.into_iter().unzip();
        let (fields_meta, fields_write): (Vec<TokenStream>, Vec<TokenStream>) = fields_meta_write.into_iter().unzip();
        let (fields_read, fields_construct): (Vec<TokenStream>, Vec<TokenStream>) =
            fields_read_fill.into_iter().unzip();

        let only_indexed: Vec<(Ident, Ident, Option<Ident>, Option<Ident>, Option<IndexMode>)> = fields
            .iter()
            .filter(|(_, _, _, _, index_mode)| index_mode.is_some())
            .map(|x| x.clone())
            .collect();

        let snippets: Vec<(TokenStream, (TokenStream, TokenStream))> = only_indexed
            .iter()
            .map(|(field, ty, sub, subsub, index_mode)| {
                let index_name = format!("{}.{}", name, field);
                let mode = translate_mode(&index_mode.as_ref().unwrap());
                let index_type = match (sub, subsub) {
                    (Some(_), Some(s1)) => s1,
                    (Some(s), None) => s,
                    _ => ty,
                };
                let declare = quote! {
                    structsy::declare_index::<#index_type>(db,#index_name,#mode)?;
                };
                let put = quote! {
                    self.#field.puts(tx,#index_name,id)?;
                };
                let remove = quote! {
                    self.#field.removes(tx,#index_name,id)?;
                };
                (declare, (put, remove))
            })
            .collect();
        let lookup_methods:Vec<TokenStream> = only_indexed.iter()
            .map(|(field, ty, sub, subsub, index_mode)| {
                let index_name = format!("{}.{}", name, field);
                let index_type = match (sub, subsub) {
                    (Some(_), Some(s1)) => s1,
                    (Some(s), None) => s,
                    _ =>ty,
                };
                let field_name = field.to_string();
                let find_by= Ident::new( &format!("find_by_{}", &field_name), Span::call_site());
                let find_by_tx= Ident::new( &format!("find_by_{}_tx", &field_name), Span::call_site());
                let find_by_range= Ident::new( &format!("find_by_{}_range", &field_name), Span::call_site());
                if index_mode == &Some(IndexMode::Cluster) {
                    let find = quote!{
                        fn #find_by(st:&structsy::Structsy, val:&#index_type) -> structsy::SRes<Vec<(structsy::Ref<Self>,Self)>> {
                            structsy::find(st,#index_name,val)
                        }
                        fn #find_by_tx(st:&mut structsy::Sytx, val:&#index_type) -> structsy::SRes<Vec<(structsy::Ref<Self>,Self)>> {
                            structsy::find_tx(st,#index_name,val)
                        }
                    };
                    let range = quote! {
                        fn #find_by_range<R:std::ops::RangeBounds<#index_type>>(st:&structsy::Structsy, range:R) -> structsy::SRes<impl Iterator<Item = (#index_type, Vec<(Ref<Self>, Self)>)>> {
                            structsy::find_range(st,#index_name,range)
                        }
                    };
                    quote! {
                        #find
                        #range
                    }
                } else {
                    let find =quote!{
                        fn #find_by(st:&structsy::Structsy, val:&#index_type) -> structsy::SRes<Option<(structsy::Ref<Self>,Self)>> {
                            structsy::find_unique(st,#index_name,val)
                        }
                        fn #find_by_tx(st:&mut structsy::Sytx, val:&#index_type) -> structsy::SRes<Option<(structsy::Ref<Self>,Self)>> {
                            structsy::find_unique_tx(st,#index_name,val)
                        }

                    };
                    let range = quote! {
                        fn #find_by_range<R:std::ops::RangeBounds<#index_type>>(st:&structsy::Structsy, range:R) -> structsy::SRes<impl Iterator<Item = (#index_type, (Ref<Self>, Self))>> {
                            structsy::find_unique_range(st,#index_name,range)
                        }
                    };

                    quote! {
                        #find
                        #range
                    }
                }
            }).collect();
        let impls = if lookup_methods.is_empty() {
            quote! {}
        } else {
            quote! {
                impl #name {
                    #( #lookup_methods )*
                }
            }
        };
        let (index_declare, index_put_remove): (Vec<TokenStream>, Vec<(TokenStream, TokenStream)>) =
            snippets.into_iter().unzip();
        let (index_put, index_remove): (Vec<TokenStream>, Vec<TokenStream>) = index_put_remove.into_iter().unzip();
        let struct_name = name.to_string();
        let data = quote! {
                fn get_description() -> structsy::StructDescription {
                    let mut fields = Vec::new();
                    #( #fields_meta )*
                    structsy::StructDescription::new(#struct_name,#hash_id,fields)
                }

                fn write(&self,write:&mut std::io::Write) -> structsy::SRes<()> {
                    use structsy::PersistentEmbedded;
                    #( #fields_write )*
                    Ok(())
                }

                fn read(read:&mut std::io::Read) -> structsy::SRes<#name> {
                    use structsy::PersistentEmbedded;
                    #( #fields_read )*
                    Ok(#name {
                    #( #fields_construct )*
                    })
                }
        };

        let indexes = quote! {
                fn declare(db:&mut structsy::Sytx)-> structsy::SRes<()> {
                    #( #index_declare )*
                    Ok(())
                }

                fn put_indexes(&self, tx:&mut structsy::Sytx, id:&structsy::Ref<Self>) -> structsy::SRes<()> {
                    use structsy::IndexableValue;
                    #( #index_put )*
                    Ok(())
                }

                fn remove_indexes(&self, tx:&mut structsy::Sytx, id:&structsy::Ref<Self>) -> structsy::SRes<()> {
                    use structsy::IndexableValue;
                    #( #index_remove )*
                    Ok(())
                }
        };
        quote! {

            impl structsy::Persistent for #name {

                #data

                #indexes
            }

            #impls
        }
    }
}
