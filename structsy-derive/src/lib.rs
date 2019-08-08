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

#[derive(Clone)]
struct FieldInfo {
    name: Ident,
    ty: Ident,
    template_ty: Option<Ident>,
    sub_template_ty: Option<Ident>,
    index_mode: Option<IndexMode>,
}

fn serializetion_tokens(name: &Ident, fields: &Vec<FieldInfo>) -> (TokenStream, TokenStream) {
    let fields_info: Vec<((TokenStream, TokenStream), (TokenStream, TokenStream))> = fields
        .iter()
        .enumerate()
        .map(|(position, field)| {
            let pos = position as u32;
            let indexed = translate_option_mode(&field.index_mode);
            let field_name = field.name.to_string();
            let field_ident = field.name.clone();
            let read_fill = quote! {
                #field_ident,
            };
            let ty = field.ty.clone();
            let desc = match (field.template_ty.clone(), field.sub_template_ty.clone()) {
                (Some(x), Some(z)) => {
                    quote! {
                        fields.push(structsy::FieldDescription::new::<#ty<#x<#z>>>(#pos,#field_name,#indexed));
                    }
                }
                (Some(x), None) => {
                    quote! {
                        fields.push(structsy::FieldDescription::new::<#ty<#x>>(#pos,#field_name,#indexed));
                    }
                }
                (None, None) => {
                    quote! {
                        fields.push(structsy::FieldDescription::new::<#ty>(#pos,#field_name,#indexed));
                    }
                }
                (None, Some(_x)) => panic!(""),
            };

            let write = quote! {
                self.#field_ident.write(write)?;
            };

            let read = quote! {
                let #field_ident= PersistentEmbedded::read(read)?;
            };
            ((desc, write), (read, read_fill))
        })
        .collect();

    let (fields_meta_write, fields_read_fill): (Vec<(TokenStream, TokenStream)>, Vec<(TokenStream, TokenStream)>) =
        fields_info.into_iter().unzip();
    let (fields_meta, fields_write): (Vec<TokenStream>, Vec<TokenStream>) = fields_meta_write.into_iter().unzip();
    let (fields_read, fields_construct): (Vec<TokenStream>, Vec<TokenStream>) = fields_read_fill.into_iter().unzip();

    let struct_name = name.to_string();
    let desc = quote! {
            fn get_description() -> structsy::StructDescription {
                let mut fields = Vec::new();
                #( #fields_meta )*
                structsy::StructDescription::new(#struct_name,fields)
            }
    };
    let serialization = quote! {
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
    (desc, serialization)
}

fn indexes_tokens(name: &Ident, fields: &Vec<FieldInfo>) -> (TokenStream, TokenStream) {
    let only_indexed: Vec<FieldInfo> = fields
        .iter()
        .filter(|f| f.index_mode.is_some())
        .map(|x| x.clone())
        .collect();

    let snippets: Vec<(TokenStream, (TokenStream, TokenStream))> = only_indexed
        .iter()
        .map(|f| {
            let index_name = format!("{}.{}", name, f.name);
            let field = f.name.clone();
            let mode = translate_mode(&f.index_mode.as_ref().unwrap());
            let index_type = match (f.template_ty.clone(), f.sub_template_ty.clone()) {
                (Some(_), Some(s1)) => s1,
                (Some(s), None) => s,
                _ => f.ty.clone(),
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
            .map(|f| {
                let index_name = format!("{}.{}", name, f.name);
                let index_type = match (f.template_ty.clone(),f.sub_template_ty.clone()) {
                    (Some(_), Some(s1)) => s1,
                    (Some(s), None) => s,
                    _ =>f.ty.clone(),
                };
                let field_name = f.name.to_string();
                let find_by= Ident::new( &format!("find_by_{}", &field_name), Span::call_site());
                let find_by_tx= Ident::new( &format!("find_by_{}_tx", &field_name), Span::call_site());
                let find_by_range= Ident::new( &format!("find_by_{}_range", &field_name), Span::call_site());
                let find_by_range_tx= Ident::new( &format!("find_by_{}_range_tx", &field_name), Span::call_site());
                if f.index_mode == Some(IndexMode::Cluster) {
                    let find = quote!{
                        fn #find_by(st:&structsy::Structsy, val:&#index_type) -> structsy::SRes<Vec<(structsy::Ref<Self>,Self)>> {
                            structsy::find(st,#index_name,val)
                        }
                        fn #find_by_tx(st:&mut structsy::Sytx, val:&#index_type) -> structsy::SRes<Vec<(structsy::Ref<Self>,Self)>> {
                            structsy::find_tx(st,#index_name,val)
                        }
                    };
                    let range = quote! {
                        fn #find_by_range<R:std::ops::RangeBounds<#index_type>>(st:&structsy::Structsy, range:R) -> structsy::SRes<impl Iterator<Item = (structsy::Ref<Self>, Self,#index_type)>> {
                            structsy::find_range(st,#index_name,range)
                        }
                    };
                    let range_tx = quote! {
                        fn #find_by_range_tx<'a, R:std::ops::RangeBounds<#index_type>>(st:&'a mut structsy::Sytx, range:R) -> structsy::SRes<structsy::RangeIterator<'a,#index_type,Self>> {
                            structsy::find_range_tx(st,#index_name,range)
                        }
                    };
                    quote! {
                        #find
                        #range
                        #range_tx
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
                        fn #find_by_range<R:std::ops::RangeBounds<#index_type>>(st:&structsy::Structsy, range:R) -> structsy::SRes<impl Iterator<Item = (Ref<Self>, Self,#index_type)>> {
                            structsy::find_unique_range(st,#index_name,range)
                        }
                    };
                    let range_tx = quote! {
                        fn #find_by_range_tx<'a,R:std::ops::RangeBounds<#index_type>>(st:&'a mut structsy::Sytx, range:R) -> structsy::SRes<structsy::UniqueRangeIterator<'a,#index_type,Self>> {
                            structsy::find_unique_range_tx(st,#index_name,range)
                        }
                    };

                    quote! {
                        #find
                        #range
                        #range_tx
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
    (indexes, impls)
}

impl PersistentInfo {
    fn field_infos(&self) -> Vec<FieldInfo> {
        self.data
            .as_ref()
            .take_struct()
            .expect("Only struct type supported")
            .fields
            .iter()
            .filter_map(|f| {
                let field = f.ident.clone().unwrap();
                let st = sub_type(&f.ty);
                let sub = st.iter().filter_map(|x| get_type_ident(*x)).next();
                let subsub = st.iter().filter_map(|x| sub_type(&x)).filter_map(get_type_ident).next();
                if let Some(ty) = get_type_ident(&f.ty) {
                    Some(FieldInfo {
                        name: field,
                        ty: ty,
                        template_ty: sub,
                        sub_template_ty: subsub,
                        index_mode: f.mode.clone(),
                    })
                } else {
                    None
                }
            })
            .collect()
    }

    fn to_tokens(&self) -> TokenStream {
        let name = &self.ident;
        let fields = self.field_infos();
        let (desc, ser) = serializetion_tokens(name, &fields);
        let (indexes, impls) = indexes_tokens(name, &fields);
        let string_name = name.to_string();
        quote! {

            impl structsy::Persistent for #name {

                fn get_name() -> &'static str {
                    #string_name
                }

                #desc
                #ser

                #indexes
            }

            #impls
        }
    }

    fn to_embedded_tokens(&self) -> TokenStream {
        let name = &self.ident;
        let fields = self.field_infos();
        let (desc, ser) = serializetion_tokens(name, &fields);

        for f in fields {
            if f.index_mode.is_some() {
                panic!("indexing not supported for Persistent Embedded structs");
            }
        }

        quote! {
            impl structsy::EmbeddedDescription for #name {
                #desc
            }
            impl structsy::PersistentEmbedded for #name {
                #ser
            }
        }
    }
}
