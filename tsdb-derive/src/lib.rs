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

#[derive(FromMeta, Debug, Clone)]
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
            tsdb::ValueMode::CLUSTER
        },
        IndexMode::Exclusive => quote! {
            tsdb::ValueMode::EXCLUSIVE
        },
        IndexMode::Replace => quote! {
            tsdb::ValueMode::REPLACE
        },
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
                    Some((field, ty, sub, subsub, f.mode.clone()))
                } else {
                    None
                }
            })
            .collect();
        let fields_info: Vec<((TokenStream,TokenStream),(TokenStream,TokenStream))> = fields
        .iter()
        .map(|(field, ty, sub, subsub,index_mode)| {
            let indexed = translate_option_mode(index_mode);
            let field_name = field.to_string();
            let ty_name = ty.to_string().to_lowercase();
                let read_fill = quote! {
                    #field,
                };
            match (sub, subsub) {
                (Some(s), Some(s1)) => {
                    let desc = quote! {
                        fields.push(tsdb::FieldDescription::new(#field_name,tsdb::FieldType::resolve::<#ty<#s<#s1>>>(),#indexed));
                    };
                    let s_name = s.to_string().to_lowercase();
                    let s1_name = s1.to_string().to_lowercase();
                    let base_write = Ident::new( &format!("write_{}_{}", &ty_name,&s_name), Span::call_site());
                    let add_write = Ident::new( &format!("write_{}", &s1_name), Span::call_site());

                    let write = quote! {
                        write.#base_write(&self.#field,TWrite::#add_write)?;
                    };

                    let base_read = Ident::new(&format!("read_{}_{}",&ty_name, &s_name), Span::call_site());
                    let add_read = Ident::new(&format!("read_{}", &s1_name), Span::call_site());

                    let read =quote! {
                        let #field = read.#base_read(TRead::#add_read)?;
                    };
                    ((desc,write),(read,read_fill))
                },
                (Some(s), None) => {
                    let s_name = s.to_string().to_lowercase();

                    let desc =quote! {
                        fields.push(tsdb::FieldDescription::new(#field_name,tsdb::FieldType::resolve::<#ty<#s>>(),#indexed));
                    };
                    let base_write = Ident::new(&format!("write_{}", &ty_name), Span::call_site());
                    let add_write = Ident::new(&format!("write_{}", &s_name), Span::call_site());
                    let write = quote! {
                        write.#base_write(&self.#field,TWrite::#add_write)?;
                    };
                    let base_read = Ident::new(&format!("read_{}", &ty_name), Span::call_site());
                    let add_read = Ident::new(&format!("read_{}", &s_name), Span::call_site());
                    let read= quote! {
                        let #field = read.#base_read(TRead::#add_read)?;
                    };
                    ((desc,write),(read,read_fill))
                },
                (None, None) => {
                    let desc = quote! {
                        fields.push(tsdb::FieldDescription::new(#field_name,tsdb::FieldType::resolve::<#ty>(),#indexed));
                    };

                    let base_write =
                        Ident::new(&format!("write_{}", &ty_name), Span::call_site());
                    let write =quote! {
                        write.#base_write(&self.#field)?;
                    };
                    let base_read = Ident::new(&format!("read_{}", &ty_name), Span::call_site());
                    let read = quote! {
                        let #field = read.#base_read()?;
                    };
                    ((desc,write),(read,read_fill))
                },
                _ => panic!("can't happen"),
            }
        })
        .collect();

        let (fields_meta_write, fields_read_fill): (Vec<(TokenStream,TokenStream)>, Vec<(TokenStream, TokenStream)>) =
            fields_info.into_iter().unzip();
        let (fields_meta, fields_write): (Vec<TokenStream>, Vec<TokenStream>) = fields_meta_write.into_iter().unzip();
        let (fields_read, fields_construct): (Vec<TokenStream>, Vec<TokenStream>) = fields_read_fill.into_iter().unzip();

        let snippets: Vec<(TokenStream, (TokenStream, TokenStream))> = fields
            .iter()
            .filter(|(_, _, _, _, index_mode)| index_mode.is_some())
            .map(|(field, ty, sub, subsub, index_mode)| {
                let index_name = format!("{}.{}", name, field);
                let mode = translate_mode(&index_mode.as_ref().unwrap());
                match (sub, subsub) {
                    (Some(_), Some(s1)) => {
                        let declare = quote! {
                            tsdb::declare_index::<#s1>(db,#index_name,#mode)?;
                        };
                        let put = quote! {
                            if let Some(val) = self.#field {
                                for single in val {
                                    tsdb::put_index::<#s1,Self>(db,#index_name,single,id)?;
                                }
                            }
                        };
                        let remove = quote! {
                            tsdb::remove_index::<#s1,Self>(tx,#index_name,&self.#field,id)?;
                        };
                        (declare, (put, remove))
                    }

                    (Some(s), None) => {
                        let declare = quote! {
                            tsdb::declare_index::<#s>(db,#index_name,#mode)?;
                        };
                        let put = quote! {
                            for single in self.#field {
                                tsdb::put_index::<#s,Self>(db,#index_name,single,id)?;
                            }
                        };
                        let remove = quote! {
                            tsdb::remove_index::<#s,Self>(tx,#index_name,&self.#field,id)?;
                        };
                        (declare, (put, remove))
                    }

                    (None, None) => {
                        let declare = quote! {
                            tsdb::declare_index::<#ty>(db,#index_name,#mode)?;
                        };
                        let put = quote! {
                            tsdb::put_index::<#ty,Self>(tx,#index_name,&self.#field,id)?;
                        };
                        let remove = quote! {
                            tsdb::remove_index::<#ty,Self>(tx,#index_name,&self.#field,id)?;
                        };
                        (declare, (put, remove))
                    }
                    _ => panic!("can't happen"),
                }
            })
            .collect();
        let (index_declare, index_put_remove): (Vec<TokenStream>, Vec<(TokenStream, TokenStream)>) =
            snippets.into_iter().unzip();
        let (index_put, index_remove): (Vec<TokenStream>, Vec<TokenStream>) = index_put_remove.into_iter().unzip();
        let struct_name = name.to_string();
        let hash_id = "".to_string();
        let data = quote! {
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
        };

        let indexes = quote! {
                fn declare(&self, db:&tsdb::Tsdb)-> tsdb::TRes<()> {
                    #( #index_declare )*
                    Ok(())
                }

                fn put_indexes(&self, tx:&mut tsdb::Tstx, id:&tsdb::Ref<Self>) -> tsdb::TRes<()> {
                    #( #index_put )*
                    Ok(())
                }

                fn remove_indexes(&self, tx:&mut tsdb::Tstx, id:&tsdb::Ref<Self>) -> tsdb::TRes<()> {
                    #( #index_remove )*
                    Ok(())
                }
        };
        quote! {
            impl tsdb::Persistent for #name {
                #data

                #indexes
            }
        }
    }
}
