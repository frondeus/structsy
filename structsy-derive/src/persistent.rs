use darling::ast::{Data, Fields, Style};
use darling::{FromDeriveInput, FromField, FromMeta, FromVariant};
use proc_macro2::{Span, TokenStream};
use quote::quote;
use syn::Type::Path;
use syn::{AngleBracketedGenericArguments, GenericArgument, Ident, PathArguments, PathSegment, Type, TypePath};

#[derive(FromDeriveInput, Debug)]
#[darling(forward_attrs(projection))]
pub struct ProjectionInfo {
    ident: Ident,
    data: Data<(), ProjectionAttr>,
    attrs: Vec<syn::Attribute>,
}

#[derive(FromDeriveInput, Debug)]
#[darling(attributes(index))]
pub struct PersistentInfo {
    ident: Ident,
    data: Data<PersistentEnum, PersistentAttr>,
}

#[derive(FromMeta, Debug, Clone, PartialEq)]
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

#[derive(FromVariant, Debug)]
struct PersistentEnum {
    ident: Ident,
    fields: Fields<syn::Type>,
}

#[derive(FromField, Debug)]
#[darling(attributes(index))]
struct PersistentAttr {
    ident: Option<Ident>,
    ty: syn::Type,
    #[darling(default)]
    mode: Option<IndexMode>,
}

#[derive(FromField, Debug)]
struct ProjectionAttr {
    ident: Option<Ident>,
    ty: syn::Type,
}

#[derive(Clone, Debug)]
struct FieldInfo {
    name: Ident,
    ty: Ident,
    template_ty: Option<Ident>,
    sub_template_ty: Option<Ident>,
    index_mode: Option<IndexMode>,
}
impl ProjectionInfo {
    fn field_infos(&self, fields: &Fields<ProjectionAttr>) -> Vec<FieldInfo> {
        fields
            .iter()
            .filter_map(|f| {
                let field = f.ident.clone().unwrap();
                let st = sub_type(&f.ty);
                let sub = st.iter().find_map(|x| get_type_ident(*x));
                let subsub = st.iter().filter_map(|x| sub_type(x)).find_map(get_type_ident);
                get_type_ident(&f.ty).map(|ty| FieldInfo {
                    name: field,
                    ty,
                    template_ty: sub,
                    sub_template_ty: subsub,
                    index_mode: None,
                })
            })
            .collect()
    }

    pub fn to_tokens(&self) -> TokenStream {
        let name = &self.ident;
        let mut target = None;
        for attr in self.attrs.iter() {
            let option = attr.parse_meta().unwrap();
            match option {
                // Match `#[projection = "xxx"]` attributes.  Match guard makes it `#[prefix = lit]`
                syn::Meta::NameValue(syn::MetaNameValue { ref path, ref lit, .. })
                    if path.get_ident().expect("projection attribute") == "projection" =>
                {
                    if let syn::Lit::Str(lit) = lit {
                        target = Some(lit.value());
                    }
                }
                _ => {
                    panic!("unsupported attribute");
                }
            }
        }
        match &self.data {
            Data::Struct(data) => {
                let fields = self.field_infos(data);
                let ser = projection_tokens(&fields);
                let target_ident = Ident::new(&target.expect("missing projection attributed"), Span::call_site());
                quote! {

                impl structsy::internal::Projection<#target_ident> for #name {

                    fn projection(source:& #target_ident) -> Self {
                        use structsy::internal::Projection;
                        #ser
                    }

                }

                }
            }
            Data::Enum(_) => {
                panic!("enum not supported as projection");
            }
        }
    }
}

impl PersistentInfo {
    fn field_infos(&self, fields: &Fields<PersistentAttr>) -> Vec<FieldInfo> {
        fields
            .iter()
            .filter_map(|f| {
                let field = f.ident.clone().unwrap();
                let st = sub_type(&f.ty);
                let sub = st.iter().find_map(|x| get_type_ident(*x));
                let subsub = st.iter().filter_map(|x| sub_type(x)).find_map(get_type_ident);
                get_type_ident(&f.ty).map(|ty| FieldInfo {
                    name: field,
                    ty,
                    template_ty: sub,
                    sub_template_ty: subsub,
                    index_mode: f.mode.clone(),
                })
            })
            .collect()
    }

    pub fn to_tokens(&self) -> TokenStream {
        let name = &self.ident;
        let string_name = name.to_string();
        match &self.data {
            Data::Struct(data) => {
                let fields = self.field_infos(data);
                let (desc, ser) = serialization_tokens(name, &fields);
                let indexes = indexes_tokens(name, &fields);
                let filters = filter_tokens(&fields);
                quote! {

                impl structsy::internal::Persistent for #name {

                    fn get_name() -> &'static str {
                        #string_name
                    }

                    #desc
                    #ser

                    #indexes
                }

                impl #name {
                    #filters
                }
                }
            }
            Data::Enum(variants) => {
                let (desc, ser) = enum_serialization_tokens(name, variants);

                quote! {
                impl structsy::internal::Persistent for #name {

                    #desc
                    #ser

                    fn declare(db:&mut structsy::Sytx)-> structsy::SRes<()> {
                        Ok(())
                    }

                    fn put_indexes(&self, tx:&mut structsy::Sytx, id:&structsy::Ref<Self>) -> structsy::SRes<()> {
                        Ok(())
                    }

                    fn remove_indexes(&self, tx:&mut structsy::Sytx, id:&structsy::Ref<Self>) -> structsy::SRes<()> {
                        Ok(())
                    }

                    fn get_name() -> &'static str {
                        #string_name
                    }
                }
                }
            }
        }
    }

    pub fn to_embedded_tokens(&self) -> TokenStream {
        let name = &self.ident;

        match &self.data {
            Data::Struct(data) => {
                let fields = self.field_infos(data);
                let (desc, ser) = serialization_tokens(name, &fields);
                let filters = filter_tokens(&fields);

                for f in fields {
                    if f.index_mode.is_some() {
                        panic!("indexing not supported for Persistent Embedded structs");
                    }
                }

                quote! {
                    impl structsy::internal::EmbeddedDescription for #name {
                        #desc
                    }
                    impl structsy::internal::PersistentEmbedded for #name {
                        #ser
                    }

                    impl #name {
                        #filters
                    }
                }
            }
            Data::Enum(variants) => {
                let (desc, ser) = enum_serialization_tokens(name, variants);

                quote! {
                    impl structsy::internal::EmbeddedDescription for #name {
                        #desc
                    }
                    impl structsy::internal::PersistentEmbedded for #name {
                        #ser
                    }
                }
            }
        }
    }
}
fn enum_serialization_tokens(name: &Ident, variants: &[PersistentEnum]) -> (TokenStream, TokenStream) {
    let enum_name = name.to_string();
    let variants_data = variants
        .iter()
        .enumerate()
        .map(|(pos, vt)| {
            let index = pos as u32;
            let tt = match vt.fields.style {
                Style::Tuple => {
                    if vt.fields.fields.len() == 1 {
                        match &vt.fields.fields[0] {
                            Type::Path(p) => Some(p.clone()),
                            _ => panic!("Supported only named types as enums values"),
                        }
                    } else if vt.fields.fields.is_empty() {
                        None
                    } else {
                        panic!("Tuples with multiple values not supported")
                    }
                }
                Style::Unit => None,
                _ => panic!("Supported only named types as enums values"),
            };
            (vt.ident.clone(), index, tt)
        })
        .collect::<Vec<_>>();

    let variants_meta = variants_data.iter().map(|(ident, index, tt)| {
        let vt_name = ident.to_string();
        if let Some(t) = tt {
            quote! {
                structsy::internal::VariantDescription::new_value::<#t>(#vt_name, #index),
            }
        } else {
            quote! {
                structsy::internal::VariantDescription::new(#vt_name, #index),
            }
        }
    });

    let variants_write = variants_data
        .iter()
        .map(|(ident, index, tt)| {
            if let Some(_t) = tt {
                quote! {
                   #name::#ident(v) => {
                        #index.write(write)?;
                        v.write(write)?;
                   }
                }
            } else {
                quote! {
                   #name::#ident => #index.write(write)?,
                }
            }
        })
        .collect::<Vec<_>>();

    let variants_read = variants_data
        .iter()
        .map(|(ident, index, tt)| {
            if let Some(t) = tt {
                quote! {
                   #index => #name::#ident(#t::read(read)?),
                }
            } else {
                quote! {
                   #index => #name::#ident,
                }
            }
        })
        .collect::<Vec<_>>();
    let desc = quote! {
            fn get_description() -> structsy::internal::Description {
                let fields  = [
                    #( #variants_meta )*
                ];
                structsy::internal::Description::Enum(
                    structsy::internal::EnumDescription::new(#enum_name,&fields)
                )
            }
    };
    let ser = quote! {
            fn write(&self,write:&mut std::io::Write) -> structsy::SRes<()> {
                use structsy::internal::PersistentEmbedded;
                match self {
                    #( #variants_write )*
                }
                Ok(())
            }

            fn read(read:&mut std::io::Read) -> structsy::SRes<#name> {
                use structsy::internal::PersistentEmbedded;
                Ok(match u32::read(read)? {
                    #( #variants_read )*
                    _ => panic!("data on disc do not match code structure"),
                })
            }
    };
    (desc, ser)
}

fn serialization_tokens(name: &Ident, fields: &[FieldInfo]) -> (TokenStream, TokenStream) {
    let fields_info = fields.iter().enumerate().map(|(position, field)| {
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
                    structsy::internal::FieldDescription::new::<#ty<#x<#z>>>(#pos,#field_name,#indexed),
                }
            }
            (Some(x), None) => {
                quote! {
                    structsy::internal::FieldDescription::new::<#ty<#x>>(#pos,#field_name,#indexed),
                }
            }
            (None, None) => {
                quote! {
                    structsy::internal::FieldDescription::new::<#ty>(#pos,#field_name,#indexed),
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
    });

    let (fields_meta_write, fields_read_fill): (Vec<(TokenStream, TokenStream)>, Vec<(TokenStream, TokenStream)>) =
        fields_info.unzip();
    let (fields_meta, fields_write): (Vec<TokenStream>, Vec<TokenStream>) = fields_meta_write.into_iter().unzip();
    let (fields_read, fields_construct): (Vec<TokenStream>, Vec<TokenStream>) = fields_read_fill.into_iter().unzip();

    let struct_name = name.to_string();
    let desc = quote! {
            fn get_description() -> structsy::internal::Description {
                let fields  = [
                    #( #fields_meta )*
                ];
                structsy::internal::Description::Struct(
                    structsy::internal::StructDescription::new(#struct_name,&fields)
                )
            }
    };
    let serialization = quote! {
            fn write(&self,write:&mut std::io::Write) -> structsy::SRes<()> {
                use structsy::internal::PersistentEmbedded;
                #( #fields_write )*
                Ok(())
            }

            fn read(read:&mut std::io::Read) -> structsy::SRes<#name> {
                use structsy::internal::PersistentEmbedded;
                #( #fields_read )*
                Ok(#name {
                #( #fields_construct )*
                })
            }
    };
    (desc, serialization)
}

fn indexes_tokens(name: &Ident, fields: &[FieldInfo]) -> TokenStream {
    let only_indexed: Vec<FieldInfo> = fields.iter().filter(|f| f.index_mode.is_some()).cloned().collect();

    let snippets = only_indexed.iter().map(|f| {
        let index_name = format!("{}.{}", name, f.name);
        let field = f.name.clone();
        let field_name = field.to_string();
        let t_name = name.to_string();
        let mode = translate_mode(f.index_mode.as_ref().unwrap());
        let index_type = match (f.template_ty.clone(), f.sub_template_ty.clone()) {
            (Some(_), Some(s1)) => s1,
            (Some(s), None) => s,
            _ => f.ty.clone(),
        };
        let declare = quote! {
            structsy::internal::declare_index::<#index_type>(db,#index_name,#mode)?;
        };
        let put = quote! {
            self.#field.puts(tx, #t_name, &[#field_name], id)?;
        };
        let remove = quote! {
            self.#field.removes(tx, #t_name, &[#field_name], id)?;
        };
        (declare, (put, remove))
    });
    let (index_declare, index_put_remove): (Vec<TokenStream>, Vec<(TokenStream, TokenStream)>) = snippets.unzip();
    let (index_put, index_remove): (Vec<TokenStream>, Vec<TokenStream>) = index_put_remove.into_iter().unzip();

    let indexes = quote! {
            fn declare(db:&mut structsy::Sytx)-> structsy::SRes<()> {
                #( #index_declare )*
                Ok(())
            }

            fn put_indexes(&self, tx:&mut structsy::Sytx, id:&structsy::Ref<Self>) -> structsy::SRes<()> {
                use structsy::internal::IndexableValue;
                #( #index_put )*
                Ok(())
            }

            fn remove_indexes(&self, tx:&mut structsy::Sytx, id:&structsy::Ref<Self>) -> structsy::SRes<()> {
                use structsy::internal::IndexableValue;
                #( #index_remove )*
                Ok(())
            }
    };
    indexes
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
            structsy::ValueMode::Cluster
        },
        IndexMode::Exclusive => quote! {
            structsy::ValueMode::Exclusive
        },
        IndexMode::Replace => quote! {
            structsy::ValueMode::Replace
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

fn filter_tokens(fields: &[FieldInfo]) -> TokenStream {
    let methods: Vec<TokenStream> = fields
        .iter()
        .map(|field| {
            let field_ident = field.name.clone();
            let t = field.ty.clone();
            let ty = match (field.template_ty.clone(), field.sub_template_ty.clone()) {
                (Some(x), Some(z)) => {
                    quote! {#t<#x<#z>>}
                }
                (Some(x), None) => quote! {#t<#x>},
                (None, None) => quote! {#t},
                (None, Some(_x)) => panic!(""),
            };
            let field_name = field.name.to_string();
            let meta_method = Ident::new(&format!("field_{}", &field_name), Span::call_site());
            quote! {
                pub fn #meta_method() -> structsy::internal::Field<Self,#ty> {
                    structsy::internal::Field::new(#field_name,|x|&x.#field_ident)
                }
            }
        })
        .collect();

    quote! {
        #( #methods )*
    }
}

fn projection_tokens(fields: &[FieldInfo]) -> TokenStream {
    let fields_info: Vec<TokenStream> = fields
        .iter()
        .map(|field| {
            let field_ident = field.name.clone();
            quote! {
                #field_ident: Projection::projection(&source.#field_ident),
            }
        })
        .collect();

    quote! {
        Self {
            #( #fields_info )*
        }
    }
}
