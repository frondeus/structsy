use darling::ast::{Data, Fields, Style};
use darling::{FromDeriveInput, FromField, FromMeta, FromVariant};
use proc_macro2::{Span, TokenStream};
use quote::quote;
use syn::Type::Path;
use syn::{AngleBracketedGenericArguments, GenericArgument, Ident, PathArguments, PathSegment, Type, TypePath};

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

#[derive(Clone)]
struct FieldInfo {
    name: Ident,
    ty: Ident,
    template_ty: Option<Ident>,
    sub_template_ty: Option<Ident>,
    index_mode: Option<IndexMode>,
}

impl PersistentInfo {
    fn field_infos(&self, fields: &Fields<PersistentAttr>) -> Vec<FieldInfo> {
        fields
            .iter()
            .filter_map(|f| {
                let field = f.ident.clone().unwrap();
                let st = sub_type(&f.ty);
                let sub = st.iter().filter_map(|x| get_type_ident(*x)).next();
                let subsub = st.iter().filter_map(|x| sub_type(&x)).filter_map(get_type_ident).next();
                if let Some(ty) = get_type_ident(&f.ty) {
                    Some(FieldInfo {
                        name: field,
                        ty,
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

    pub fn to_tokens(&self) -> TokenStream {
        let name = &self.ident;
        let string_name = name.to_string();
        match &self.data {
            Data::Struct(data) => {
                let fields = self.field_infos(&data);
                let (desc, ser) = serialization_tokens(name, &fields);
                let (indexes, impls) = indexes_tokens(name, &fields);
                let filters = filter_tokens(name, &fields, false);
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
                    #impls
                    #filters
                }
                }
            }
            Data::Enum(variants) => {
                let (desc, ser) = enum_serialization_tokens(name, &variants);

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
                let fields = self.field_infos(&data);
                let (desc, ser) = serialization_tokens(name, &fields);
                let filters = filter_tokens(name, &fields, true);

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
                let (desc, ser) = enum_serialization_tokens(name, &variants);

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
                    } else if vt.fields.fields.len() == 0 {
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

fn serialization_tokens(name: &Ident, fields: &Vec<FieldInfo>) -> (TokenStream, TokenStream) {
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
        })
        .collect();

    let (fields_meta_write, fields_read_fill): (Vec<(TokenStream, TokenStream)>, Vec<(TokenStream, TokenStream)>) =
        fields_info.into_iter().unzip();
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
                structsy::internal::declare_index::<#index_type>(db,#index_name,#mode)?;
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
                        #[deprecated]
                        fn #find_by(st:&structsy::Structsy, val:&#index_type) -> structsy::SRes<Vec<(structsy::Ref<Self>,Self)>> {
                            structsy::internal::find(st,#index_name,val)
                        }
                        #[deprecated]
                        fn #find_by_tx(st:&mut structsy::Sytx, val:&#index_type) -> structsy::SRes<Vec<(structsy::Ref<Self>,Self)>> {
                            structsy::internal::find_tx(st,#index_name,val)
                        }
                    };
                    let range = quote! {
                        #[deprecated]
                        fn #find_by_range<R:std::ops::RangeBounds<#index_type>>(st:&structsy::Structsy, range:R) -> structsy::SRes<impl Iterator<Item = (structsy::Ref<Self>, Self,#index_type)>> {
                            structsy::internal::find_range(st,#index_name,range)
                        }
                    };
                    let range_tx = quote! {
                        #[deprecated]
                        fn #find_by_range_tx<'a, R:std::ops::RangeBounds<#index_type>>(st:&'a mut structsy::Sytx, range:R) -> structsy::SRes<structsy::RangeIterator<'a,#index_type,Self>> {
                            structsy::internal::find_range_tx(st,#index_name,range)
                        }
                    };
                    quote! {
                        #find
                        #range
                        #range_tx
                    }
                } else {
                    let find =quote!{
                        #[deprecated]
                        fn #find_by(st:&structsy::Structsy, val:&#index_type) -> structsy::SRes<Option<(structsy::Ref<Self>,Self)>> {
                            structsy::internal::find_unique(st,#index_name,val)
                        }
                        #[deprecated]
                        fn #find_by_tx(st:&mut structsy::Sytx, val:&#index_type) -> structsy::SRes<Option<(structsy::Ref<Self>,Self)>> {
                            structsy::internal::find_unique_tx(st,#index_name,val)
                        }

                    };
                    let range = quote! {
                        #[deprecated]
                        fn #find_by_range<R:std::ops::RangeBounds<#index_type>>(st:&structsy::Structsy, range:R) -> structsy::SRes<impl Iterator<Item = (Ref<Self>, Self,#index_type)>> {
                            structsy::internal::find_unique_range(st,#index_name,range)
                        }
                    };
                    let range_tx = quote! {
                        #[deprecated]
                        fn #find_by_range_tx<'a,R:std::ops::RangeBounds<#index_type>>(st:&'a mut structsy::Sytx, range:R) -> structsy::SRes<structsy::UniqueRangeIterator<'a,#index_type,Self>> {
                            structsy::internal::find_unique_range_tx(st,#index_name,range)
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
            #( #lookup_methods )*
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
    (indexes, impls)
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

fn is_simple_type(field: &FieldInfo) -> bool {
    let (t, r) = match (field.template_ty.clone(), field.sub_template_ty.clone()) {
        (Some(r), Some(z)) => (z, Some(r.clone())),
        (Some(x), None) => (x, Some(field.ty.clone())),
        (None, None) => (field.ty.clone(), None),
        (None, Some(_x)) => panic!(""),
    };
    match t.to_string().as_str() {
        "String" | "u8" | "u16" | "u32" | "u64" | "u128" | "i8" | "i16" | "i32" | "i64" | "i128" | "f32" | "f64"
        | "bool" => true,
        _ => match r.map(|x| x.to_string()).unwrap_or(String::from("")).as_str() {
            "Ref" => true,
            _ => false,
        },
    }
}

fn basic_filter_gen(
    mode: &str,
    field: Ident,
    ty: Ident,
    cont_ty: Option<Ident>,
    second_ty: Option<Ident>,
    builder: &TokenStream,
) -> TokenStream {
    let field_name = field.to_string();
    let ty_str_lower = ty.to_string().to_lowercase();
    let target_field_name = if let (Some(oth), Some(sec)) = (&cont_ty, &second_ty) {
        format!(
            "field_{}_{}_{}",
            field_name,
            sec.to_string().to_lowercase(),
            oth.to_string().to_lowercase(),
        )
    } else if let Some(other) = &cont_ty {
        format!(
            "field_{}_{}_{}",
            field_name,
            other.to_string().to_lowercase(),
            ty_str_lower
        )
    } else {
        format!("field_{}_{}", field_name, ty_str_lower)
    };

    let method_ident = Ident::new(&target_field_name, Span::call_site());
    let method_range_ident = Ident::new(&format!("{}_range", target_field_name), Span::call_site());

    let range_filter = Ident::new(&format!("{}_range", mode), Span::call_site());
    let condition_filter = Ident::new(&format!("{}_condition", mode), Span::call_site());

    let tt = if let (Some(oth), Some(sec)) = (&cont_ty, &second_ty) {
        quote! {#sec<#oth<#ty>>}
    } else if let Some(other) = cont_ty {
        quote! {#other<#ty>}
    } else {
        quote! {#ty}
    };

    quote! {
        pub fn #method_ident(builder:&mut #builder,v:#tt){
            builder.#condition_filter(structsy::internal::Field::new(#field_name,|x|&x.#field),v);
        }
        pub fn #method_range_ident< R: std::ops::RangeBounds<#tt>>(builder:&mut #builder,v:R){
            builder.#range_filter(structsy::internal::Field::new(#field_name,|x|&x.#field),v);
        }
    }
}

fn filter_tokens(name: &Ident, fields: &Vec<FieldInfo>, embedded: bool) -> TokenStream {
    let mode;
    let filter;
    if embedded {
        mode = "simple".to_string();
        filter = quote! {structsy::internal::EmbeddedFilterBuilder<#name>};
    } else {
        mode = "indexable".to_string();
        filter = quote! {structsy::internal::FilterBuilder<#name>};
    }
    let methods: Vec<TokenStream> = fields
        .iter()
        .map(|field| {
            let is_simple = is_simple_type(field);
            let field_ident = field.name.clone();
            let ty = field.ty.clone();
            match (field.template_ty.clone(), field.sub_template_ty.clone()) {
                (Some(x), Some(z)) => {
                    double_template_field_methods(is_simple, filter.clone(), field_ident, ty, x, z, &mode)
                }
                (Some(x), None) => template_field_methods(is_simple, filter.clone(), field_ident, ty, x, &mode),
                (None, None) => simple_field_methos(is_simple, filter.clone(), field_ident, ty, &mode),
                (None, Some(_x)) => panic!(""),
            }
        })
        .collect();

    quote! {
        #( #methods )*
    }
}

fn double_template_field_methods(
    is_simple: bool,
    filter: TokenStream,
    field: Ident,
    ty: Ident,
    x: Ident,
    z: Ident,
    _mode: &str,
) -> TokenStream {
    let field_name = field.to_string();
    let ty_str_lower = ty.to_string().to_lowercase();
    if !is_simple {
        panic!("Complex types not supported, only supported simple types and Vec or Option of simple types");
    } else if x.to_string() == "Ref" {
        let prefix = format!("ref_{}", ty_str_lower);
        let z_str_lower = z.to_string().to_lowercase();
        let basic = basic_filter_gen(
            &prefix,
            field.clone(),
            z.clone(),
            Some(x.clone()),
            Some(ty.clone()),
            &filter,
        );
        let method_ident_query = Ident::new(
            &format!("field_{}_structsyquery_{}", field_name, z_str_lower),
            Span::call_site(),
        );
        let method_name = Ident::new(&format!("ref_{}_query", ty_str_lower), Span::call_site());
        quote! {
            #basic

            pub fn #method_ident_query(builder:&mut #filter,v:structsy::StructsyQuery<#z>){
                builder.#method_name(structsy::internal::Field::new(#field_name,|x|&x.#field),v);
            }

        }
    } else {
        panic!("Complex types not supported, only supported simple types and Vec or Option of simple types");
    }
}
fn template_field_methods(
    is_simple: bool,
    filter: TokenStream,
    field: Ident,
    ty: Ident,
    x: Ident,
    mode: &str,
) -> TokenStream {
    let field_name = field.to_string();
    let ty_str_lower = ty.to_string().to_lowercase();
    if !is_simple {
        let x_str_lower = x.to_string().to_lowercase();
        let method_ident = Ident::new(
            &format!("field_{}_embeddedfilter_{}", field_name, x_str_lower),
            Span::call_site(),
        );
        let condition_method = Ident::new("simple_persistent_embedded", Span::call_site());
        quote! {
            pub fn #method_ident(builder:&mut #filter,v:structsy::EmbeddedFilter<#x>){
                builder.#condition_method(structsy::interna::Field::new(#field_name,|x|&x.#field),v);
            }
        }
    } else if ty.to_string() == "Ref" {
        let x_str_lower = x.to_string().to_lowercase();
        let basic = basic_filter_gen("ref", field.clone(), x.clone(), Some(ty.clone()), None, &filter);
        let method_ident_query = Ident::new(
            &format!("field_{}_structsyquery_{}", field_name, x_str_lower),
            Span::call_site(),
        );
        quote! {
            #basic
            pub fn #method_ident_query(builder:&mut #filter,v:structsy::StructsyQuery<#x>){
                builder.ref_query(structsy::internal::Field::new(#field_name,|x|&x.#field),v);
            }
        }
    } else {
        let x_str_lower = x.to_string().to_lowercase();
        let method_ident_contains = Ident::new(&format!("field_{}_{}", field_name, x_str_lower), Span::call_site());
        if x.to_string() == "bool" {
            let method_ident = Ident::new(
                &format!("field_{}_{}_{}", field_name, ty_str_lower, x_str_lower),
                Span::call_site(),
            );
            let condition_method_name = format!("simple_{}_condition", ty_str_lower);
            let condition_method_name_contains = format!("simple_{}_single_condition", ty_str_lower);
            let condition_method = Ident::new(&condition_method_name, Span::call_site());
            let condition_method_contains = Ident::new(&condition_method_name_contains, Span::call_site());
            quote! {
                pub fn #method_ident(builder:&mut #filter,v:#ty<#x>){
                    builder.#condition_method(structsy::internal::Field::new(#field_name,|x|&x.#field),v);
                }
                pub fn #method_ident_contains(builder:&mut #filter,v:#x){
                    builder.#condition_method_contains(structsy::internal::Field::new(#field_name,|x|&x.#field),v);
                }
            }
        } else {
            let additional = if x.to_string() == "String" {
                let condition_method = Ident::new(
                    &format!("{}_{}_single_condition", mode, ty_str_lower),
                    Span::call_site(),
                );
                let method_str_ident = Ident::new(&format!("field_{}_str", field_name), Span::call_site());
                quote! {
                    pub fn #method_str_ident(builder:&mut #filter,v:&str){
                        builder.#condition_method(structsy::internal::Field::new(#field_name,|x|&x.#field),v.to_string());
                    }
                }
            } else {
                quote! {}
            };
            let prefix = format!("{}_{}", mode, ty_str_lower);
            let basic = basic_filter_gen(&prefix, field.clone(), x.clone(), Some(ty.clone()), None, &filter);
            let condition_method_name_contains = format!("{}_{}_single_condition", mode, ty_str_lower);
            let condition_method_contains = Ident::new(&condition_method_name_contains, Span::call_site());
            let range_single_method = Ident::new(&format!("{}_{}_single_range", mode, ty_str_lower), Span::call_site());
            let method_range_single_ident = Ident::new(
                &format!("field_{}_{}_range", field_name, x.to_string().to_lowercase()),
                Span::call_site(),
            );
            quote! {
                #basic
                #additional
                pub fn #method_ident_contains(builder:&mut #filter,v:#x){
                    builder.#condition_method_contains(structsy::internal::Field::new(#field_name,|x|&x.#field),v);
                }
                pub fn #method_range_single_ident< R: std::ops::RangeBounds<#x> >(builder:&mut #filter,v:R){
                    builder.#range_single_method(structsy::internal::Field::new(#field_name,|x|&x.#field),v);
                }
            }
        }
    }
}
fn simple_field_methos(is_simple: bool, filter: TokenStream, field: Ident, ty: Ident, mode: &str) -> TokenStream {
    let field_name = field.to_string();
    let ty_str_lower = ty.to_string().to_lowercase();
    let method_ident = Ident::new(&format!("field_{}_{}", field_name, ty_str_lower), Span::call_site());
    if !is_simple {
        let method_ident = Ident::new(
            &format!("field_{}_embeddedfilter_{}", field_name, ty_str_lower),
            Span::call_site(),
        );
        let condition_method = Ident::new("simple_persistent_embedded", Span::call_site());
        quote! {
            pub fn #method_ident(builder:&mut #filter,v:structsy::EmbeddedFilter<#ty>){
                builder.#condition_method(structsy::internal::Field::new(#field_name,|x|&x.#field),v);
            }
        }
    } else if ty.to_string() == "bool" {
        let condition_method = Ident::new("simple_condition", Span::call_site());
        quote! {
            pub fn #method_ident(builder:&mut #filter,v:#ty){
                builder.#condition_method(structsy::internal::Field::new(#field_name,|x|&x.#field),v);
            }
        }
    } else {
        let addtional = if ty.to_string() == "String" {
            let condition_method = Ident::new(&format!("{}_condition", mode), Span::call_site());
            let method_str_ident = Ident::new(&format!("field_{}_str", field_name), Span::call_site());
            let method_str_ident_range = Ident::new(&format!("field_{}_str_range", field_name), Span::call_site());
            let range_method = Ident::new(&format!("{}_range_str", mode), Span::call_site());
            quote! {
                pub fn #method_str_ident(builder:&mut #filter,v:&str){
                    builder.#condition_method(structsy::internal::Field::new(#field_name,|x|&x.#field),v.to_string());
                }
                pub fn #method_str_ident_range<'a,R: std::ops::RangeBounds<&'a str>>(builder:&mut #filter,v:R){
                    builder.#range_method(structsy::internal::Field::new(#field_name,|x|&x.#field),v);
                }
            }
        } else {
            quote! {}
        };
        let basic = basic_filter_gen(&mode, field, ty, None, None, &filter);

        quote! {
            #basic
            #addtional
        }
    }
}
