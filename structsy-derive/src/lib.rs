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
use std::borrow::Borrow;
use syn::Type::Path;
use syn::{
    parse_macro_input, AngleBracketedGenericArguments, AttributeArgs, DeriveInput, FnArg, GenericArgument,
    GenericParam, Ident, Item, Meta, NestedMeta, Pat, PathArguments, PathSegment, ReturnType, Signature, TraitItem,
    Type, TypeParamBound, TypePath,
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
enum Operation {
    Equals(String, String),
    Range(String, String),
}

fn extract_fields(s: &Signature) -> Vec<Operation> {
    let mut res = Vec::new();
    let mut mapping = Vec::new();
    if s.generics.params.len() == 1 {
        if let Some(GenericParam::Type(t)) = s.generics.params.first() {
            if !t.bounds.is_empty() {
                let name = t.ident.clone();
                if let Some(TypeParamBound::Trait(bound)) = t.bounds.first() {
                    if let Some(seg) = bound.path.segments.last() {
                        if let PathArguments::AngleBracketed(a) = &seg.arguments {
                            if let Some(GenericArgument::Type(Type::Path(tp))) = a.args.first() {
                                if let Some(last_s) = tp.path.segments.first() {
                                    mapping.push((name.to_string(), last_s.ident.to_string()));
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    let mut inps = s.inputs.iter();
    // Skip self checked in check_method
    inps.next();
    while let Some(FnArg::Typed(f)) = inps.next() {
        let name = if let Pat::Ident(ref i) = &*f.pat {
            Some(i.ident.to_string())
        } else {
            None
        };
        let ty = if let Type::Path(t) = &*f.ty {
            Some(t.path.segments.last().unwrap().ident.to_string())
        } else {
            None
        };
        let mut range = false;
        for (n, rn) in &mapping {
            if let (Some(nam), Some(rt)) = (&name, &ty) {
                if n == rt {
                    res.push(Operation::Range(nam.clone(), rn.clone()));
                    range = true;
                }
            }
        }
        if !range {
            if let (Some(n), Some(t)) = (name, ty) {
                res.push(Operation::Equals(n, t));
            }
        }
    }
    res
}

fn check_method(s: &Signature, target_type: &str) {
    if s.constness.is_some() {
        panic!(" const methods not suppored: {:?}", s);
    }
    if s.asyncness.is_some() {
        panic!(" async methods not suppored: {:?}", s);
    }
    if s.asyncness.is_some() {
        panic!(" unsafe methods not suppored: {:?}", s);
    }
    if s.abi.is_some() {
        panic!(" extern methods not suppored: {:?}", s);
    }
    if let ReturnType::Type(_, t) = &s.output {
        if let Type::Path(ref p) = t.borrow() {
            let last = p.path.segments.last().expect("expect return type");
            let name = last.ident.to_string();
            if name != "IterResult" && name != "FirstResult" {
                panic!("only allowed return types are 'IterResult' and 'FirstResult' ");
            }
            if let PathArguments::AngleBracketed(ref a) = &last.arguments {
                if let Some(GenericArgument::Type(t)) = a.args.first() {
                    if let Type::Path(ref p) = t.borrow() {
                        let last = p.path.segments.last().expect("expect return type");
                        let name = last.ident.to_string();
                        if name != target_type {
                            panic!("the return type should be {}<{}> ", name, target_type);
                        }
                    }
                }
            }
        } else {
            panic!(" expected a return type");
        }
    } else {
        panic!(" expected a return type");
    }
    if let Some(FnArg::Receiver(r)) = s.inputs.first() {
        if r.reference.is_none() {
            panic!("first argument of a method should be &self");
        }
    } else {
        panic!("first argument of a method should be &self");
    }
    if s.inputs.len() < 2 {
        panic!("function should have at least two arguments");
    }
    let mut range = false;
    if s.generics.params.len() == 1 {
        if let Some(GenericParam::Type(t)) = s.generics.params.first() {
            if !t.bounds.is_empty() {
                range = true;
            }
        }
    }
    if !s.generics.params.is_empty() && !range {
        panic!("generics not supported {:?}", s.generics.params.first());
    }
}

fn impl_trait_methods(item: TraitItem, target_type: &str) -> Option<proc_macro2::TokenStream> {
    if let TraitItem::Method(m) = item {
        if m.default.is_some() {
            None
        } else {
            check_method(&m.sig, target_type);
            let type_ident = Ident::new(target_type, Span::call_site());
            let fields = extract_fields(&m.sig);
            let conditions = fields.into_iter().map(|f| match f {
                Operation::Equals(f, ty) => {
                    let par_ident = Ident::new(&f, Span::call_site());
                    let to_call = format!("field_{}_{}", f, ty.to_lowercase());
                    let filter_ident = Ident::new(&to_call, Span::call_site());
                    quote! {
                        #type_ident::#filter_ident(&mut builder, #par_ident);
                    }
                }
                Operation::Range(f, ty) => {
                    let par_ident = Ident::new(&f, Span::call_site());
                    let to_call = format!("field_{}_{}_range", f, ty.to_lowercase());
                    let filter_ident = Ident::new(&to_call, Span::call_site());
                    quote! {
                        #type_ident::#filter_ident(&mut builder, #par_ident);
                    }
                }
            });
            let sign = m.sig.clone();
            Some(quote! {
                #sign {
                    let mut builder = structsy::StructsyQuery::new_filter(self);
                    #( #conditions)*
                    Ok(structsy::StructsyQuery::into_iter(self, builder))
                }
            })
        }
    } else {
        panic!("support only methods in a trait");
    }
}

#[proc_macro_attribute]
pub fn queries(args: proc_macro::TokenStream, original: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let parsed: Item = syn::parse(original).unwrap();
    let args: AttributeArgs = parse_macro_input!(args as AttributeArgs);
    let expeted_type = if let Some(NestedMeta::Meta(Meta::Path(x))) = args.first() {
        x.segments
            .last()
            .expect(" queries has the type as argument ")
            .ident
            .to_string()
    } else {
        panic!("queries expect the type as argument");
    };
    let name;
    let mut methods = Vec::<proc_macro2::TokenStream>::new();
    match parsed.clone() {
        Item::Trait(tr) => {
            name = tr.ident.clone();
            for iten in tr.items {
                if let Some(meth_impl) = impl_trait_methods(iten, &expeted_type) {
                    methods.push(meth_impl);
                }
            }
        }
        _ => panic!("not a trait"),
    }
    let expeted_type_ident = Ident::new(&expeted_type, Span::call_site());
    let gen = quote! {
        #parsed

        impl <Q: structsy::StructsyQuery<#expeted_type_ident>>  #name for Q {
            #( #methods )*
        }
    };
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
                        structsy::FieldDescription::new::<#ty<#x<#z>>>(#pos,#field_name,#indexed),
                    }
                }
                (Some(x), None) => {
                    quote! {
                        structsy::FieldDescription::new::<#ty<#x>>(#pos,#field_name,#indexed),
                    }
                }
                (None, None) => {
                    quote! {
                        structsy::FieldDescription::new::<#ty>(#pos,#field_name,#indexed),
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
                let fields  = [
                    #( #fields_meta )*
                ];
                structsy::StructDescription::new(#struct_name,&fields)
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

fn allowed_filter_types(field: &FieldInfo) -> bool {
    let (t, r) = match (field.template_ty.clone(), field.sub_template_ty.clone()) {
        (Some(r), Some(z)) => (z, Some(r.clone())),
        (Some(x), None) => (x, Some(field.ty.clone())),
        (None, None) => (field.ty.clone(), None),
        (None, Some(_x)) => panic!(""),
    };
    match t.to_string().as_str() {
        "String" | "u8" | "u16" | "u32" | "u64" | "u128" | "i8" | "i16" | "i32" | "i64" | "i128" | "f32" | "f64"
        | "bool" => true,
        _ => match r.map(|x| x.to_string()).unwrap_or(String::from("_")).as_str() {
            "Ref" => true,
            _ => false,
        },
    }
}

fn filter_tokens(name: &Ident, fields: &Vec<FieldInfo>) -> TokenStream {
    let methods: Vec<TokenStream> = fields
        .iter()
        .filter(|x| allowed_filter_types(x))
        .map(|field| {
            let field_name = field.name.to_string();
            let field_ident = field.name.clone();
            let ty = field.ty.clone();
            match (field.template_ty.clone(), field.sub_template_ty.clone()) {
                (Some(x), Some(z)) => {
                    let method_ident = Ident::new(&format!("field_{}", field_name), Span::call_site());
                    quote! {
                        pub fn #method_ident(builder:&mut structsy::FilterBuilder<#name>,v:#ty<#x<#z>>) {
                        }
                    }
                }
                (Some(x), None) => {
                    if ty.to_string() == "Ref" {
                        let method_ident = Ident::new(
                            &format!("field_{}_{}", field_name, ty.to_string().to_lowercase()),
                            Span::call_site(),
                        );
                        let condition_method = Ident::new("simple_condition", Span::call_site());
                        quote! {
                            pub fn #method_ident(builder:&mut structsy::FilterBuilder<#name>,v:#ty<#x>){
                                builder.#condition_method(#field_name,v,|x|&x.#field_ident);
                            }
                        }
                    } else {
                        let method_ident = Ident::new(
                            &format!("field_{}_{}", field_name, ty.to_string().to_lowercase()),
                            Span::call_site(),
                        );
                        let method_ident_contains = Ident::new(
                            &format!("field_{}_{}", field_name, x.to_string().to_lowercase()),
                            Span::call_site(),
                        );
                        if x.to_string() == "bool" {
                            let condition_method_name = format!("simple_{}_condition", ty.to_string().to_lowercase());
                            let condition_method_name_contains = format!("simple_{}_single_condition", ty.to_string().to_lowercase());
                            let condition_method = Ident::new(&condition_method_name, Span::call_site());
                            let condition_method_contains = Ident::new(&condition_method_name_contains,Span::call_site());
                            quote! {
                                pub fn #method_ident(builder:&mut structsy::FilterBuilder<#name>,v:#ty<#x>){
                                    builder.#condition_method(#field_name,v,|x|&x.#field_ident);
                                }
                                pub fn #method_ident_contains(builder:&mut structsy::FilterBuilder<#name>,v:#x){
                                    builder.#condition_method_contains(#field_name,v,|x|&x.#field_ident);
                                }
                            }
                        } else {
                            let condition_method_name = format!("indexable_{}_condition", ty.to_string().to_lowercase());
                            let condition_method_name_contains = format!("indexable_{}_single_condition", ty.to_string().to_lowercase());
                            let condition_method = Ident::new(&condition_method_name, Span::call_site());
                            let condition_method_contains = Ident::new(&condition_method_name_contains,Span::call_site());
                            let range_single_method = Ident::new(&format!("indexable_{}_single_range", ty.to_string().to_lowercase()), Span::call_site());
                            let method_range_single_ident = Ident::new(&format!("field_{}_{}_range", field_name, x.to_string().to_lowercase()),Span::call_site());
                            let range_method = Ident::new(&format!("indexable_{}_range", ty.to_string().to_lowercase()), Span::call_site());
                            let method_range_ident = Ident::new(&format!("field_{}_{}_range", field_name, ty.to_string().to_lowercase()),Span::call_site());
                            quote! {
                                pub fn #method_ident(builder:&mut structsy::FilterBuilder<#name>,v:#ty<#x>){
                                    builder.#condition_method(#field_name,v,|x|&x.#field_ident);
                                }
                                pub fn #method_ident_contains(builder:&mut structsy::FilterBuilder<#name>,v:#x){
                                    builder.#condition_method_contains(#field_name,v,|x|&x.#field_ident);
                                }
                                pub fn #method_range_single_ident< R: std::ops::RangeBounds<#x> >(builder:&mut structsy::FilterBuilder<#name>,v:R){
                                    builder.#range_single_method(#field_name,v,|x|&x.#field_ident);
                                }
                                pub fn #method_range_ident< R: std::ops::RangeBounds<#ty<#x>>>(builder:&mut structsy::FilterBuilder<#name>,v:R){
                                    builder.#range_method(#field_name,v,|x|&x.#field_ident);
                                }
                            }
                        }
                    }
                }
                (None, None) => {
                    let method_ident = Ident::new(
                        &format!("field_{}_{}", field_name, ty.to_string().to_lowercase()),
                        Span::call_site(),
                        );
                    if ty.to_string() == "bool" {
                        let condition_method = Ident::new("simple_condition", Span::call_site());
                        quote! {
                            pub fn #method_ident(builder:&mut structsy::FilterBuilder<#name>,v:#ty){
                                builder.#condition_method(#field_name,v,|x|&x.#field_ident);
                            }
                        }
                    } else {
                        let condition_method = Ident::new("indexable_condition", Span::call_site());

                        let method_range_ident = Ident::new(
                            &format!("field_{}_{}_range", field_name, ty.to_string().to_lowercase()),
                            Span::call_site(),
                            );
                        quote! {
                            pub fn #method_ident(builder:&mut structsy::FilterBuilder<#name>,v:#ty){
                                builder.#condition_method(#field_name,v,|x|&x.#field_ident);
                            }
                            pub fn #method_range_ident< R: std::ops::RangeBounds<#ty>>(builder:&mut structsy::FilterBuilder<#name>,v:R){
                                builder.indexable_range(#field_name,v,|x|&x.#field_ident);
                            }
                        }
                    }
                }
                (None, Some(_x)) => panic!(""),
            }
        })
    .collect();

    quote! {
        #( #methods )*
    }
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

    fn to_tokens(&self) -> TokenStream {
        let name = &self.ident;
        let fields = self.field_infos();
        let (desc, ser) = serialization_tokens(name, &fields);
        let (indexes, impls) = indexes_tokens(name, &fields);
        let filters = filter_tokens(name, &fields);
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

            impl #name {
                #impls
                #filters
            }
        }
    }

    fn to_embedded_tokens(&self) -> TokenStream {
        let name = &self.ident;
        let fields = self.field_infos();
        let (desc, ser) = serialization_tokens(name, &fields);

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
