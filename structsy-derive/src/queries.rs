use proc_macro2::Span;
use std::borrow::Borrow;
use syn::{
    AttributeArgs, FnArg, GenericArgument, GenericParam, Ident, Item, Meta, NestedMeta, Pat, PathArguments, ReturnType,
    Signature, TraitItem, Type, TypeParamBound,
};
use quote::quote;
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
        if r.reference.is_some() {
            panic!("first argument of a method should be \"self\"");
        }
    } else {
        panic!("first argument of a method should be \"self\"");
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
            let mut sign = m.sig.clone();
            if let Some(f) = sign.inputs.first_mut() {
                *f = syn::parse_str::<FnArg>("mut self").expect("mut self parse correctly");
            }
            Some(quote! {
                #sign {
                    let mut builder = self.filter_builder();
                    #( #conditions)*
                    Ok(self)
                }
            })
        }
    } else {
        panic!("support only methods in a trait");
    }
}

pub fn persistent_queries(parsed: Item, args: AttributeArgs, embedded:bool) -> proc_macro2::TokenStream {
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
    if embedded {
    quote! {
        #parsed

        impl #name for structsy::EmbeddedFilter<#expeted_type_ident> {
            #( #methods )*
        }
    }
    } else {
    quote! {
        #parsed

        impl #name for structsy::StructsyQuery<#expeted_type_ident> {
            #( #methods )*
        }
    }
    }
}

