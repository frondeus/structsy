use proc_macro2::Span;
use quote::quote;
use std::borrow::Borrow;
use syn::{
    AttributeArgs, FnArg, GenericArgument, GenericParam, Ident, Item, Meta, NestedMeta, Pat, PathArguments, ReturnType,
    Signature, TraitItem, Type,
};
enum Operation {
    Order(String),
    Equals(String),
    Query(String),
    Range(String),
}

fn extract_fields(s: &Signature) -> Vec<Operation> {
    let mut res = Vec::new();
    let mapping = s
        .generics
        .params
        .iter()
        .filter(|p| if let GenericParam::Type(_) = p { true } else { false })
        .filter_map(|p| {
            if let GenericParam::Type(t) = p {
                if !t.bounds.is_empty() {
                    return Some(t.ident.clone());
                }
            }
            None
        })
        .collect::<Vec<_>>();

    let mut inps = s.inputs.iter();
    // Skip self argument checked in check_method
    inps.next();
    while let Some(FnArg::Typed(f)) = inps.next() {
        let name = if let Pat::Ident(ref i) = &*f.pat {
            Some(i.ident.to_string())
        } else {
            None
        };
        let ty = if let Type::Path(t) = &*f.ty {
            let t = t.path.segments.last().unwrap();
            if let PathArguments::AngleBracketed(p) = &t.arguments {
                if let Some(GenericArgument::Type(Type::Path(pt))) = p.args.first() {
                    let last_pt = pt.path.segments.last().map(|x| x.ident.to_string());
                    Some((t.ident.to_string(), last_pt))
                } else {
                    Some((t.ident.to_string(), None))
                }
            } else {
                Some((t.ident.to_string(), None))
            }
        } else if let Type::Reference(t) = &*f.ty {
            if let Type::Path(nt) = &*t.elem {
                let last = nt.path.segments.last().unwrap().ident.to_string();
                if last == "str" {
                    Some((last, None))
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        };
        let mut range = false;
        for n in &mapping {
            if let (Some(nam), Some(rt)) = (&name, &ty) {
                if n == &rt.0 {
                    res.push(Operation::Range(nam.clone()));
                    range = true;
                }
            }
        }
        if !range {
            if let (Some(n), Some(t)) = (name, ty) {
                if t.0 == "Order" {
                    res.push(Operation::Order(n));
                } else if t.0 == "EmbeddedFilter" || t.0 == "StructsyQuery" || t.0 == "SnapshotQuery" || t.0 == "Filter"
                {
                    res.push(Operation::Query(n));
                } else {
                    res.push(Operation::Equals(n));
                }
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
            if name != "Self" {
                panic!("only allowed return type is 'Self' ");
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
    let not_suported = s
        .generics
        .params
        .iter()
        .filter(|x| {
            if let GenericParam::Type(t) = x {
                if !t.bounds.is_empty() {
                    return false;
                }
            } else if let GenericParam::Lifetime(_) = x {
                return false;
            }
            true
        })
        .collect::<Vec<_>>();
    if !not_suported.is_empty() {
        panic!("generics not supported {:?}", not_suported);
    }
}

fn impl_trait_methods(item: TraitItem, target_type: &str, embedded: bool) -> Option<proc_macro2::TokenStream> {
    if let TraitItem::Method(m) = item {
        if m.default.is_some() {
            None
        } else {
            check_method(&m.sig, target_type);
            let type_ident = Ident::new(target_type, Span::call_site());
            let fields = extract_fields(&m.sig);
            let conditions_count = fields
                .iter()
                .filter(|f| match f {
                    Operation::Order(_) => false,
                    _ => true,
                })
                .count();
            let conditions = fields.into_iter().map(|f| match f {
                Operation::Order(f) => {
                    let par_ident = Ident::new(&f, Span::call_site());
                    let field_access_ident = Ident::new(&format!("field_{}",f), Span::call_site());
                    quote! {
                        structsy::internal::OrderAction::order((#type_ident::#field_access_ident(), self.filter_builder()), #par_ident);
                    }
                }
                Operation::Equals(f) => {
                    let par_ident = Ident::new(&f, Span::call_site());
                    let field_access_ident = Ident::new(&format!("field_{}",f), Span::call_site());
                    quote! {
                        structsy::internal::EqualAction::equal((#type_ident::#field_access_ident(), filter.filter_builder()), #par_ident);
                    }
                }
                Operation::Range(f) => {
                    let par_ident = Ident::new(&f, Span::call_site());
                    let field_access_ident = Ident::new(&format!("field_{}",f), Span::call_site());
                    quote! {
                        structsy::internal::RangeAction::range((#type_ident::#field_access_ident(), filter.filter_builder()), #par_ident);
                    }
                }
                Operation::Query(f) => {
                    let par_ident = Ident::new(&f, Span::call_site());
                    let field_access_ident = Ident::new(&format!("field_{}",f), Span::call_site());
                    quote! {
                        structsy::internal::QueryAction::query((#type_ident::#field_access_ident(), filter.filter_builder()), #par_ident);
                    }
                }
            });
            let mut sign = m.sig;
            if let Some(f) = sign.inputs.first_mut() {
                *f = syn::parse_str::<FnArg>("mut self").expect("mut self parse correctly");
            }
            if conditions_count > 1 {
                if embedded {
                    Some(quote! {
                        #sign {
                            use structsy::internal::EmbeddedQuery;
                            let mut filter = structsy::Filter::<#type_ident>::new();
                            #( #conditions)*
                            self.add_group(filter);
                            self
                        }
                    })
                } else {
                    Some(quote! {
                        #sign {
                            use structsy::internal::Query;
                            let mut filter = structsy::Filter::<#type_ident>::new();
                            #( #conditions)*
                            self.add_group(filter);
                            self
                        }
                    })
                }
            } else {
                Some(quote! {
                    #sign {
                        let filter = &mut self;
                        #( #conditions)*
                        self
                    }
                })
            }
        }
    } else {
        panic!("support only methods in a trait");
    }
}

pub fn persistent_queries(parsed: Item, args: AttributeArgs, embedded: bool) -> proc_macro2::TokenStream {
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
                if let Some(meth_impl) = impl_trait_methods(iten, &expeted_type, embedded) {
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

            impl <Q:structsy::internal::EmbeddedQuery<#expeted_type_ident>>  #name for Q {
                #( #methods )*
            }
        }
    } else {
        quote! {
            #parsed

            impl <Q:structsy::internal::Query<#expeted_type_ident>> #name for Q {
                #( #methods )*
            }
        }
    }
}
