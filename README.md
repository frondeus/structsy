# Structsy
[![build status](https://gitlab.com/tglman/structsy/badges/master/build.svg)](https://gitlab.com/tglman/structsy/commits/master)
[![coverage report](https://gitlab.com/tglman/structsy/badges/master/coverage.svg)](https://gitlab.com/tglman/structsy/commits/master)



*Structsy* is a simple, single file, embedded, struct database for rust, with support of transactions.

## SCOPE

This project is born with the scope to try new concepts for persist data with structs, 
still being useful for small scale standalone applications.

The first concept is implement the persistence behaviour with the use of derive macro, covering data persistence and indexing, taking inspiration from what serde does for serialization.
  
Another experimental concept is to define data access query with pure traits, of witch the implementation may be automatically generated, tacking inspiration from the code generation of [mockiato](https://github.com/mockiato/mockiato)

The version 0.1 only support the basic persistence of struct, without any complex query support.

## COMPILING THE SOURCE 

Checkout the source code:

```
git clone https://gitlab.com/tglman/structsy.git
```


Compile and Test

``` 
cargo test 
```


## INSTALL

Add it as dependency of your project:

```toml
[dependencies]
structsy="0.1"
```

## USAGE EXAMPLE 

Persist a simple struct.

```rust
use structsy::{Structsy, StructsyError, StructsyTx};
use structsy_derive::Persistent;

#[derive(Persistent)]
struct MyData {
    name: String,
    address: String,
}
/// ......
let db = Structsy::open("my_data.db")?;
db.define::<MyData>()?;

let my_data = MyData {
   name: "Structsy".to_string(),
   address: "https://gitlab.com/tglman/structsy".to_string(),
};
let mut tx = db.begin()?;
tx.insert(&my_data)?;
db.commit(tx)?;
```




