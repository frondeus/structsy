# Structsy
[![build status](https://gitlab.com/tglman/structsy/badges/master/build.svg)](https://gitlab.com/tglman/structsy/commits/master)
[![coverage report](https://gitlab.com/tglman/structsy/badges/master/coverage.svg)](https://gitlab.com/tglman/structsy/commits/master)


*Structsy* is a simple, single file, embedded, struct database for rust, with support of transactions.

## SCOPE

This project is born with the scope to try new concepts for persist data with structs, 
still being useful for small scale standalone applications.

The first concept is implement the persistence behaviour with the use of derive macro, covering data persistence and indexing, taking inspiration from what serde does for serialization.
  
Another experimental concept is to define data access query with pure traits, of which the implementation may be automatically generated, taking inspiration from the code generation of [mockiato](https://github.com/mockiato/mockiato)

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
use structsy_derive::{queries, Persistent};

#[derive(Persistent, Debug, PartialEq)]
struct MyData {
    #[index(mode = "cluster")]
    name: String,
    address: String,
}

#[queries(MyData)]
trait MyDataQuery {
    /// The parameters name have two match the field names and type
    /// like the `address` parameter match the `address` field of the struct.
    fn search(self, address: String) -> Self;
}

fn main() -> Result<(), StructsyError> {
    let db = Structsy::open("example_basic_query.db")?;
    db.define::<MyData>()?;

    let my_data = MyData {
        name: "Structsy".to_string(),
        address: "https://gitlab.com/tglman/structsy".to_string(),
    };
    let mut tx = db.begin()?;
    let _id = tx.insert(&my_data)?;
    tx.commit()?;

    let to_find = "https://gitlab.com/tglman/structsy".to_string();
    let mut iter = db.query::<MyData>().search(to_find.clone()).into_iter();
    let (_id, data) = iter.next().unwrap();
    assert_eq!(data.address, to_find);

    Ok(())
}
```




