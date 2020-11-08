use structsy::{Structsy, StructsyTx};
use structsy_derive::{Persistent, Projection};

#[derive(Persistent)]
struct User {
    name: String,
    password: String,
}
impl User {
    fn new(user: &str, password: &str) -> User {
        User {
            name: user.to_string(),
            password: password.to_string(),
        }
    }
}

#[derive(Projection, Debug)]
#[projection = "User"]
struct UserProjection {
    name: String,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Structsy::open("prjections.stry")?;
    db.define::<User>()?;
    let mut tx = db.begin()?;
    tx.insert(&User::new("user", "pwd"))?;
    tx.insert(&User::new("other_user", "pwd"))?;
    tx.commit()?;
    let iter = db.query::<User>().projection::<UserProjection>().into_iter();
    for p in iter {
        println!("{:?}", p);
    }
    Ok(())
}
