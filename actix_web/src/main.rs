use actix_web::{web, App, HttpRequest, HttpResponse, HttpServer, ResponseError};
use serde::{Deserialize, Serialize};
use std::io::Error as IOError;
use structsy::{Structsy, StructsyError, StructsyTx};
use structsy_derive::Persistent;

#[derive(Debug)]
enum Error {
    StructsyError(StructsyError),
    ActixError(actix_web::Error),
    IOError(IOError),
}

impl From<StructsyError> for Error {
    fn from(e: StructsyError) -> Self {
        Error::StructsyError(e)
    }
}
impl From<actix_web::Error> for Error {
    fn from(e: actix_web::Error) -> Self {
        Error::ActixError(e)
    }
}
impl From<IOError> for Error {
    fn from(e: IOError) -> Self {
        Error::IOError(e)
    }
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}
impl ResponseError for Error {
    fn error_response(&self) -> HttpResponse {
        HttpResponse::InternalServerError().body(&format!("{:?}", &self))
    }
}

#[derive(Serialize, Deserialize, Persistent)]
struct Coffee {
    brand: String,
    size: u32,
    time: String,
}

#[derive(Serialize, Deserialize)]
struct CoffeeItem {
    id: String,
    coffee: Coffee,
}

#[derive(Serialize, Deserialize)]
struct CoffeeList {
    coffees: Vec<CoffeeItem>,
}

#[derive(Serialize, Deserialize, Persistent)]
struct Beer {
    brand: String,
    size: u32,
    time: String,
}

#[derive(Serialize, Deserialize)]
struct BeerItem {
    id: String,
    beer: Beer,
}

#[derive(Serialize, Deserialize)]
struct BeerList {
    beers: Vec<BeerItem>,
}

async fn drink_coffee(
    (coffee, db): (web::Json<Coffee>, web::Data<Structsy>),
) -> Result<HttpResponse, Error> {
    let mut tx = db.begin()?;
    tx.insert(&coffee.0)?;
    tx.commit()?;
    Ok(HttpResponse::from("OK"))
}
async fn list_coffees(db: web::Data<Structsy>) -> Result<HttpResponse, Error> {
    let mut coffees = Vec::new();
    for (id, coffee) in db.scan::<Coffee>()? {
        coffees.push(CoffeeItem {
            id: id.to_string(),
            coffee,
        });
    }
    Ok(HttpResponse::Ok().json(CoffeeList { coffees }))
}
async fn update_coffee(
    (db, coffee, request): (web::Data<Structsy>, web::Json<Coffee>, HttpRequest),
) -> Result<HttpResponse, Error> {
    let p_id: structsy::Ref<Coffee> = request.match_info()["id"].parse()?;
    let mut tx = db.begin()?;
    tx.update(&p_id, &coffee.0)?;
    tx.commit()?;
    Ok(HttpResponse::from("OK"))
}
async fn delete_coffee(
    (db, request): (web::Data<Structsy>, HttpRequest),
) -> Result<HttpResponse, Error> {
    let p_id: structsy::Ref<Coffee> = request.match_info()["id"].parse()?;
    let mut tx = db.begin()?;
    tx.delete(&p_id)?;
    tx.commit()?;
    Ok(HttpResponse::from("OK"))
}

async fn drink_beer(
    (beer, db): (web::Json<Beer>, web::Data<Structsy>),
) -> Result<HttpResponse, Error> {
    let mut tx = db.begin()?;
    tx.insert(&beer.0)?;
    tx.commit()?;
    Ok(HttpResponse::from("OK"))
}
async fn list_beers(db: web::Data<Structsy>) -> Result<HttpResponse, Error> {
    let mut beers = Vec::new();
    for (id, beer) in db.scan::<Beer>()? {
        beers.push(BeerItem {
            id: id.to_string(),
            beer,
        });
    }
    Ok(HttpResponse::Ok().json(BeerList { beers }))
}
async fn update_beer(
    (db, beer, request): (web::Data<Structsy>, web::Json<Beer>, HttpRequest),
) -> Result<HttpResponse, Error> {
    let p_id: structsy::Ref<Beer> = request.match_info()["id"].parse()?;
    let mut tx = db.begin()?;
    tx.update(&p_id, &beer.0)?;
    tx.commit()?;
    Ok(HttpResponse::from("OK"))
}
async fn delete_beer(
    (db, request): (web::Data<Structsy>, HttpRequest),
) -> Result<HttpResponse, Error> {
    let p_id: structsy::Ref<Beer> = request.match_info()["id"].parse()?;
    let mut tx = db.begin()?;
    tx.delete(&p_id)?;
    tx.commit()?;
    Ok(HttpResponse::from("OK"))
}

#[actix_rt::main]
async fn main() -> Result<(), Error> {
    let structsy = Structsy::open("track.db")?;
    structsy.define::<Coffee>()?;
    structsy.define::<Beer>()?;
    let data_persistence = web::Data::new(structsy);
    HttpServer::new(move || {
        App::new()
            .data(data_persistence.clone())
            .service(
                web::scope("coffee")
                    .service(web::resource("create").route(web::post().to(drink_coffee)))
                    .service(web::resource("list").route(web::get().to(list_coffees)))
                    .service(web::resource("delete/update").route(web::post().to(update_coffee)))
                    .service(web::resource("delete/{id}").route(web::delete().to(delete_coffee))),
            )
            .service(
                web::scope("beer")
                    .service(web::resource("create").route(web::post().to(drink_beer)))
                    .service(web::resource("list").route(web::get().to(list_beers)))
                    .service(web::resource("delete/{id}").route(web::post().to(update_beer)))
                    .service(web::resource("delete/{id}").route(web::delete().to(delete_beer))),
            )
    })
    .bind("127.0.0.1:8080")?
    .run()
    .await?;
    Ok(())
}
