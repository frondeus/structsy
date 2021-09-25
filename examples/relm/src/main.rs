use std::path::PathBuf;

use gtk::prelude::*;
use gtk::Inhibit;
use gtk::Orientation::{Horizontal, Vertical};
use relm::Widget;
use relm_derive::widget;
use relm_derive::Msg;
use structsy::{SRes, Structsy, StructsyTx};
use structsy_derive::Persistent;

// These constants stand for the columns of the listmodel and the listview
const TYPE_COL: u32 = 0;
const BRAND_COL: u32 = 1;
const SIZE_COL: u32 = 2;

#[derive(Persistent, Debug)]
struct Coffee {
    brand: String,
    size: u32,
}

impl Coffee {
    fn into_model(&self, store: &gtk::ListStore) {
        store.insert_with_values(
            None,
            &[
                (TYPE_COL, &"Coffe".to_owned()),
                (BRAND_COL, &self.brand.to_owned()),
                (SIZE_COL, &self.size.to_string()),
            ],
        );
    }
}

#[derive(Persistent, Debug)]
struct Beer {
    brand: String,
    size: u32,
}

impl Beer {
    fn into_model(&self, store: &gtk::ListStore) {
        store.insert_with_values(
            None,
            &[
                (TYPE_COL, &"Beer".to_owned()),
                (BRAND_COL, &self.brand.to_owned()),
                (SIZE_COL, &self.size.to_string()),
            ],
        );
    }
}

pub struct Config {
    path: PathBuf,
}

#[derive(Default)]
pub struct NewData {
    brand: String,
    size: u32,
    drink_type: Option<u32>,
}

pub struct Model {
    data: Structsy,
    new: NewData,
}

impl Model {
    fn new(config: Config) -> SRes<Self> {
        let data = Structsy::open(Structsy::config(config.path).create(true))?;
        data.define::<Beer>()?;
        data.define::<Coffee>()?;
        Ok(Self {
            data,
            new: Default::default(),
        })
    }

    fn create_list_model(&self) -> SRes<gtk::ListStore> {
        let model = gtk::ListStore::new(&[String::static_type(), String::static_type(), String::static_type()]);
        for (_, coffe) in self.data.scan::<Coffee>()? {
            coffe.into_model(&model);
        }
        for (_, beer) in self.data.scan::<Beer>()? {
            beer.into_model(&model);
        }
        Ok(model)
    }
    fn insert_new_interna(&self) -> SRes<()> {
        if self.new.brand.len() > 0 && self.new.size != 0 && self.new.drink_type.is_some() {
            let mut tx = self.data.begin()?;
            if self.new.drink_type.unwrap() == 0 {
                tx.insert(&Beer {
                    brand: self.new.brand.clone(),
                    size: self.new.size,
                })?;
            } else if self.new.drink_type.unwrap() == 1 {
                tx.insert(&Coffee {
                    brand: self.new.brand.clone(),
                    size: self.new.size,
                })?;
            }
            tx.commit()?;
        }
        Ok(())
    }
    fn insert_new(&self) {
        self.insert_new_interna().unwrap();
    }
    fn create_combobox_model(&self) -> gtk::ListStore {
        let store = gtk::ListStore::new(&[String::static_type()]);
        store.insert_with_values(None, &[(0, &"Beer".to_owned())]);
        store.insert_with_values(None, &[(0, &"Coffee".to_owned())]);
        store
    }
}

#[derive(Msg)]
pub enum Msg {
    // …
    Quit,
    Add,
    ChangedBrand(String),
    ChangedAmount(String),
    ChangedType(Option<u32>),
}

#[widget]
impl Widget for Application {
    fn model(config: Config) -> Model {
        Model::new(config).unwrap()
    }

    fn update(&mut self, event: Msg) {
        match event {
            Msg::Quit => gtk::main_quit(),
            Msg::Add => {
                self.model.insert_new();
                self.widgets
                    .list
                    .set_model(Some(&self.model.create_list_model().unwrap()));
            }
            Msg::ChangedBrand(s) => {
                self.model.new.brand = s;
            }
            Msg::ChangedAmount(s) => {
                if let Ok(size) = s.parse::<u32>() {
                    self.model.new.size = size;
                }
            }
            Msg::ChangedType(t) => {
                self.model.new.drink_type = t;
            }
        }
    }

    fn init_view(&mut self) {
        let column = gtk::TreeViewColumn::new();
        let cell = gtk::CellRendererText::new();
        column.pack_start(&cell, true);
        // Assiciate view's column with model's id column
        column.add_attribute(&cell, "text", 0);
        let cell = gtk::CellRendererText::new();
        column.pack_start(&cell, true);
        column.add_attribute(&cell, "text", 1);
        self.widgets.list.append_column(&column);
        let cell = gtk::CellRendererText::new();
        column.pack_start(&cell, true);
        column.add_attribute(&cell, "text", 2);
        self.widgets.list.append_column(&column);

        let cell = gtk::CellRendererText::new();
        self.widgets.drink_type.pack_start(&cell, true);
        self.widgets.drink_type.add_attribute(&cell, "text", 0);
        self.widgets
            .drink_type
            .set_model(Some(&self.model.create_combobox_model()))
    }

    view! {
        gtk::Window {
            gtk::Box {
                orientation: Vertical,
                #[name="list"]
                gtk::TreeView {
                    model: Some(&self.model.create_list_model().unwrap()),
                },
                gtk::Box {
                    orientation: Horizontal,
                    #[name="drink_type"]
                    gtk::ComboBox {
                        changed(entry) => {
                            Msg::ChangedType(entry.active())
                        },
                    },
                    gtk::Entry {
                        changed(entry) => {
                            Msg::ChangedBrand(entry.text().to_string())
                        }
                    },
                    gtk::Entry {
                        changed(entry) => {
                            Msg::ChangedAmount(entry.text().to_string())
                        }
                    },
                    gtk::Button {
                        label: "Add",
                        clicked => Msg::Add,
                    }
                },
            },
            delete_event(_, _) => (Msg::Quit, Inhibit(false)),
        }
    }
}

fn main() {
    Application::run(Config {
        path: "./data.db".into(),
    })
    .unwrap();
}
