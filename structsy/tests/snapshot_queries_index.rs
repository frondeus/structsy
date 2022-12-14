use std::ops::RangeBounds;
use structsy::{SRes, Structsy, StructsyTx};
use structsy_derive::{queries, Persistent};
use tempfile::tempdir;

fn structsy_inst(name: &str, test: fn(db: &Structsy) -> SRes<()>) {
    let dir = tempdir().expect("can make a tempdir");
    let file = dir.path().join(format!("{}.stry", name));

    let db = Structsy::open(&file).expect("can open just create");
    test(&db).expect("test is fine");
}

#[derive(Persistent)]
struct Basic {
    #[index(mode = "cluster")]
    name: String,
}
impl Basic {
    fn new(name: &str) -> Basic {
        Basic { name: name.to_string() }
    }
}

#[queries(Basic)]
trait BasicQuery {
    fn by_name(self, name: String) -> Self;
    fn by_name_str(self, name: &str) -> Self;
    fn by_range<R: RangeBounds<String>>(self, name: R) -> Self;
    fn by_range_str<'a, R: RangeBounds<&'a str>>(self, name: R) -> Self;
}

#[test]
pub fn basic_query() {
    structsy_inst("basic_query", |db| {
        db.define::<Basic>()?;
        let mut tx = db.begin()?;
        tx.insert(&Basic::new("aaa"))?;
        tx.commit()?;
        let snapshot = db.snapshot()?;
        let count = snapshot.query::<Basic>().into_iter().count();
        assert_eq!(count, 1);
        let count = snapshot.query::<Basic>().by_name("aaa".to_string()).into_iter().count();
        assert_eq!(count, 1);
        let count = snapshot.query::<Basic>().by_name_str("aaa").into_iter().count();
        assert_eq!(count, 1);
        Ok(())
    });
}

#[test]
pub fn basic_range_query() {
    structsy_inst("basic_query", |db| {
        db.define::<Basic>()?;
        let mut tx = db.begin()?;
        tx.insert(&Basic::new("aaa"))?;
        tx.insert(&Basic::new("bbb"))?;
        tx.insert(&Basic::new("ccc"))?;
        tx.commit()?;
        let snapshot = db.snapshot()?;
        let count = snapshot
            .query::<Basic>()
            .by_range("aaa".to_string().."bbb".to_string())
            .into_iter()
            .count();
        assert_eq!(count, 1);
        let result = snapshot
            .query::<Basic>()
            .by_range("aaa".to_string().."ccc".to_string())
            .into_iter()
            .collect::<Vec<_>>();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].1.name, "aaa".to_string());
        assert_eq!(result[1].1.name, "bbb".to_string());
        let result = snapshot
            .query::<Basic>()
            .by_range("aaa".to_string()..="ccc".to_string())
            .into_iter()
            .collect::<Vec<_>>();
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].1.name, "aaa".to_string());
        assert_eq!(result[1].1.name, "bbb".to_string());
        assert_eq!(result[2].1.name, "ccc".to_string());
        Ok(())
    });
}

#[test]
pub fn basic_range_query_str() {
    structsy_inst("basic_query", |db| {
        db.define::<Basic>()?;
        let mut tx = db.begin()?;
        tx.insert(&Basic::new("aaa"))?;
        tx.insert(&Basic::new("bbb"))?;
        tx.insert(&Basic::new("ccc"))?;
        tx.commit()?;
        let snapshot = db.snapshot()?;
        let count = db.query::<Basic>().by_range_str("aaa".."bbb").into_iter().count();
        assert_eq!(count, 1);
        let result = snapshot
            .query::<Basic>()
            .by_range_str("aaa".."ccc")
            .into_iter()
            .collect::<Vec<_>>();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].1.name, "aaa".to_string());
        assert_eq!(result[1].1.name, "bbb".to_string());
        let result = snapshot
            .query::<Basic>()
            .by_range_str("aaa"..="ccc")
            .into_iter()
            .collect::<Vec<_>>();
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].1.name, "aaa".to_string());
        assert_eq!(result[1].1.name, "bbb".to_string());
        assert_eq!(result[2].1.name, "ccc".to_string());
        Ok(())
    });
}

#[derive(Persistent)]
struct BasicVec {
    #[index(mode = "cluster")]
    names: Vec<String>,
}
impl BasicVec {
    fn new(vals: &[String]) -> BasicVec {
        BasicVec { names: Vec::from(vals) }
    }
}

#[queries(BasicVec)]
trait BasicVecQuery {
    fn by_single_name(self, names: String) -> Self;
    fn by_name(self, names: Vec<String>) -> Self;
    fn by_single_range<R: RangeBounds<String>>(self, names: R) -> Self;
    fn by_range<R: RangeBounds<Vec<String>>>(self, names: R) -> Self;

    fn by_single_name_str(self, names: &str) -> Self;
    /*
    fn by_name_str(self, names: Vec<&str>) -> Self;
    fn by_single_range_str<'a, R: RangeBounds<&'a str>>(self, names: R) -> Self;
    fn by_range_str<'a, R: RangeBounds<Vec<&'a str>>>(self, names: R) -> Self;
    */
}

#[test]
pub fn basic_vec_query() {
    structsy_inst("basic_vec_query", |db| {
        db.define::<BasicVec>()?;
        let mut tx = db.begin()?;
        let data = vec!["aaa".to_string()];
        tx.insert(&BasicVec::new(&data))?;
        let datab = vec!["bbb".to_string()];
        tx.insert(&BasicVec::new(&datab))?;
        tx.commit()?;
        let snapshot = db.snapshot()?;
        let count = snapshot.query::<BasicVec>().by_name(data).into_iter().count();
        assert_eq!(count, 1);
        let count = snapshot
            .query::<BasicVec>()
            .by_single_name(String::from("aaa"))
            .into_iter()
            .count();
        assert_eq!(count, 1);
        let count = snapshot
            .query::<BasicVec>()
            .by_single_name_str("aaa")
            .into_iter()
            .count();
        assert_eq!(count, 1);
        Ok(())
    });
}

#[test]
pub fn basic_vec_range_query() {
    structsy_inst("basic_vec_range_query", |db| {
        db.define::<BasicVec>()?;
        let mut tx = db.begin()?;
        let dataa = vec!["aaa".to_string()];
        let datab = vec!["bbb".to_string()];
        let datac = vec!["ccc".to_string()];
        tx.insert(&BasicVec::new(&dataa))?;
        tx.insert(&BasicVec::new(&datab))?;
        tx.insert(&BasicVec::new(&datac))?;
        tx.commit()?;
        let snapshot = db.snapshot()?;
        let count = snapshot
            .query::<BasicVec>()
            .by_range(dataa.clone()..datab.clone())
            .into_iter()
            .count();
        assert_eq!(count, 1);
        let result = snapshot
            .query::<BasicVec>()
            .by_range(dataa.clone()..datac.clone())
            .into_iter()
            .collect::<Vec<_>>();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].1.names[0], "aaa".to_string());
        assert_eq!(result[1].1.names[0], "bbb".to_string());
        let result = snapshot
            .query::<BasicVec>()
            .by_range(dataa.clone()..=datac.clone())
            .into_iter()
            .collect::<Vec<_>>();
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].1.names[0], "aaa".to_string());
        assert_eq!(result[1].1.names[0], "bbb".to_string());
        assert_eq!(result[2].1.names[0], "ccc".to_string());
        Ok(())
    });
}

#[test]
pub fn basic_vec_sinble_range_query() {
    structsy_inst("basic_vec_range_query", |db| {
        db.define::<BasicVec>()?;
        let mut tx = db.begin()?;
        let aaa = "aaa".to_string();
        let bbb = "bbb".to_string();
        let ccc = "ccc".to_string();
        let dataa = vec![aaa.clone()];
        let datab = vec![bbb.clone()];
        let datac = vec![ccc.clone()];
        tx.insert(&BasicVec::new(&dataa))?;
        tx.insert(&BasicVec::new(&datab))?;
        tx.insert(&BasicVec::new(&datac))?;
        tx.commit()?;
        let snapshot = db.snapshot()?;
        let count = snapshot
            .query::<BasicVec>()
            .by_single_range(aaa.clone()..bbb.clone())
            .into_iter()
            .count();
        assert_eq!(count, 1);
        let result = snapshot
            .query::<BasicVec>()
            .by_single_range(aaa.clone()..ccc.clone())
            .into_iter()
            .collect::<Vec<_>>();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].1.names[0], "aaa".to_string());
        assert_eq!(result[1].1.names[0], "bbb".to_string());
        let result = snapshot
            .query::<BasicVec>()
            .by_single_range(aaa.clone()..=ccc.clone())
            .into_iter()
            .collect::<Vec<_>>();
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].1.names[0], "aaa".to_string());
        assert_eq!(result[1].1.names[0], "bbb".to_string());
        assert_eq!(result[2].1.names[0], "ccc".to_string());
        Ok(())
    });
}

#[derive(Persistent)]
struct BasicOption {
    #[index(mode = "cluster")]
    name: Option<String>,
}
impl BasicOption {
    fn new(name: Option<String>) -> BasicOption {
        BasicOption { name }
    }
}

#[queries(BasicOption)]
trait BasicOptionQuery {
    fn by_single_name(self, name: String) -> Self;
    fn by_name(self, name: Option<String>) -> Self;
    fn by_single_range<R: RangeBounds<String>>(self, name: R) -> Self;
    fn by_range<R: RangeBounds<Option<String>>>(self, name: R) -> Self;
    fn by_single_name_str(self, name: &str) -> Self;

    /*
    //TODO: support in futures also this cases
    fn by_name_str(self, name: Option<&str>) -> Self;
    fn by_single_range_str<'a,R: RangeBounds<&'a str>>(self, name: R) -> Self;
    fn by_range_str<'a, R: RangeBounds<Option<&'a str>>>(self, name: R) -> Self;
    */
}

#[test]
pub fn basic_option_query() {
    structsy_inst("basic_option_query", |db| {
        db.define::<BasicOption>()?;
        let mut tx = db.begin()?;
        let data = Some("aaa".to_string());
        tx.insert(&BasicOption::new(data.clone()))?;
        tx.commit()?;
        let snapshot = db.snapshot()?;
        let count = snapshot.query::<BasicOption>().by_name(data).into_iter().count();
        assert_eq!(count, 1);
        Ok(())
    });
}

#[test]
pub fn basic_option_none_query() {
    structsy_inst("basic_option_query", |db| {
        db.define::<BasicOption>()?;
        let mut tx = db.begin()?;
        let data = Some("aaa".to_string());
        tx.insert(&BasicOption::new(data.clone()))?;
        let datab = Some("bbb".to_string());
        tx.insert(&BasicOption::new(datab.clone()))?;
        tx.insert(&BasicOption::new(None))?;
        tx.commit()?;
        let snapshot = db.snapshot()?;
        let count = snapshot.query::<BasicOption>().by_name(data).into_iter().count();
        assert_eq!(count, 1);
        let count = snapshot.query::<BasicOption>().by_name(None).into_iter().count();
        assert_eq!(count, 1);
        let count = snapshot
            .query::<BasicOption>()
            .by_single_name(String::from("aaa"))
            .into_iter()
            .count();
        assert_eq!(count, 1);
        let count = snapshot
            .query::<BasicOption>()
            .by_single_name_str("aaa")
            .into_iter()
            .count();
        assert_eq!(count, 1);
        Ok(())
    });
}

#[test]
pub fn basic_option_range_query() {
    structsy_inst("basic_option_range_query", |db| {
        db.define::<BasicOption>()?;
        let mut tx = db.begin()?;
        let dataa = Some("aaa".to_string());
        let datab = Some("bbb".to_string());
        let datac = Some("ccc".to_string());
        tx.insert(&BasicOption::new(dataa.clone()))?;
        tx.insert(&BasicOption::new(datab.clone()))?;
        tx.insert(&BasicOption::new(datac.clone()))?;
        tx.commit()?;
        let snapshot = db.snapshot()?;
        let count = snapshot
            .query::<BasicOption>()
            .by_range(dataa.clone()..datab.clone())
            .into_iter()
            .count();
        assert_eq!(count, 1);
        let result = snapshot
            .query::<BasicOption>()
            .by_range(dataa.clone()..datac.clone())
            .into_iter()
            .collect::<Vec<_>>();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].1.name, Some("aaa".to_string()));
        assert_eq!(result[1].1.name, Some("bbb".to_string()));
        let result = snapshot
            .query::<BasicOption>()
            .by_range(dataa.clone()..=datac.clone())
            .into_iter()
            .collect::<Vec<_>>();
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].1.name, Some("aaa".to_string()));
        assert_eq!(result[1].1.name, Some("bbb".to_string()));
        assert_eq!(result[2].1.name, Some("ccc".to_string()));
        Ok(())
    });
}

#[test]
pub fn basic_option_range_single_query() {
    structsy_inst("basic_option_range_query", |db| {
        db.define::<BasicOption>()?;
        let mut tx = db.begin()?;
        let aaa = "aaa".to_string();
        let bbb = "bbb".to_string();
        let ccc = "ccc".to_string();
        let dataa = Some(aaa.clone());
        let datab = Some(bbb.clone());
        let datac = Some(ccc.clone());
        tx.insert(&BasicOption::new(dataa.clone()))?;
        tx.insert(&BasicOption::new(datab.clone()))?;
        tx.insert(&BasicOption::new(datac.clone()))?;
        tx.commit()?;
        let snapshot = db.snapshot()?;
        let count = snapshot
            .query::<BasicOption>()
            .by_single_range(aaa.clone()..bbb.clone())
            .into_iter()
            .count();
        assert_eq!(count, 1);
        let result = snapshot
            .query::<BasicOption>()
            .by_single_range(aaa.clone()..ccc.clone())
            .into_iter()
            .collect::<Vec<_>>();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].1.name, Some("aaa".to_string()));
        assert_eq!(result[1].1.name, Some("bbb".to_string()));
        let result = snapshot
            .query::<BasicOption>()
            .by_single_range(aaa.clone()..=ccc.clone())
            .into_iter()
            .collect::<Vec<_>>();
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].1.name, Some("aaa".to_string()));
        assert_eq!(result[1].1.name, Some("bbb".to_string()));
        assert_eq!(result[2].1.name, Some("ccc".to_string()));
        Ok(())
    });
}

#[test]
pub fn basic_option_range_none_query() {
    structsy_inst("basic_option_range_query", |db| {
        db.define::<BasicOption>()?;
        let mut tx = db.begin()?;
        let dataa = Some("aaa".to_string());
        let datab = Some("bbb".to_string());
        tx.insert(&BasicOption::new(dataa.clone()))?;
        tx.insert(&BasicOption::new(datab.clone()))?;
        tx.insert(&BasicOption::new(None))?;
        tx.commit()?;
        let snapshot = db.snapshot()?;
        let result = snapshot
            .query::<BasicOption>()
            .by_range(dataa.clone()..None)
            .into_iter()
            .collect::<Vec<_>>();
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].1.name, Some("aaa".to_string()));
        assert_eq!(result[1].1.name, Some("bbb".to_string()));
        assert_eq!(result[2].1.name, None);
        let result = snapshot
            .query::<BasicOption>()
            .by_range(dataa.clone()..=None)
            .into_iter()
            .collect::<Vec<_>>();
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].1.name, Some("aaa".to_string()));
        assert_eq!(result[1].1.name, Some("bbb".to_string()));
        assert_eq!(result[2].1.name, None);
        let result = snapshot
            .query::<BasicOption>()
            .by_range(datab.clone()..=None)
            .into_iter()
            .collect::<Vec<_>>();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].1.name, Some("bbb".to_string()));
        assert_eq!(result[1].1.name, None);
        let result = snapshot
            .query::<BasicOption>()
            .by_range(None..=dataa.clone())
            .into_iter()
            .collect::<Vec<_>>();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].1.name, Some("aaa".to_string()));
        assert_eq!(result[1].1.name, None);
        let result = snapshot
            .query::<BasicOption>()
            .by_range(None..datab.clone())
            .into_iter()
            .collect::<Vec<_>>();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].1.name, Some("aaa".to_string()));
        assert_eq!(result[1].1.name, None);
        Ok(())
    });
}

#[derive(Persistent)]
struct TwoFields {
    #[index(mode = "cluster")]
    name: String,
    #[index(mode = "cluster")]
    surname: String,
}
impl TwoFields {
    fn new(name: &str, surname: &str) -> TwoFields {
        TwoFields {
            name: name.to_string(),
            surname: surname.to_string(),
        }
    }
}

#[queries(TwoFields)]
trait TwoFieldsQuery {
    fn by_name(self, name: String) -> Self;
    fn by_surname(self, surname: String) -> Self;
}

#[test]
pub fn two_fileds_query() {
    structsy_inst("basic_query", |db| {
        db.define::<TwoFields>()?;
        let mut tx = db.begin()?;
        tx.insert(&TwoFields::new("aaa", "bbb"))?;
        tx.insert(&TwoFields::new("aaa", "ccc"))?;
        tx.insert(&TwoFields::new("zzz", "bbb"))?;
        tx.commit()?;
        let snapshot = db.snapshot()?;
        let count = snapshot
            .query::<TwoFields>()
            .by_name("aaa".to_string())
            .by_surname("ccc".to_string())
            .into_iter()
            .count();
        assert_eq!(count, 1);
        Ok(())
    });
}

#[derive(Persistent)]
struct TestDefault {
    #[index(mode = "cluster")]
    name: String,
}
impl TestDefault {
    fn new(name: &str) -> TestDefault {
        TestDefault { name: name.to_string() }
    }
}

#[queries(TestDefault)]
trait TestDefaultQuery: Sized {
    fn by_name(self, name: String) -> Self;

    fn find_anto(self) -> Self {
        self.by_name("anto".to_string())
    }
}

#[test]
pub fn test_default_query() {
    structsy_inst("basic_query", |db| {
        db.define::<TestDefault>()?;
        let mut tx = db.begin()?;
        tx.insert(&TestDefault::new("aaa"))?;
        tx.insert(&TestDefault::new("anto"))?;
        tx.insert(&TestDefault::new("zzz"))?;
        tx.commit()?;
        let snapshot = db.snapshot()?;
        let count = snapshot.query::<TestDefault>().find_anto().into_iter().count();
        assert_eq!(count, 1);
        Ok(())
    });
}
