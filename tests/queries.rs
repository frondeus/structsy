use structsy::{Structsy, IterResult, SRes, StructsyTx};
use structsy_derive::{Persistent,queries};
use tempfile::tempdir;
use std::ops::RangeBounds;

#[derive(Persistent)]
struct Basic {
    name:String,
}
impl Basic {
    fn new(name:&str) -> Basic {
        Basic {
            name:name.to_string(),
        }
    }
}

#[queries(Basic)]
trait BasicQuery {
    fn by_name(&self, name:String) ->  IterResult<Basic>;
    fn by_range<R:RangeBounds<String>>(&self, name:R) ->  IterResult<Basic>;
}


fn structsy_inst(name:&str, test:fn(db:&Structsy)-> SRes<()>) {
    let dir = tempdir().expect("can make a tempdir");
    let file = dir.path().join(format!("{}.stry",name));

    let db = Structsy::open(&file).expect("can open just create");
    test(&db).expect("test is fine");
}

#[test]
pub fn basic_query() {
    structsy_inst("basic_query",|db| {
        db.define::<Basic>()?;
        let mut tx = db.begin()?;
        tx.insert(&Basic::new("aaa"))?;
        db.commit(tx)?;
        let count = BasicQuery::by_name(db,"aaa".to_string())?.into_iter().count();
        assert_eq!(count,1);
        Ok(())
    });
}

#[test]
pub fn basic_range_query() {
    structsy_inst("basic_query",|db| {
        db.define::<Basic>()?;
        let mut tx = db.begin()?;
        tx.insert(&Basic::new("aaa"))?;
        tx.insert(&Basic::new("bbb"))?;
        tx.insert(&Basic::new("ccc"))?;
        db.commit(tx)?;
        let count =BasicQuery::by_range(db,"aaa".to_string().."bbb".to_string())?.into_iter().count();
        assert_eq!(count,1);
        let result =BasicQuery::by_range(db,"aaa".to_string().."ccc".to_string())?.into_iter().collect::<Vec<_>>();
        assert_eq!(result.len(),2);
        assert_eq!(result[0].1.name, "aaa".to_string());
        assert_eq!(result[1].1.name, "bbb".to_string());
        let result =BasicQuery::by_range(db,"aaa".to_string()..="ccc".to_string())?.into_iter().collect::<Vec<_>>();
        assert_eq!(result.len(),3);
        assert_eq!(result[0].1.name, "aaa".to_string());
        assert_eq!(result[1].1.name, "bbb".to_string());
        assert_eq!(result[2].1.name, "ccc".to_string());
        Ok(())
    });
}

#[derive(Persistent)]
struct BasicVec {
    names:Vec<String>,
}
impl BasicVec {
    fn new(vals:&[String]) -> BasicVec {
        BasicVec {
            names:Vec::from(vals),    
        }
    }
}

#[queries(BasicVec)]
trait BasicVecQuery {
    fn by_singe_name(&self, names:String) ->  IterResult<BasicVec>;
    fn by_name(&self, names:Vec<String>) ->  IterResult<BasicVec>;
    fn by_single_range<R:RangeBounds<String>>(&self, names:R) ->  IterResult<BasicVec>;
    fn by_range<R:RangeBounds<Vec<String>>>(&self, names:R) ->  IterResult<BasicVec>;
}

#[test]
pub fn basic_vec_query() {
    structsy_inst("basic_vec_query",|db| {
        db.define::<BasicVec>()?;
        let mut tx = db.begin()?;
        let data = vec!["aaa".to_string()];
        tx.insert(&BasicVec::new(&data))?;
        db.commit(tx)?;
        let count = BasicVecQuery::by_name(db,data)?.into_iter().count();
        assert_eq!(count,1);
        Ok(())
    });
}

#[test]
pub fn basic_vec_range_query() {
    structsy_inst("basic_vec_range_query",|db| {
        db.define::<BasicVec>()?;
        let mut tx = db.begin()?;
        let dataa = vec!["aaa".to_string()];
        let datab = vec!["bbb".to_string()];
        let datac = vec!["ccc".to_string()];
        tx.insert(&BasicVec::new(&dataa))?;
        tx.insert(&BasicVec::new(&datab))?;
        tx.insert(&BasicVec::new(&datac))?;
        db.commit(tx)?;
        let count =BasicVecQuery::by_range(db,dataa.clone()..datab.clone())?.into_iter().count();
        assert_eq!(count,1);
        let result =BasicVecQuery::by_range(db,dataa.clone()..datac.clone())?.into_iter().collect::<Vec<_>>();
        assert_eq!(result.len(),2);
        assert_eq!(result[0].1.names[0], "aaa".to_string());
        assert_eq!(result[1].1.names[0], "bbb".to_string());
        let result =BasicVecQuery::by_range(db,dataa.clone()..=datac.clone())?.into_iter().collect::<Vec<_>>();
        assert_eq!(result.len(),3);
        assert_eq!(result[0].1.names[0], "aaa".to_string());
        assert_eq!(result[1].1.names[0], "bbb".to_string());
        assert_eq!(result[2].1.names[0], "ccc".to_string());
        Ok(())
    });
}
