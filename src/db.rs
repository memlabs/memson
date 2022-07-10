
use std::io::{self, BufRead, Write};
use std::fs::OpenOptions;
use std::fs::File;
use std::path::Path;
use serde_json::Map;
use std::cmp::{Ord, Ordering};
use std::sync::Arc;
use std::collections::BTreeMap;
use serde::{Deserialize,Serialize};
pub use serde_json::Value as Json; 
use serde_json::Number;
use serde_json::json;


pub type Result<T> = std::result::Result<T, &'static str>;

#[derive(Clone, Debug)]
pub enum JsonVal {
    Val(Json),
    Arc(Arc<Json>),
}

impl JsonVal {
    pub fn as_ref(&self) -> &Json {
        match self {
            JsonVal::Val(val) => val,
            JsonVal::Arc(val) => val.as_ref(),
        }
    }

    pub fn into_arc(self) -> Arc<Json> {
        match self {
            JsonVal::Val(val) => Arc::new(val),
            JsonVal::Arc(val) => val,
        }
    }

    pub fn into_json(self) -> Json {
        match self {
            JsonVal::Arc(val) => val.as_ref().clone(),
            JsonVal::Val(val) => val,
        }
    }
}

fn add_numbers(x: &Number, y: &Number) -> Json {
    match (x.is_f64(), y.is_f64()) {
        (true, true) => Json::from(x.as_f64().unwrap() + y.as_f64().unwrap()),
        (true, false) => Json::from(x.as_f64().unwrap() + y.as_i64().unwrap() as f64),
        (false, true) => Json::from(x.as_i64().unwrap() as f64 + y.as_f64().unwrap()),
        (false, false) => Json::from(x.as_i64().unwrap() + y.as_i64().unwrap()),
    }
}

fn mul_numbers(x: &Number, y: &Number) -> Json {
    match (x.is_f64(), y.is_f64()) {
        (true, true) => Json::from(x.as_f64().unwrap() * y.as_f64().unwrap()),
        (true, false) => Json::from(x.as_f64().unwrap() * y.as_i64().unwrap() as f64),
        (false, true) => Json::from(x.as_i64().unwrap() as f64 * y.as_f64().unwrap()),
        (false, false) => Json::from(x.as_i64().unwrap() * y.as_i64().unwrap()),
    }
}

fn div_numbers(x: &Number, y: &Number) -> Json {
    match (x.is_f64(), y.is_f64()) {
        (true, true) => Json::from(x.as_f64().unwrap() / y.as_f64().unwrap()),
        (true, false) => Json::from(x.as_f64().unwrap() / y.as_f64().unwrap()),
        (false, true) => Json::from(x.as_f64().unwrap() / y.as_f64().unwrap()),
        (false, false) => Json::from(x.as_f64().unwrap() / y.as_f64().unwrap()),
    }
}

fn sub_numbers(x: &Number, y: &Number) -> Json {
    match (x.is_f64(), y.is_f64()) {
        (true, true) => Json::from(x.as_f64().unwrap() - y.as_f64().unwrap()),
        (true, false) => Json::from(x.as_f64().unwrap() - y.as_i64().unwrap() as f64),
        (false, true) => Json::from(x.as_i64().unwrap() as f64 - y.as_f64().unwrap()),
        (false, false) => Json::from(x.as_i64().unwrap() - y.as_i64().unwrap()),
    }
}


#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Cmd {
    #[serde(rename="+")]
    Add(Box<Cmd>, Box<Cmd>),    
    #[serde(rename="avg")]
    Avg(Box<Cmd>),   
    #[serde(rename="/")]
    Div(Box<Cmd>, Box<Cmd>),
    #[serde(rename="eval")]
    Eval(Json),
    #[serde(rename="==")]
    Eq(Box<Cmd>, Box<Cmd>),      
    #[serde(rename="first")]
    First(Box<Cmd>),            
    #[serde(rename=">=")]
    Ge(Box<Cmd>, Box<Cmd>),      
    #[serde(rename="get")]
    Get(String),
    #[serde(rename=">")]
    Gt(Box<Cmd>, Box<Cmd>),    
    #[serde(rename="if")]
    If(Box<Cmd>, Box<Cmd>, Box<Cmd>),    
    #[serde(rename="key")]
    Key(String, Box<Cmd>),
    #[serde(rename="last")]    
    Last(Box<Cmd>),  
    #[serde(rename="len")]    
    Len(Box<Cmd>),   
    #[serde(rename="<=")]    
    Le(Box<Cmd>, Box<Cmd>), 
    #[serde(rename="<")]    
    Lt(Box<Cmd>, Box<Cmd>), 
    #[serde(rename="max")]
    Max(Box<Cmd>),        
    #[serde(rename="min")]
    Min(Box<Cmd>),
    #[serde(rename="*")]
    Mul(Box<Cmd>, Box<Cmd>), 
    #[serde(rename="!=")]
    NotEq(Box<Cmd>, Box<Cmd>), 
    #[serde(rename="prod")]
    Product(Box<Cmd>), 
    #[serde(rename="prods")]
    Products(Box<Cmd>),         
    #[serde(rename="unique")]
    Unique(Box<Cmd>),            
    #[serde(rename="rm")]
    Rm(String),
    #[serde(rename="set")]
    Set(String, Box<Cmd>),
    #[serde(rename="-")]
    Sub(Box<Cmd>, Box<Cmd>),    
    #[serde(rename="sum")]
    Sum(Box<Cmd>),
    #[serde(rename="sums")]
    Sums(Box<Cmd>),      
    #[serde(rename="sql")]
    Sql(Box<Query>),
    #[serde(rename="stmt")]
    Statement(Vec<Cmd>),  
    #[serde(rename="val")]
    Val(Json),
    #[serde(rename="watch")]
    Watch(String, Box<Cmd>),
}

impl Cmd {

    fn eval_val(&self, doc: &Json) -> Json {
        match self {
            Cmd::Get(key) => get(doc, key),
            Cmd::Add(lhs, rhs) => {
                let (lhs, rhs) = (lhs.eval_val(doc), rhs.eval_val(doc));
                add(&lhs, &rhs)
            }
            Cmd::Val(Json::String(s)) => {
                if let Some(key) = s.strip_prefix('$') {
                    get(doc, key)
                } else {
                    Json::String(s.clone())
                }
            }   
            _ => unimplemented!(),
        }
    }

    fn eval_docs(&self, docs: &[Json]) -> Cmd {
        fn bin_f<F:Fn(Box<Cmd>, Box<Cmd>) -> Cmd>(f: F,lhs: Cmd, rhs: Cmd, docs: &[Json]) -> Cmd { 
            let x = Box::new(lhs.eval_docs(docs));
            let y = Box::new(rhs.eval_docs(docs));
            f(x, y) 
        }
        match self {
            Cmd::Eq(lhs, rhs) => bin_f(Cmd::Eq, lhs.as_ref().clone(), rhs.as_ref().clone(), docs),
            Cmd::NotEq(lhs, rhs) => bin_f(Cmd::NotEq, lhs.as_ref().clone(), rhs.as_ref().clone(), docs),
            Cmd::Val(Json::String(s)) => {
                if let Some(key) = s.strip_prefix('$') {
                    let val = docs.iter().map(|doc| doc.get(key).cloned().unwrap_or(Json::Null)).collect();
                    Cmd::Val(Json::Array(val))
                } else {
                    Cmd::Val(Json::String(s.clone()))
                }

            }
            _ => unimplemented!(),
        }
    }

    pub fn parse(val: Json) -> Cmd {
        match val {
            Json::Object(obj) => {
                if obj.len() == 1 {
                    let mut it = obj.into_iter();
                    let (key, val) = it.next().unwrap();
                    match key.as_str() {
                        "+" | "add" => parse_bin_cmd(Cmd::Add, val),
                        "-" | "sub" => parse_bin_cmd(Cmd::Sub, val),
                        "*" | "mul" => parse_bin_cmd(Cmd::Mul, val),
                        "/" | "div" => parse_bin_cmd(Cmd::Div, val),
                        "==" | "eq" => parse_bin_cmd(Cmd::Eq, val),
                        "!=" | "neq" => parse_bin_cmd(Cmd::NotEq, val),
                        "<" | "lt" => parse_bin_cmd(Cmd::Lt, val),
                        "<=" | "le" => parse_bin_cmd(Cmd::Le, val),
                        ">" | "gt" => parse_bin_cmd(Cmd::Gt, val),
                        ">=" | "ge" => parse_bin_cmd(Cmd::Ge, val),                        
                        "avg" => parse_unr_cmd(Cmd::Avg, val),
                        "eval" => Cmd::Eval(val),
                        "first" => parse_unr_cmd(Cmd::First, val),
                        "get" => parse_unr_str_cmd(Cmd::Get, val),
                        "if" => parse_tern_cmd(Cmd::If, val),
                        "key" => parse_key(val),
                        "last" => parse_unr_cmd(Cmd::Last, val),
                        "len" => parse_unr_cmd(Cmd::Len, val),
                        "max" => parse_unr_cmd(Cmd::Max, val),
                        "min" => parse_unr_cmd(Cmd::Min, val),
                        "rm" => parse_unr_str_cmd(Cmd::Rm, val),
                        "set" => parse_set(val),
                        "sum" => parse_unr_cmd(Cmd::Sum, val),
                        "sums" => parse_unr_cmd(Cmd::Sums, val),
                        "sql" => Cmd::Sql(Box::new(Query::parse(val))),
                        "unique" => parse_unr_cmd(Cmd::Unique, val),
                        "val" => Cmd::Val(val),
                        "watch" => parse_op(Cmd::Watch, val),
                        _ => Cmd::Val(json!({key: val}))
                    }
                } else {
                    Cmd::Val(Json::Object(obj))
                }
            }
            Json::Array(arr) => {
                let mut out = Vec::new();
                for val in arr {
                    out.push(Cmd::parse(val));
                }
                Cmd::Statement(out)
            }
            val => Cmd::Val(val),
        }
    }
}



#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Query {
    select: Option<Map<String,Json>>,
    from: Cmd,
    by: Option<Vec<Cmd>>,
    #[serde(rename="where")]
    filters: Option<Cmd>
}

impl Query {
    fn parse(val: Json) -> Query {
        match val {
            Json::Object(obj) => {
                let from = Cmd::parse(obj.get("from").cloned().unwrap());
                let filters = obj.get("where").cloned().map(Cmd::parse);
                let select = match obj.get("select") {
                    None => None,
                    Some(Json::Object(obj)) => Some(obj.clone()),
                    _ => None,
                };
                let by: Option<Vec<Cmd>> = match obj.get("by").cloned() {
                    Some(Json::Array(arr)) => Some(arr.into_iter().map(Cmd::parse).collect()),
                    _ => None,
                };
                Query{ select, from, by, filters }
            }
            _ => unimplemented!(),
        }
    }
    
    fn eval(&self, db: &mut InMemDb) -> Result<Json> {
        // evaluate from
        let docs = self.eval_from(db)?;
        // filter docs
        let docs = self.eval_where(db, docs)?;

        let val = self.eval_by(docs, db);

        Ok(match val {
            Json::Array(arr) => self.eval_select_vec(arr),
            Json::Object(obj) => self.eval_select_obj(obj),
            _ => Json::Null,
        })
    }

    fn eval_select_obj(&self, obj: Map<String, Json>) -> Json {
        Json::Object(obj)
    }

    fn eval_select_vec(&self, docs: Vec<Json>) -> Json {
        if let Some(selects) = &self.select {
            let mut out = Vec::new();
            for doc in docs {
                let mut obj = Map::new();
                for (key, select) in selects.iter() {
                    let cmd = Cmd::parse(select.clone());
                    let val = cmd.eval_val(&doc);
                    let k = if let Some(k) = key.strip_prefix('$') { k.to_string() } else { key.clone() }; 
                    obj.insert(k, val);
                }
                out.push(Json::Object(obj));
            }
            Json::Array(out)
        } else {
            Json::Array(docs)
        }
    }

    fn eval_by(&self, docs: Vec<Json>, _db: &mut InMemDb) -> Json {
        let bys = match &self.by {
            Some(bys) => bys.clone(),
            None => return Json::Array(docs),
        };
        
        let mut aggs = Map::new();

        for doc in docs {
            let by: Cmd = bys[0].clone(); 
            let key = match by.eval_val(&doc) {
                Json::String(s) => s,
                val => val.to_string(), 
            };
            let vals = aggs.entry(key.to_string()).or_insert_with(|| Json::Array(Vec::new()));
            vals.as_array_mut().unwrap().push(doc);
        }

        Json::Object(aggs)
    }

    fn eval_from(&self, db: &mut InMemDb) -> Result<Vec<Json>> {
        let val = db.eval(self.from.clone())?;
        match val.into_json() {
            Json::Array(arr) => Ok(arr),
            _ => unimplemented!(),
        }
    }

    fn eval_where(&self, db: &mut InMemDb, docs: Vec<Json>) -> Result<Vec<Json>> {
        Ok(if let Some(filter) = self.filters.clone() {
            let cmd = filter.eval_docs(&docs);
            match db.eval(cmd)? {
                JsonVal::Val(Json::Array(flags)) => {
                    let mut out = Vec::new();
                    for (flag, doc) in flags.iter().zip(docs.into_iter()) {
                        if flag.as_bool().unwrap_or(false) {
                            out.push(doc);
                        }
                    }
                    out
                }
                _ => unimplemented!(),
            }
        } else {
            docs
        })
    }
}

fn parse_unr_cmd<F: Fn(Box<Cmd>) -> Cmd>(f: F, arg: Json) -> Cmd {
    f(parse_arg(arg))
}

fn parse_bin_cmd<F: Fn(Box<Cmd>, Box<Cmd>) -> Cmd>(f: F, val: Json) -> Cmd {
    match val {
        Json::Array(mut arr) if arr.len() == 2 => {
            let rhs = parse_arg(arr.remove(1));
            let lhs = parse_arg(arr.remove(0));
            f(lhs, rhs)
        }
        val => Cmd::Val(val),
    }
}

fn parse_tern_cmd<F: Fn(Box<Cmd>, Box<Cmd>, Box<Cmd>) -> Cmd>(f: F, val: Json) -> Cmd {
    match val {
        Json::Array(mut arr) if arr.len() == 3 => {
            let z = parse_arg(arr.remove(2));
            let y = parse_arg(arr.remove(1));
            let x = parse_arg(arr.remove(0));
            f(x, y, z)
        }
        val => Cmd::Val(val),
    }
}

fn parse_arg(val: Json) -> Box<Cmd> {
    Box::new(Cmd::parse(val))
}

fn parse_unr_str_cmd<F:Fn(String) -> Cmd>(f: F, val: Json) -> Cmd {
    match val {
        Json::String(s) => f(s),
        val => Cmd::Val(val),
    }
}

fn parse_op<F:Fn(String, Box<Cmd>) -> Cmd>(f: F, val: Json) -> Cmd {
    match val {
        Json::Array(mut arr) if arr.len() == 2 => {
            let arg = arr.remove(1);
            let key = match arr.remove(0) {
                Json::String(s) => s,
                val => return Cmd::Val(Json::Array(vec![val, arg])),
            };
            f(key, parse_arg(arg))
        }
        val => Cmd::Val(val),
    }
}

fn parse_key(val: Json) -> Cmd {
    parse_op(Cmd::Key, val)
}

fn parse_set(val: Json) -> Cmd {
    parse_op(Cmd::Set, val)
}

struct Entry {
    val: Arc<Json>,
    watchers: Vec<Cmd>
}

impl Entry {
    fn from(val: Arc<Json>) -> Self {
        Self { val, watchers: Vec::new() }
    }

}

pub struct Db {
    rdb: InMemDb,
    hdb: OnDiskDb,
}

impl Db {
    pub fn open<P:AsRef<Path>>(path: P) -> io::Result<Self> {
        let mut hdb = OnDiskDb::open(path)?;
        let rdb = hdb.populate()?;
        Ok(Db { rdb, hdb })
    }

    pub fn eval(&mut self, cmd: Cmd) -> Result<JsonVal> {
        match cmd {
            Cmd::Set(key, cmd) => {
                let val = self.rdb.eval(*cmd)?;
                self.hdb.insert(&key, val.as_ref()).map_err(|_| "cannot write to hdb")?;
                let v =val.into_arc();
                match self.rdb.set(key, v) {
                    Some(val) => Ok(JsonVal::Arc(val)),
                    None => Ok(JsonVal::Val(Json::Null))
                }
                
                
            }
            _ => unimplemented!()
        }
    }
}

struct OnDiskDb {
    file: File,
}

impl OnDiskDb {
    fn open<P:AsRef<Path>>(path: P) -> std::io::Result<OnDiskDb> {
        
        let file = OpenOptions::new().append(true).create(true).read(true).open(path)?;
        Ok(OnDiskDb{ file })
    }

    fn populate(&mut self) -> io::Result<InMemDb> {
        let mut rdb = InMemDb::new();
        let mut buf = io::BufReader::new(&mut self.file);
        for line in buf.lines() {
            let (key, val): (String, Json) = serde_json::from_str(&line.unwrap()).unwrap();
            rdb.set(key, Arc::new(val));
        }
        Ok(rdb)
    }

    fn insert(&mut self, key: &str, val: &Json) -> std::io::Result<()> {
        let entry = (key, val);
        let s = serde_json::to_string(&entry).unwrap() + "\n";
        self.file.write(s.as_bytes()).map(|_| ())
    }
}


struct InMemDb {
    data: BTreeMap<String, Entry>,
}

impl InMemDb {
    fn new() -> Self {
        Self {
            data: BTreeMap::new(),
        }
    }

    fn get(&self, key: &str) -> Option<Arc<Json>> {
        self.data.get(key).map(|e| e.val.clone())
    }

    fn eval_get(&self, key: &str) -> JsonVal {
        let keys: Vec<_> = key.split('.').collect();

        match self.get(keys[0]) {
            Some(val) => {
                
                if keys.len() > 1 {
                    JsonVal::Val(gets(val.as_ref(), &keys[1..]))
                } else {
                    JsonVal::Arc(val)
                }
                
            }
            None => JsonVal::Val(Json::Null)
        }
 
    }
    
    fn set(&mut self, key: String, val: Arc<Json>) -> Option<Arc<Json>> {
        let r = if let Some(entry) = self.data.get_mut(&key) {
            let old_val = entry.val.clone();
            entry.val = val;                     
            Some(old_val)
        } else {
            self.data.insert(key.clone(), Entry::from(val));
            None
        };
        
        let entry = self.data.get(&key).unwrap();

        for watcher in entry.watchers.clone() {
            let _ = self.eval(watcher);
        }

        r
    }

    fn set_val(&mut self, key: String, arg: Cmd) -> Result<JsonVal> {
        let val = self.eval(arg)?;
        Ok(self.set(key, val.into_arc()).map(JsonVal::Arc).unwrap_or_else(|| JsonVal::Arc(Arc::new(Json::Null))))
    }    

    pub fn eval(&mut self, cmd: Cmd) -> Result<JsonVal> {
        match cmd {
            Cmd::Add(lhs, rhs) => self.eval_binary_cmd(*lhs, *rhs, add),
            Cmd::Avg(arg) => self.eval_unary_cmd(*arg, avg),
            Cmd::Div(lhs, rhs) => self.eval_binary_cmd(*lhs, *rhs, div),
            Cmd::Eval(arg) => self.eval_eval(arg),
            Cmd::Eq(lhs, rhs) => self.eval_binary_cmd(*lhs, *rhs, eq),
            Cmd::First(arg) => self.eval_unary_cmd(*arg, &first),
            Cmd::Ge(lhs, rhs) => self.eval_binary_cmd(*lhs, *rhs, ge),
            Cmd::Get(key) => Ok(self.eval_get(&key)),
            Cmd::Gt(lhs, rhs) => self.eval_binary_cmd(*lhs, *rhs, gt),
            Cmd::If(pred, lhs, rhs) => self.eval_if(*pred, *lhs, *rhs),
            Cmd::Key(key, arg) => {
                let val = self.eval(*arg)?;
                Ok(JsonVal::Val(self.eval_key(&key, val.as_ref())))
            }
            Cmd::Last(arg) => self.eval_unary_cmd(*arg, last),
            Cmd::Len(arg) => self.eval_unary_cmd(*arg, len),
            Cmd::Le(lhs, rhs) => self.eval_binary_cmd(*lhs, *rhs, le),
            Cmd::Lt(lhs, rhs) => self.eval_binary_cmd(*lhs, *rhs, lt),
            Cmd::Max(arg) => self.eval_unary_cmd(*arg, max),
            Cmd::Min(arg) => self.eval_unary_cmd(*arg, min),
            Cmd::Mul(lhs, rhs) => self.eval_binary_cmd(*lhs, *rhs, mul),
            Cmd::NotEq(lhs, rhs) => self.eval_binary_cmd(*lhs, *rhs, not_eq),
            Cmd::Product(_arg) => unimplemented!(),
            Cmd::Products(_args) => unimplemented!(),
            Cmd::Rm(key) => {
                match self.data.remove(&key) {
                    Some(entry) => Ok(JsonVal::Arc(entry.val)),
                    None => Ok(JsonVal::Val(Json::Null)),
                }
            }
            Cmd::Set(key, arg) => self.set_val(key, *arg),
            Cmd::Sub(lhs, rhs) => self.eval_binary_cmd(*lhs, *rhs, sub),
            Cmd::Sum(arg) => self.eval_unary_cmd(*arg, sum),
            Cmd::Sums(arg) => self.eval_unary_cmd(*arg, sums),
            Cmd::Sql(sql) => Ok(JsonVal::Val(sql.eval(self)?)),
            Cmd::Statement(stmt) => {
                let mut out = Vec::with_capacity(stmt.len());
                for val in stmt {
                    out.push(self.eval(val)?.into_json());
                }
                Ok(JsonVal::Val(Json::Array(out)))
            }
            Cmd::Watch(watch, cmd) => self.eval_watch(&watch, *cmd),
            Cmd::Unique(arg) => self.eval_unary_cmd(*arg, unique),
            Cmd::Val(val) => Ok(JsonVal::Val(val)),
        }
    }

    fn eval_watch(&mut self, watch: &str, cmd: Cmd) -> Result<JsonVal> {
        if let Some(entry) = self.data.get_mut(watch) {
            entry.watchers.push(cmd);
            Ok(JsonVal::Val(Json::Null))
        } else {
            Err("bad key")
        }
    }

    fn eval_key(&self, key: &str, val: &Json) -> Json {
        json_key(key, val)
    }

    fn eval_eval(&mut self, arg: Json) -> Result<JsonVal> {
        let val = self.eval(Cmd::parse(arg))?;
        let cmd = Cmd::parse(val.into_json());
        self.eval(cmd)
    }

    fn eval_unary_cmd<F:Fn(&Json) -> Json>(&mut self, arg: Cmd, f: F) -> Result<JsonVal> {
        let val = self.eval(arg)?;
        Ok(JsonVal::Val(f(val.as_ref())))
    }

    fn eval_binary_cmd<F:Fn(&Json, &Json) -> Json>(&mut self, lhs: Cmd, rhs: Cmd, f: F) -> Result<JsonVal> {
        let (x, y)  = (self.eval(lhs)?, self.eval(rhs)?);
        Ok(JsonVal::Val(f(x.as_ref(), y.as_ref())))
    }

    fn eval_if(&mut self, pred: Cmd, lhs: Cmd, rhs: Cmd) -> Result<JsonVal> {
        match self.eval(pred)?.as_ref() {
            Json::Bool(true) => self.eval(lhs),
            Json::Bool(false) => self.eval(rhs),
            _ => Err("bad type"),            
        }
    }
}

fn scalars_op<G:Fn(f64, f64) -> f64>(x: &Number, y: &Number, g: G) -> Json {
    match (x.as_f64(), y.as_f64()) {
        (Some(x), Some(y)) => Json::from(g(x, y)),
        (Some(x), None) => Json::from(g(x as f64, y.as_f64().unwrap())),
        (None, Some(y)) => Json::from(g(x.as_f64().unwrap(), y as f64)),
        (None, None) => Json::from(g(x.as_f64().unwrap(), y.as_f64().unwrap())),
    }
}

fn vec_vec_op<F:Fn(&Json, &Json) -> Json>(lhs: &[Json], rhs: &[Json], op: F) -> Json {
    Json::Array(lhs.iter().zip(rhs.iter()).map(|(x,y)| op(x, y)).collect())
}

fn vec_scalar_op<F:Fn(&Json, &Json) -> Json>(lhs: &[Json], rhs:&Json, op: F) -> Json {
    Json::Array(lhs.iter().map(|x| op(x, rhs)).collect())
} 

fn scalar_vec_op<F:Fn(&Json, &Json) -> Json>(lhs: &Json, rhs:&[Json], op: F) -> Json {
    Json::Array(rhs.iter().map(|y| op(lhs, y)).collect())
} 

fn gets(val: &Json, keys: &[&str]) -> Json {
    let mut i = 0;
    let mut out = Json::Null;
    let mut v = val;
    while i < keys.len() {
        let key = keys[i];
        out = get(v, key);
        v = &out;
        i += 1;
    }
    out
}

fn get(val: &Json, key: &str) -> Json {
    match val {
        Json::Array(arr) => Json::Array(arr.iter().map(|e| get(e, key)).collect()),
        Json::Object(obj) => obj.get(key).cloned().unwrap_or(Json::Null),
        _ => Json::Null,
    }
}

pub fn add(x: &Json, y: &Json) -> Json {
    match (x, y) {
        (Json::Number(x), Json::Number(y)) => add_numbers(x, y),
        (Json::Bool(lhs), y@Json::Number(_)) => add(&Json::from(*lhs as i64), y),
        (x@Json::Number(_), Json::Bool(y)) => add(x, &Json::from(*y as i64)),
        (Json::Array(x), Json::Array(y)) => vec_vec_op(x, y, &add),
        (Json::Array(x), y) => vec_scalar_op(x, y, &add),
        (x, Json::Array(y)) => scalar_vec_op(x, y, &add),
        (Json::String(x), Json::String(y)) => Json::String(x.clone() + y),
        (Json::String(x), Json::Number(y)) => Json::String(x.to_string() + &y.to_string()),
        (Json::Number(lhs), Json::String(rhs)) => Json::String(lhs.to_string() + rhs),
        (Json::Bool(lhs), Json::Bool(rhs)) => Json::from(*lhs as i64 + *rhs as i64),         
        _ => Json::Null
    }
}

pub fn mul(x: &Json, y: &Json) -> Json {
    match (x, y) {
        (Json::Number(x), Json::Number(y)) => mul_numbers(x, y),
        (Json::Array(x), Json::Array(y)) => vec_vec_op(x, y, &mul),
        (Json::Array(x), y) => vec_scalar_op(x, y, &mul),
        (x, Json::Array(y)) => scalar_vec_op(x, y, &mul),
        _ => Json::Null,
    }
}

pub fn div(x: &Json, y: &Json) -> Json {
    match (x, y) {
        (Json::Number(x), Json::Number(y)) => div_numbers(x, y),
        (Json::Array(x), Json::Array(y)) => vec_vec_op(x, y, &div),
        (Json::Array(x), y) => vec_scalar_op(x, y, &div),
        (x, Json::Array(y)) => scalar_vec_op(x, y, &div),
        _ => Json::Null,
    }
}

pub fn sub(x: &Json, y: &Json) -> Json {
    match (x, y) {
        (Json::Number(x), Json::Number(y)) => sub_numbers(x, y),
        (Json::Array(x), Json::Array(y)) => vec_vec_op(x, y, &sub),
        (Json::Array(x), y) => vec_scalar_op(x, y, &sub),
        (x, Json::Array(y)) => scalar_vec_op(x, y, &sub),
        _ => Json::Null
    }
}

pub fn avg(val: &Json) -> Json {
    match val {
        Json::Array(arr) => {
            let total = sum_arr(arr);
            div(&total, &Json::from(arr.len() as f64))
        }
        x@Json::Number(_) => x.clone(),
        _ => unimplemented!()
    }
}

pub fn eq(x: &Json, y: &Json) -> Json {
    match (x,y) {
        (Json::Array(x), Json::Array(y)) => Json::Array(x.iter().zip(y.iter()).map(|(x,y)| eq(x,y)).collect()),
        (Json::Array(x), y) => Json::Array(x.iter().map(|x| eq(x, y)).collect()),
        (x, Json::Array(y)) => Json::Array(y.iter().map(|y| eq(x, y)).collect()),
        (x, y) => Json::Bool(x == y)
    }
}


pub fn not_eq(x: &Json, y: &Json) -> Json {
    match (x,y) {
        (Json::Array(x), Json::Array(y)) => Json::Array(x.iter().zip(y.iter()).map(|(x,y)| not_eq(x,y)).collect()),
        (Json::Array(x), y) => Json::Array(x.iter().map(|x| not_eq(x, y)).collect()),
        (x, Json::Array(y)) => Json::Array(y.iter().map(|y| not_eq(x, y)).collect()),
        (x, y) => Json::Bool(x != y)
    }
}


//TODO (make generic)
fn num_op<F:Fn(Ordering) -> bool>(x: &Number, y: &Number, f: F) -> bool {
    match (x.as_i64(), y.as_i64()) {
        (Some(x), Some(y)) => f(x.cmp(&y)),
        (Some(x), None) => {
            let lhs = x as f64;
            let rhs: f64 = y.as_f64().unwrap();
            f(lhs.partial_cmp(&rhs).unwrap())
        }
        (None, Some(y)) => {
            let lhs = x.as_f64().unwrap();
            let rhs = y as f64;
            f(lhs.partial_cmp(&rhs).unwrap())
        }
        (None, None) => f(x.as_f64().unwrap().partial_cmp(&y.as_f64().unwrap()).unwrap())
    }
}


fn is_lt(o: Ordering) -> bool {
    o.is_lt()
}

fn is_le(o: Ordering) -> bool {
    o.is_le()
}

fn is_ge(o: Ordering) -> bool {
    o.is_ge()
}

fn is_gt(o: Ordering) -> bool {
    o.is_gt()
}

fn cmp<F:Fn(&Json, &Json) -> Json, G:Fn(Ordering) -> bool>(x: &Json, y: &Json, f: F, g: G) -> Json {
    match (x,y) {
        (Json::Array(x), Json::Array(y)) => Json::Array(x.iter().zip(y.iter()).map(|(x,y)| f(x,y)).collect()),
        (Json::Array(x), y) => Json::Array(x.iter().map(|e| f(e, y)).collect()),
        (x, Json::Array(y)) => Json::Array(y.iter().map(|e| f(x, e)).collect()),
        (Json::String(x), Json::String(y)) => Json::Bool(g(x.cmp(y))),
        (Json::Number(x), Json::Number(y)) => Json::Bool(num_op(x, y, g)),
        _ => unimplemented!()
    }    
}

fn lt(x: &Json, y: &Json) -> Json {
    cmp(x, y, lt, is_lt)
}

fn le(x: &Json, y: &Json) -> Json {
    cmp(x, y, le, is_le)
}

fn ge(x: &Json, y: &Json) -> Json {
    cmp(x, y, ge, is_ge)
}

fn gt(x: &Json, y: &Json) -> Json {
    cmp(x, y, gt, is_gt)
}

pub fn first(val: &Json) -> Json {
    match val {
        Json::Array(arr) => {
            if arr.is_empty() {
                Json::Null
            } else {
                arr[0].clone()
            }
        }
        val => val.clone(),
    }
}

fn last(val: &Json) -> Json {
    match val {
        Json::Array(arr) => {
            if arr.is_empty() {
                Json::Null
            } else {
                let pos = arr.len() - 1;
                arr[pos].clone()
            }
        }
        val => val.clone(),
    }
}

fn len(val: &Json) -> Json {
    match val {
        Json::Array(arr) => Json::from(arr.len()),
        Json::Object(obj) => Json::from(obj.len()),
        _ => Json::from(1),
    }
}

pub fn max(val: &Json) -> Json {
    match val {
        Json::Array(_arr) => {
            unimplemented!()
        }
        val => val.clone(),
    }
}

pub fn min(val: &Json) -> Json {
    match val {
        Json::Array(_arr) => {
            unimplemented!()
        }
        val => val.clone(),
    }
}

fn sum_arr(arr: &[Json]) -> Json {
    let mut sum = Json::from(0i64);
    for e in arr {
        sum = add(&sum, e);
    }
    sum
}

fn sum(val: &Json) -> Json {
    match val {
        Json::Array(arr) => sum_arr(arr),
        Json::Number(val) => Json::from(val.clone()),
        _ => Json::from(0),
    }
}

fn reduce_sum((mut vec, sum): (Vec<Json>, Json), e: &Json) -> (Vec<Json>, Json) {
    let total = add(&sum, e);
    vec.push(total.clone());
    (vec, total)
}

fn sums(val: &Json) -> Json {
    match val {
        Json::Array(arr) =>  {
            let (v, _) = arr.iter().fold((Vec::new(), Json::from(0i64)), reduce_sum);
            Json::Array(v)
        }
        Json::Number(val) => Json::Array(vec![Json::from(val.clone())]),
        _ => Json::Array(vec![Json::from(0)]),
    }
}

fn json_key(k: &str, val: &Json) -> Json {
    match val {
        Json::Array(arr) => Json::Array(arr.iter().map(|e| json_key(k, e)).collect()),
        Json::Object(obj) => obj.get(k).cloned().unwrap_or(Json::Null),
        _ => Json::Null,
    }
}

fn unique(val: &Json) -> Json {
    match val {
        Json::Array(arr) => {
            let mut set = Vec::new();
            for val in arr {
                if !set.contains(val) {
                    set.push(val.clone());
                }
            }
            Json::Array(set)
        }
        val => val.clone(),
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use approx_eq::assert_approx_eq;

    fn num(val: Json) -> Number {
        match val {
            Json::Number(n) => n,
            _ => panic!("bad json")
        }
    }

    fn assert_op<F:Fn(&Number, &Number) -> Json>(f: F, x: Json, y: Json, expected: Json) {
        assert_eq!(f(&num(x), &num(y)), expected);
    }

    fn assert_add(x: Json, y: Json, expected: Json) {
        assert_op(add_numbers, x, y, expected);
    }

    fn assert_mul(x: Json, y: Json, expected: Json) {
        assert_op(mul_numbers, x, y, expected);
    }    

    fn assert_div(x: Json, y: Json, expected: Json) {
        assert_op(div_numbers, x, y, expected);
    }        

    fn assert_sub(x: Json, y: Json, expected: Json) {
        assert_op(sub_numbers, x, y, expected);
    }        

    fn assert_nums(x: Json, y: Json) {
        assert_approx_eq!(x.as_f64().unwrap(), y.as_f64().unwrap());
    }

    #[test]
    fn add_number_ok() {
        assert_add(Json::from(1), Json::from(1), Json::from(2));
        assert_add(Json::from(1.2), Json::from(1.2), Json::from(2.4));
        assert_add(Json::from(1), Json::from(1.2), Json::from(2.2));
        assert_add(Json::from(1.2), Json::from(1), Json::from(2.2));
    }    

    #[test]
    fn mul_number_ok() {
        assert_mul(Json::from(2), Json::from(3), Json::from(6));
        assert_mul(Json::from(2), Json::from(2.2), Json::from(4.4));
        assert_mul(Json::from(2.2), Json::from(2), Json::from(4.4));
        assert_nums(mul(&Json::from(2.2), &Json::from(2.2)), Json::from(4.84));
    }   

    #[test]
    fn div_number_ok() {
        assert_div(Json::from(2), Json::from(4), Json::from(0.5));
        assert_nums(div(&Json::from(2.1), &Json::from(3)), Json::from(0.7));
        assert_nums(div(&Json::from(2), &Json::from(3.2)), Json::from(0.625));
        assert_div(Json::from(2.2), Json::from(2.2), Json::from(1.0));
    }   

    #[test]
    fn sub_number_ok() {
        assert_sub(Json::from(2), Json::from(4), Json::from(-2));
        assert_nums(sub(&Json::from(2), &Json::from(2.2)), Json::from(-0.2));
        assert_nums(sub(&Json::from(2.2), &Json::from(2)), Json::from(0.2));
        assert_nums(sub(&Json::from(2.2), &Json::from(2.1)), Json::from(0.1));
    }   

    #[test]
    fn json_add() {
        assert_eq!(add(&json!(1i64), &json!(2i64)), json!(1 + 2));
        assert_eq!(add(&json!(1.1f64), &json!(2.3)), 1.1 + 2.3);
        assert_eq!(add(&json!([1,2,3]), &json!(2)), json!([1+2,2+2,3+2]));
        assert_eq!(add(&json!(2),&json!([1,2,3])), json!([2+1,2+2,2+3]));
        assert_eq!(add(&json!("abc"),&json!("def")), json!("abcdef"));
    }

    #[test]
    fn json_avg() {
        assert_eq!(avg(&json!(1)), json!(1));
        assert_eq!(avg(&json!(1.0)), json!(1.0));
        assert_eq!(avg(&json!([1,2,3,4,5])), json!(3.0));  
        assert_eq!(avg(&json!([1.0,2.0,3.0,4.0,5.0])), json!(3.0));                
    }

    #[test]
    fn json_div() {
        assert_eq!(div(&json!(1), &json!(2)), json!(0.5));
        assert_eq!(div(&json!(1.0), &json!(2.0)), json!(0.5));
        assert_eq!(div(&json!([1,2,3]), &json!([1,2,3])), json!([1.0,1.0,1.0]));  
        assert_eq!(div(&json!(1), &json!([1,2,4])), json!([1.0, 0.5, 0.25]));                
        assert_eq!(div(&json!([1,2,4]), &json!(1)), json!([1.0, 2.0, 4.0]));
    }   
    
    #[test]
    fn json_eval() {
        assert_eq!(false, true);
    }

    #[test]
    fn json_eq() {
        assert_eq!(eq(&json!(1), &json!(1)), json!(true));
        assert_eq!(eq(&json!(1), &json!(2)), json!(false));
        assert_eq!(eq(&json!("a"), &json!("a")), json!(true));
        assert_eq!(eq(&json!("a"), &json!("b")), json!(false));
        assert_eq!(eq(&json!("a"), &json!("a")), json!(true));
        assert_eq!(eq(&json!("a"), &json!("b")), json!(false));                
    }

    #[test]
    fn json_first() {
        assert_eq!(first(&json!([1,2,3,4])), json!(1));
        assert_eq!(first(&json!([])), Json::Null);    
        assert_eq!(first(&json!([1])), json!(1));                
    }   
    
    #[test]
    fn json_ge() {
        assert_eq!(ge(&json!(1), &json!(2)), json!(false));
        assert_eq!(ge(&json!(2), &json!(2)), json!(true)); 
        assert_eq!(ge(&json!(3), &json!(2)), json!(true));      
    }    
    
    #[test]
    fn json_gt() {
        assert_eq!(gt(&json!(1), &json!(2)), json!(false));
        assert_eq!(gt(&json!(2), &json!(2)), json!(false));
        assert_eq!(gt(&json!(3), &json!(2)), json!(true));      
    }   
    
    #[test]
    fn json_if() {
        assert!(false) // todo implement tests  
    }    
    
    #[test]
    fn json_key() {
        assert!(false);   
    }    

    #[test]
    fn json_sum() {
        assert_eq!(sum(&json!(1i64)), json!(1));
        assert_eq!(sum(&json!(1.23f64)), json!(1.23));
        assert_eq!(sum(&json!(vec![1i64,2,3])), json!(6));
        assert_eq!(sum(&json!(vec![true,true,false])), json!(2));
    }    

    #[test]
    fn json_sums() {
        assert_eq!(sums(&json!(1i64)), json!([1]));
        assert_eq!(sums(&json!(1.23f64)), json!([1.23]));
        assert_eq!(sums(&json!(vec![1i64,2,3])), json!([1, 1+2, 1+2+3]));
    }        

    #[test]
    fn unique_ok() {
        assert_eq!(unique(&json!([1,1,2,2,2,3])), json!([1,2,3]));
        assert_eq!(unique(&json!([4,1,1,2,2,2,3])), json!([4,1,2,3]));
        assert_eq!(unique(&json!(1)), json!(1));
    }   
    
    #[test]
    fn len_ok() {
        assert_eq!(len(&json!([1,1,2,2,2,3])), json!(6));
        assert_eq!(len(&json!([4,1,1,2,2,2,3])), json!(7));
        assert_eq!(len(&json!(1)), json!(1));
        assert_eq!(len(&json!([])), json!(0));
        assert_eq!(len(&json!({"a": 1, "b": 2, "c": [1,2,3]})), json!(3))
    }
}

