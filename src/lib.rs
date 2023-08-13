use std::path::Path;

use sqlite::Value;
use thiserror::Error;

#[cfg(test)]
mod tests {
    /* use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    } */
}

#[derive(Error, Debug)]
pub enum GpkgError {
    #[error("Unable to open GeoPackage File")]
    OpenError(sqlite::Error),
    #[error("The SQLite file does not have a GeoPackage structure")]
    SchemaError,
    #[error("SQLite Query Error")]
    QueryError(sqlite::Error),
}

pub struct Gpkg {
    db: sqlite::Connection,
}

impl Gpkg {
    pub fn read_from_db(file: impl AsRef<Path>) -> Result<Gpkg, GpkgError> {
        match sqlite::open(file.as_ref()) {
            Ok(db) => {
                {
                    match db.prepare("select name from sqlite_schema where name = 'gpkg_contents';")
                    {
                        Ok(mut stmt) => {
                            if stmt.iter().count() == 0 {
                                return Err(GpkgError::SchemaError);
                            }
                        }
                        Err(e) => return Err(GpkgError::OpenError(e)),
                    }
                }
                Ok(Self { db })
            }
            Err(e) => Err(GpkgError::OpenError(e)),
        }
    }
    pub fn list_layers(&self) -> Result<Vec<String>, GpkgError> {
        match self.db.prepare("SELECT table_name from gpkg_contents;") {
            //In the future, search for different layer types
            Ok(mut stmt) => {
                Ok(stmt
                    .iter()
                    .filter_map(|row_result| match row_result {
                        Ok(row) => Some(row.read::<&str, _>("table_name").to_owned()),
                        Err(_) => None, // In the future, Solve this kind of error
                    })
                    .collect())
            }
            Err(e) => Err(GpkgError::QueryError(e)),
        }
    }
    pub fn get_features(
        &self,
        layer_name: &str,
        columns: Option<&[&str]>,
    ) -> Result<Vec<(geo::Geometry, Vec<Value>)>, GpkgError> {
        let columns = match columns {
            Some(cols) => format!("geom, {}", cols.join(", ")),
            None => "geom".to_owned(),
        };
        match self
            .db
            .prepare(format!("Select {} from 1", columns).as_str())
        {
            Ok(mut stmt) => match stmt.bind((1, layer_name)) {
                Ok(_) => {
                    let res = stmt.iter().filter_map(|row_result| match row_result {
                        Ok(row) => {
                            let geom = row.read::<&[u8], _>(0);
                            None
                        }
                        Err(_) => None,
                    });
                    todo!()
                }
                Err(e) => Err(GpkgError::QueryError(e)),
            },
            Err(e) => Err(GpkgError::QueryError(e)),
        }
    }
}

#[derive(Error, Debug)]
pub enum GeometryError {
    #[error("Unable to identify bytestream endness")]
    EndNess,
    #[error("Unable to decode bytestream content")]
    Content,
    #[error("Invalid Geometry Type Identifier")]
    TypeIdentifier(u32),
}

fn bytes_to_wkb(bytes: &[u8]) -> Result<geo::Geometry, GeometryError> {
    //This need to become a Result
    let little_endian = if let Some(endness) = bytes.get(0) {
        if *endness == 1 {
            true
        } else {
            false
        }
    } else {
        return Err(GeometryError::EndNess);
    };
    let geom_type_num = if let (Some(d0), Some(d1), Some(d2), Some(d3)) =
        (bytes.get(1), bytes.get(2), bytes.get(3), bytes.get(4))
    {
        let type_bytes = [*d0, *d1, *d2, *d3];
        if little_endian {
            u32::from_le_bytes(type_bytes)
        } else {
            u32::from_be_bytes(type_bytes)
        }
    } else {
        return Err(GeometryError::Content);
    };
    todo!()
}
