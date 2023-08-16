use std::path::Path;

use geo::{line_string, Coord};
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
    #[error("Error trying to decode Point")]
    PointError,
    #[error("Malformed Gpkg Geometry hearder")]
    Malform,
    #[error("Unable to identify bytestream endness")]
    EndNess,
    #[error("Unable to decode bytestream content")]
    Content,
    #[error("Invalid Geometry Type Identifier")]
    TypeIdentifier(u32),
}

fn gpkg_geometry(bytes: &[u8]) -> Result<geo::Geometry, GeometryError> {
    //Interpretar o header
    // magic number (2 bytes)
    if let (Some(magic_1), Some(magic_2)) = (bytes.get(0), bytes.get(1)) {
        if *magic_1 != 0x47 || *magic_2 != 0x50 {
            return Err(GeometryError::Malform);
        }
    } else {
        return Err(GeometryError::Malform);
    }
    //Not caring abou tthe version for now
    let flags = if let Some(f) = bytes.get(4) {
        GeoPackageBinary::new(*f)
    } else {
        return Err(GeometryError::Malform);
    };
    if !flags.valid_envelope() {
        return Err(GeometryError::Malform);
    }
    let srs_id = if let Some(Ok(s)) = bytes.get(4..(4 + 4)).map(|sl| sl.try_into()) {
        if flags.little_endian() {
            i32::from_le_bytes(s)
        } else {
            i32::from_be_bytes(s)
        }
    } else {
        return Err(GeometryError::Malform);
    };
    let envelope_len: usize = match flags.envelope_type {
        0 => 0,
        1 => 4,
        2 | 3 => 6,
        4 => 8,
        _ => {
            return Err(GeometryError::Malform);
        }
    };
    let mut envelope: Vec<f64> = Vec::with_capacity(envelope_len);
    if let Some(env) = bytes.get(8..(8 + 8 * envelope_len)) {
        for ck in env.chunks(8) {
            if let Ok(ck) = ck.try_into() {
                envelope.push(if flags.little_endian() {
                    f64::from_le_bytes(ck)
                } else {
                    f64::from_be_bytes(ck)
                });
            } else {
                return Err(GeometryError::Malform);
            }
        }
    } else {
        return Err(GeometryError::Malform);
    }
    if let Some(geom_bytes) = bytes.get((8 + 8 * envelope_len)..) {
        wkb_bytes_to_geo(geom_bytes)
    } else {
        Err(GeometryError::Malform)
    }
}

struct GeoPackageBinary {
    empty_geometry: bool,
    envelope_type: u8,
    little_endian: bool,
}
impl GeoPackageBinary {
    pub fn new(b: u8) -> Self {
        let empty_geometry = b & 0b10000 != 0;
        let envelope_type = b & 0b1110;
        let little_endian = b & 0b1 != 0;
        Self {
            empty_geometry,
            envelope_type,
            little_endian,
        }
    }
    pub fn is_empty(&self) -> bool {
        self.empty_geometry
    }
    pub fn valid_envelope(&self) -> bool {
        self.envelope_type < 5
    }
    pub fn little_endian(&self) -> bool {
        self.little_endian
    }
    pub fn envelope(&self) -> &u8 {
        &self.envelope_type
    }
}

fn wkb_bytes_to_geo(bytes: &[u8]) -> Result<geo::Geometry, GeometryError> {
    let little_endian = if let Some(endness) = bytes.get(0) {
        if *endness == 1 {
            true
        } else {
            false
        }
    } else {
        return Err(GeometryError::EndNess);
    };
    let geom_type_num = if let Some(Ok(num)) = bytes.get(1..5).map(|sl| sl.try_into()) {
        if little_endian {
            u32::from_le_bytes(num)
        } else {
            u32::from_be_bytes(num)
        }
    } else {
        return Err(GeometryError::Content);
    };
    let mut f64_stream = Vec::new();
    for ck in bytes[5..].chunks(8) {
        if let Ok(ck_array) = ck.try_into() {
            f64_stream.push(if little_endian {
                f64::from_le_bytes(ck_array)
            } else {
                f64::from_be_bytes(ck_array)
            });
        } else {
            return Err(GeometryError::Content);
        }
    }
    match geom_type_num {
        1 => {
            if f64_stream.len() != 2 {
                return Err(GeometryError::Content);
            }
            Ok(geo::geometry::Geometry::Point(geo::geometry::Point::new(
                *f64_stream.get(0).unwrap(),
                *f64_stream.get(1).unwrap(),
            )))
        }
        2 => {
            if f64_stream.len() < 2 || f64_stream.len() % 2 != 0 {
                return Err(GeometryError::Content);
            }
            Ok(geo::geometry::Geometry::LineString(
                geo::geometry::LineString::new(
                    f64_stream
                        .chunks(2)
                        .map(|fs| Coord {
                            x: *fs.get(0).unwrap(),
                            y: *fs.get(1).unwrap(),
                        })
                        .collect::<Vec<_>>(),
                ),
            ))
        }
        3 => {
            if f64_stream.len() < 2 || f64_stream.len() % 2 != 0 {
                return Err(GeometryError::Content);
            }
            Ok(geo::geometry::Geometry::Polygon(
                geo::geometry::Polygon::new(
                    geo::geometry::LineString::new(
                        f64_stream
                            .chunks(2)
                            .map(|fs| Coord {
                                x: *fs.get(0).unwrap(),
                                y: *fs.get(1).unwrap(),
                            })
                            .collect::<Vec<_>>(),
                    ),
                    vec![],
                ),
            ))
        }
        k => unimplemented!("Geometry type {k} still not implemented"),
    }
}

fn wkb_bytes_to_point(bytes: &[u8]) -> Result<(geo::Point, usize), GeometryError> {
    let mut bytes_consumed = 0_usize;
    let little_endian = if let Some(byte_order) = bytes.get(0) {
        match byte_order {
            1 => true,
            0 => false,
            _ => return Err(GeometryError::PointError),
        }
    } else {
        return Err(GeometryError::PointError);
    };
    bytes_consumed += 1;
    let geom_type = if let Some(Ok(type_bytes)) = bytes.get(1..5).map(|b| b.try_into()) {
        match little_endian {
            true => i32::from_le_bytes(type_bytes),
            false => i32::from_be_bytes(type_bytes),
        }
    } else {
        return Err(GeometryError::PointError);
    };
    bytes_consumed += 4;
    if let (Some(Ok(x_bytes)), Some(Ok(y_bytes))) = (
        bytes.get(5..13).map(|b| b.try_into()),
        bytes.get(13..21).map(|b| b.try_into()),
    ) {
        bytes_consumed += 16;
        Ok((
            match little_endian {
                true => (f64::from_le_bytes(x_bytes), f64::from_le_bytes(y_bytes)).into(),
                false => (f64::from_be_bytes(x_bytes), f64::from_be_bytes(y_bytes)).into(),
            },
            bytes_consumed,
        ))
    } else {
        Err(GeometryError::PointError)
    }
}

fn wkb_bytes_to_linearring(bytes: &[u8]) -> Result<(Vec<geo::Point>, usize), GeometryError> {
    let mut bytes_consumed = 0_usize;
    let n_points = if let Some(Ok(len_bytes)) = bytes.get(0..4).map(|b| b.try_into()) {
        i32::from_le_bytes(len_bytes) as usize
    } else {
        return Err(GeometryError::Malform);
    };
    bytes_consumed += 4;
    let mut points = Vec::with_capacity(n_points);
    for _ in 0..n_points {
        if let Some(bytes) = bytes.get(bytes_consumed..) {
            if let Ok((point, consumed)) = wkb_bytes_to_point(bytes) {
                bytes_consumed += consumed;
                points.push(point);
            } else {
                return Err(GeometryError::Malform);
            }
        } else {
            return Err(GeometryError::Malform);
        }
    }
    Ok((points, bytes_consumed))
}

fn wkb_bytes_to_linestring(bytes: &[u8]) -> Result<(geo::LineString, usize), GeometryError> {
    let mut bytes_consumed = 0;
    if let Some(byte_order) = bytes.get(0) {
        let little_endian = match byte_order {
            1 => true,
            _ => false,
        };
        bytes_consumed += 1;
        if let Some(Ok(type_bytes)) = bytes.get(1..5).map(|b| b.try_into()) {
            let type_assert = match little_endian {
                true => u32::from_le_bytes(type_bytes),
                false => u32::from_be_bytes(type_bytes),
            };
            if type_assert != 2 {
                return Err(GeometryError::TypeIdentifier(type_assert));
            }
            bytes_consumed += 4;
            if let Some(Ok(n_points_bytes)) = bytes.get(5..9).map(|b| b.try_into()) {
                let n_points = match little_endian {
                    true => u32::from_le_bytes(n_points_bytes),
                    false => u32::from_be_bytes(n_points_bytes),
                };
                bytes_consumed += 4;
                let mut vec_points = Vec::with_capacity(n_points as usize);
                for _ in 0..n_points {
                    if let Some(bytes) = bytes.get(bytes_consumed..) {
                        if let Ok((point, consumed)) = wkb_bytes_to_point(bytes) {
                            bytes_consumed += consumed;
                            vec_points.push(point);
                        }
                    }
                }
                let line_string = geo::LineString::from(vec_points);
                return Ok((line_string, bytes_consumed));
            }
        }
    }
    Err(GeometryError::Malform)
}
