use std::{collections::HashMap, path::Path};

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
    ) -> Result<Vec<(geo::Geometry, HashMap<String, sqlite::Value>)>, GpkgError> {
        let query_string = match columns {
            Some(cols) => format!("SELECT geom, {} from {};", cols.join(", "), layer_name),
            None => format!("SELECT geom from {};", layer_name),
        };
        match self.db.prepare(query_string.as_str()) {
            Ok(mut stmt) => {
                let res = stmt
                    .iter()
                    .filter_map(|row_result| match row_result {
                        Ok(row) => {
                            let geom = row.read::<&[u8], _>(0);
                            let mut other_values: HashMap<String, sqlite::Value> = HashMap::new();
                            if let Some(cols) = columns {
                                for (k, &col) in cols.iter().enumerate() {
                                    other_values.insert(col.to_owned(), row[k + 1].clone());
                                }
                            }
                            match wkb_bytes_to_geo(geom) {
                                Ok(geom) => Some((geom, other_values)),
                                Err(_) => None,
                            }
                        }
                        Err(_) => None,
                    })
                    .collect::<Vec<_>>();
                // Ok(res)
                Ok(res)
            }
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
    if let Some(end_byte) = bytes.get(0) {
        let litte_endian = *end_byte == 1;
        let mut bytes_consumed = 1_usize;
        if let Some(Ok(num)) = bytes.get(1..5).map(|b| b.try_into()) {
            let geom_type_num = if litte_endian {
                u32::from_le_bytes(num)
            } else {
                u32::from_be_bytes(num)
            };
            bytes_consumed += 5;
            let (geom, used_bytes) = match geom_type_num {
                1 => match wkb_bytes_to_point(bytes) {
                    Ok((geom, used_bytes)) => (geo::Geometry::Point(geom), used_bytes),
                    Err(e) => return Err(e),
                },
                2 => match wkb_bytes_to_linestring(bytes) {
                    Ok((geom, used_bytes)) => (geo::Geometry::LineString(geom), used_bytes),
                    Err(e) => return Err(e),
                },
                3 => match wkb_bytes_to_polygon(bytes) {
                    Ok((geom, bytes_used)) => (geo::Geometry::Polygon(geom), bytes_used),
                    Err(e) => return Err(e),
                },
                4 => unimplemented!("Multipoint Geometry"),
                5 => unimplemented!("MultilineString Not Implemented"),
                6 => match wkb_bytes_to_multipolygon(bytes) {
                    Ok((geom, bytes_used)) => (geo::Geometry::MultiPolygon(geom), bytes_used),
                    Err(e) => return Err(e),
                },
                other => return Err(GeometryError::TypeIdentifier(other)),
            };
            return Ok(geom);
        }
    }
    Err(GeometryError::Malform)
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
    let _geom_type = if let Some(Ok(type_bytes)) = bytes.get(1..5).map(|b| b.try_into()) {
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
        let little_endian = *byte_order == 1;
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
fn wkb_bytes_to_polygon(bytes: &[u8]) -> Result<(geo::Polygon, usize), GeometryError> {
    let mut bytes_consumed = 0_usize;
    if let Some(byte_order_byte) = bytes.get(0) {
        let little_endian = *byte_order_byte == 1u8;
        bytes_consumed += 1;
        if let Some(Ok(type_bytes)) = bytes.get(1..5).map(|b| b.try_into()) {
            let wkbtype = if little_endian {
                u32::from_le_bytes(type_bytes)
            } else {
                u32::from_be_bytes(type_bytes)
            };
            bytes_consumed += 4;
            if wkbtype != 3 {
                return Err(GeometryError::TypeIdentifier(wkbtype));
            }
            if let Some(Ok(nrings_bytes)) = bytes.get(5..9).map(|b| b.try_into()) {
                let n_rings = if little_endian {
                    u32::from_le_bytes(nrings_bytes)
                } else {
                    u32::from_be_bytes(nrings_bytes)
                };
                bytes_consumed += 4;
                let mut exterior_ring: geo::LineString = geo::LineString::new(vec![]);
                let mut interior_rings = Vec::with_capacity(n_rings as usize - 1);
                for k in 0..n_rings {
                    if let Some(bytes_offset) = bytes.get(bytes_consumed..) {
                        match wkb_bytes_to_linestring(bytes_offset) {
                            Ok((line, n_bytes)) => {
                                if k == 0 {
                                    exterior_ring = line;
                                } else {
                                    interior_rings.push(line);
                                }
                                bytes_consumed += n_bytes;
                            }
                            Err(e) => return Err(e),
                        }
                    } else {
                        return Err(GeometryError::Malform);
                    }
                }
                return Ok((
                    geo::Polygon::new(exterior_ring, interior_rings),
                    bytes_consumed,
                ));
            }
        }
    }
    Err(GeometryError::Malform)
}

fn wkb_bytes_to_multipolygon(bytes: &[u8]) -> Result<(geo::MultiPolygon, usize), GeometryError> {
    if let Some(byte_order) = bytes.get(0) {
        let mut bytes_consumed = 0_usize;
        let little_endian = *byte_order == 1;
        bytes_consumed += 1;
        if let Some(Ok(type_bytes)) = bytes.get(1..5).map(|b| b.try_into()) {
            bytes_consumed += 4;
            let geom_type = if little_endian {
                u32::from_le_bytes(type_bytes)
            } else {
                u32::from_be_bytes(type_bytes)
            };
            if geom_type != 6 {
                return Err(GeometryError::TypeIdentifier(geom_type));
            }
            if let Some(Ok(n_poly_bytes)) = bytes.get(5..9).map(|b| b.try_into()) {
                bytes_consumed += 4;
                let n_polys = if little_endian {
                    u32::from_le_bytes(n_poly_bytes)
                } else {
                    u32::from_be_bytes(n_poly_bytes)
                };
                let mut polygons = Vec::with_capacity(n_polys as usize);
                for _ in 0..n_polys {
                    if let Some(bytes) = bytes.get(bytes_consumed..) {
                        match wkb_bytes_to_polygon(bytes) {
                            Ok((poly, bytes_used)) => {
                                polygons.push(poly);
                                bytes_consumed += bytes_used;
                            }
                            Err(e) => return Err(e),
                        }
                    } else {
                        return Err(GeometryError::Malform);
                    }
                }
                return Ok((geo::MultiPolygon::new(polygons), bytes_consumed));
            }
        }
    }
    Err(GeometryError::Malform)
}
