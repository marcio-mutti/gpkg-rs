#[test]
fn open_localidades() {
    let loc = gpkg_rs::Gpkg::read_from_db(
        "/home/marcio/Documentos/Rust/Pessoal/gpkg-rs/Localidados_Brasileiras_2010.gpkg",
    )
    .unwrap();
    let loc_layers = loc.list_layers().unwrap();
    assert_eq!(loc_layers, vec!["Localidados_Brasileiras_2010"])
}
