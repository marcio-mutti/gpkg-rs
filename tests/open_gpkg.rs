#[test]
fn open_localidades() {
    let loc = gpkg_rs::Gpkg::read_from_db(
        "/home/marcio/Documentos/Rust/Pessoal/gpkg-rs/Localidados_Brasileiras_2010.gpkg",
    )
    .unwrap();
    let loc_layers = loc.list_layers().unwrap();
    assert_eq!(loc_layers, vec!["Localidados_Brasileiras_2010"])
}

#[test]
fn get_features() {
    let loc = gpkg_rs::Gpkg::read_from_db(
        "/home/marcio/Documentos/Rust/Pessoal/gpkg-rs/Localidados_Brasileiras_2010.gpkg",
    )
    .unwrap();
    let dados_recuperados = loc
        .get_features(
            "Localidados_Brasileiras_2010",
            Some(&["setor_localidade", "nome_categoria", "moradores"]),
        )
        .unwrap();
    assert_eq!(dados_recuperados.len(), 21_525);
}
