extern crate glob;
extern crate protoc_rust;
extern crate ethers;
extern crate ethers_solc;
extern crate convert_case;

use protoc_rust::Customize;
use std::env;
use std::fs;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use ethers::contract::Abigen;
use convert_case::{Case, Casing};
// use ethers_solc::Solc;

fn main() {
    let out_dir = env::var("OUT_DIR").unwrap();
    let cur_crate = env::var("CARGO_MANIFEST_DIR").unwrap();

    // create contracts rust files from contract solidity files
    let contract_src_path = Path::new(&cur_crate).join("src/contracts");
    let contract_dest_path = Path::new(&out_dir).join("contracts");
    fs::create_dir_all(&contract_dest_path).unwrap();
    // let sol_src_files = glob_simple("./src/contracts/*.sol");
    let abi_src_files = glob_simple("./src/contracts/*.json");

    abi_src_files.iter().for_each(|file| {
        let contract_name = Path::new(file).file_stem().unwrap().to_str().unwrap();
        let abi_file_path = contract_src_path.join(format!("{}.abi", contract_name)).to_str().unwrap().to_owned();

        Abigen::new(contract_name, abi_file_path).expect("create contract builder")
            .generate().expect("generate contract bindings")
            .write_to_file(
                contract_dest_path.join(format!("{}.rs", contract_name.to_case(Case::Snake)))
            ).unwrap();
    });
    // for file in &abi_src_files {

    //     // let target_contracts_dir = Path::new(cur_crate.as_str()).join(file.as_str());
    //     // let compiled = Solc::default()
    //     //     .compile_source(&target_contracts_dir)
    //     //     .expect(format!("Could not compile contracts: {}", target_contracts_dir.to_str().unwrap()).as_str());

    //     let contract_name = Path::new(file).file_stem().unwrap().to_str().unwrap();

    //     // let (abi, _, _) = compiled
    //     //     .find(contract_name)
    //     //     .expect(format!("could not find contract: {} out of {:?}", file, compiled.contracts).as_str())
    //     //     .into_parts_or_default();

    //     let abi_file_path = contract_src_path.join(format!("{}.abi", contract_name)).to_str().unwrap().to_owned();
    //     // let mut abi_file = File::create(&abi_file_path).unwrap();
    //     // abi_file.write_all(serde_json::to_string(&abi).expect("serialize abi").as_bytes()).expect("write abi to file");
    //     // abi_file.flush().expect("flush abi to file");

    //     Abigen::new(contract_name, abi_file_path).expect("create contract builder")
    //         .generate().expect("generate contract bindings")
    //         .write_to_file(
    //             contract_dest_path.join(format!("{}.rs", contract_name.to_case(Case::Snake)))
    //         ).unwrap();
    // }

    // create corresponding mod.rs
    let mod_file_content = abi_src_files
        .iter()
        .map(|abi_file| {
            let abi_path = Path::new(abi_file);
            format!(
                "pub mod {};",
                abi_path
                    .file_stem()
                    .expect("Unable to extract stem")
                    .to_str()
                    .expect("Unable to extract filename")
                    .to_case(Case::Snake)
            )
        })
        .collect::<Vec<_>>()
        .join("\n");

    let mut mod_file = File::create(contract_dest_path.join("mod.rs")).unwrap();
    mod_file
        .write_all(mod_file_content.as_bytes())
        .expect("Unable to write mod file");


    // create proto.rs files from proto files
    let proto_dest_path = Path::new(&out_dir).join("protos");
    fs::create_dir_all(&proto_dest_path).unwrap();

    let proto_src_files = glob_simple("./src/protos/*.proto");
    println!("{:?}", proto_src_files);

    protoc_rust::Codegen::new()
        .out_dir(&proto_dest_path.to_str().unwrap())
        .inputs(
            &proto_src_files
                .iter()
                .map(|proto_file| proto_file.as_ref())
                .collect::<Vec<&str>>(),
        )
        .includes(&["./src/protos"])
        .customize(Customize::default())
        .run()
        .expect("Error generating rust files from smallbank protos");

    // Create mod.rs accordingly
    let mod_file_content = proto_src_files
        .iter()
        .map(|proto_file| {
            let proto_path = Path::new(proto_file);
            format!(
                "pub mod {};",
                proto_path
                    .file_stem()
                    .expect("Unable to extract stem")
                    .to_str()
                    .expect("Unable to extract filename")
            )
        })
        .collect::<Vec<_>>()
        .join("\n");

    let mut mod_file = File::create(proto_dest_path.join("mod.rs")).unwrap();
    mod_file
        .write_all(mod_file_content.as_bytes())
        .expect("Unable to write mod file");
}

fn glob_simple(pattern: &str) -> Vec<String> {
    glob::glob(pattern)
        .expect("glob")
        .map(|g| {
            g.expect("item")
                .as_path()
                .to_str()
                .expect("utf-8")
                .to_owned()
        })
        .collect()
}