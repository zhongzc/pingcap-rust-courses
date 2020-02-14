extern crate clap;

use clap::{App, AppSettings, Arg, SubCommand};
use kvs::{KvStore, Result};
use std::env;
use std::process::exit;

fn main() -> Result<()> {
    let matches = App::new(env!("CARGO_PKG_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .subcommand(
            SubCommand::with_name("set")
                .about("Set the value of a string key to a string")
                .arg(Arg::with_name("KEY").help("A string key").required(true))
                .arg(
                    Arg::with_name("VALUE")
                        .help("The string value of the key")
                        .required(true),
                ),
        )
        .subcommand(
            SubCommand::with_name("get")
                .about("Get the string value of a given string key")
                .arg(Arg::with_name("KEY").help("A string key").required(true)),
        )
        .subcommand(
            SubCommand::with_name("rm")
                .about("Remove a given key")
                .arg(Arg::with_name("KEY").help("A string key").required(true)),
        )
        .get_matches();

    let mut kvs = KvStore::open(env::current_dir()?)?;

    match matches.subcommand() {
        ("get", Some(get_command)) => {
            let key = get_command.value_of("KEY").unwrap();
            let val = kvs.get(key.to_owned())?;
            match val {
                Some(v) => println!("{}", v),
                None => {
                    println!("Key not found");
                }
            };
            Ok(())
        }
        ("set", Some(set_command)) => {
            let key = set_command.value_of("KEY").unwrap();
            let val = set_command.value_of("VALUE").unwrap();
            kvs.set(key.to_owned(), val.to_owned())?;
            Ok(())
        }
        ("rm", Some(rm_command)) => {
            let key = rm_command.value_of("KEY").unwrap();
            let res = kvs.remove(key.to_owned());
            if res.is_err() {
                println!("Key not found");
                exit(1);
            }
            Ok(())
        }
        _ => unreachable!(),
    }
}
