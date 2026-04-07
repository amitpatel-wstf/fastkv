use std::io::{self, Write};
use c_cli::{Store, parse_command, CommandError};

fn main() {
    let mut store = Store::new();

    loop {
        print!("redis> ");
        io::stdout().flush().unwrap();

        let mut input = String::new();
        io::stdin().read_line(&mut input).unwrap();

        match parse_command(&input) {
            Ok(cmd) => {
                let output = store.execute(cmd);
                println!("{}", output);
            }

            Err(err) => match err {
                CommandError::EmptyInput => continue,
                CommandError::MissingArguments => println!("Error: Missing arguments"),
                CommandError::InvalidCommand => println!("Error: Invalid command"),
            },
        }
    }
}