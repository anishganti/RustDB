mod utils;
use utils::btree::BTree;
use std::io;

fn main() {
    let mut database = BTree::new();

    println!("Please type something, or stop to escape:");
    let mut input_string = String::new();


    loop {
        input_string.clear(); 
        io::stdin().read_line(&mut input_string).unwrap(); // 
        println!("{}", input_string);

        let trimmed_input = input_string.trim();

        if trimmed_input == "stop" {
            break;
        }

        let mut args = trimmed_input.split_whitespace();
        let op = args.next().unwrap_or(""); 
        let key = args.next().unwrap_or("").parse::<usize>().unwrap_or(1);
        let mut result = None;

        if op == "read" {
            result = database.read(key);
        } else if op == "write" {
            database = database.write(key, key);
        } 

        match result {
            Some(value) => println!("Result: {}", value),
            None => println!("No result"),
        }
    }

    println!("See you later!");
}


