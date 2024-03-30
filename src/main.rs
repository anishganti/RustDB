mod utils;
use utils::btree::BTreeNode;
use std::io;

fn main() {
    let mut root = BTreeNode::new();

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
        let value: usize = 0;
        let result = match op {
            "read"  => root.read(key),
            "write" => root.write(key, value),
            _ => Some(3),
        };

        match result {
            Some(value) => println!("Result: {}", value),
            None => println!("No result"),
        }
    }

    println!("See you later!");
}