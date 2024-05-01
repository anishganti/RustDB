mod btree;
use btree::tree::BTree;
use std::fs::File;
use std::io::{self, Write};

//  writing different types of data
//  imposing key order for different key types 
//  test split pages
//  removing nodes
//  replicas
//  partitions 


fn main() {
    let file_path = "/Users/anishganti/RustDB/src/test.bin";
    let wal_path = "/Users/anishganti/RustDB/src/test_wal.bin";

    match create_binary_file(file_path, 4096) {
        Ok(_) => println!("New database '{}' created successfully.", file_path),
        Err(err) => eprintln!("Error creating database file: {}", err),
    }

    match create_binary_file(wal_path, 0) {
        Ok(_) => println!("New write ahead log (WAL) '{}' created successfully.", file_path),
        Err(err) => eprintln!("Error creating WAL file: {}", err),
    }

    // Load the database by opening the file and WAL from disk. 
    let mut database = match BTree::new(file_path,  wal_path) {
        Ok(btree) => btree,
        Err(err) => {
            eprintln!("Error creating BTree instance: {}", err);
            return;
        }
    };

    // Recover any lost changes made before. 
    database.recover();
    
    println!("Please type something, or stop to escape:");
    let mut input_string = String::new();


    loop {
        input_string.clear(); 
        io::stdin().read_line(&mut input_string).unwrap(); // 

        let trimmed_input = input_string.trim();

        if trimmed_input == "stop" {
            database.flush();
            break;
        }

        let mut args = trimmed_input.split_whitespace();
        let op = args.next().unwrap_or(""); 
        let key = args.next().unwrap_or("").parse::<u16>().unwrap_or(1);
        let value = args.next().unwrap_or("").parse::<u16>().unwrap_or(0);
        let mut result = None;

        if op == "read" {
            result = database.read(key);
        } else if op == "write" {
            database.write(key, value);
        }

        match result {
            Some(value) => println!("Result: {}", value),
            None => println!("No result"),
        }
    }

    println!("See you later!");
}

fn create_binary_file(file_path: &str, length : usize) -> io::Result<()> {
    // Create a new file at the specified file_path
    let mut file = File::create(file_path)?;

    // Define the content of the binary file (4096 bytes)
    let mut bytes = vec![0u8; length];  // Create a vector initialized with 4096 zeros

    // Set the 5th byte (index 4) to 1 since the root should be initialized as the leaf
    if bytes.len() > 4 && length == 4096  {
        bytes[4] = 1;
    } else {
        //return Err(io::Error::new(io::ErrorKind::InvalidInput, "Insufficient bytes in buffer"));
    }

    // Write the bytes to the file
    file.write_all(&bytes)?;

    // Close the file
    file.flush()?;  // Flush any buffered data to ensure all bytes are written
    drop(file);     // Close the file explicitly by dropping the file variable

    Ok(())
}

