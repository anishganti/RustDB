const BRANCHING_FACTOR: u16 = 5;
const LEVELS: usize = 3;
const MAX_PAGE_BYTES: usize = 4096;

extern crate linked_hash_map;

use std::collections::HashMap;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::{self, Read, Write, Seek};
use std::convert::TryInto;
use self::linked_hash_map::LinkedHashMap;

/// An in-memory cache to hold the most recently accessed pages. 
/// and reduce the number of I/O operations. Uses Least Recently Used (LRU)
/// method to evict old pages if the cache is full. 
pub struct LRUCache {
    map: LinkedHashMap<u32, BTreeNode>
}

impl LRUCache {
    fn new() -> Self {
       Self { 
        map : LinkedHashMap::new()
       }
    }

    fn insert(&mut self, key: u32, val: BTreeNode){
        if(self.map.contains_key(&key)){
            self.map.remove(&key);
        } else if (self.map.len() == 10){
            self.map.pop_back();
        } 

        self.map.insert(key, val);
    }

    fn get(&self, key: u32) -> &BTreeNode{
        self.map.get(&key).unwrap()
    }

    fn get_mut(&mut self, key: u32) -> Option<&mut BTreeNode>{
        self.map.get_mut(&key)
    }


    fn contains_key(&self, key: u32) -> bool {
        self.map.contains_key(&key)
    }

}

/// The structure of the a single node (page) within the overall B-Tree. 
/// Contains either the key-value stores (if leaf node) or key ranges with locations to the child nodes. 
pub struct BTreeNode {
    id: u32, 
    leaf: u8, 
    num_keys: u16, 

    keys: Vec<u16>, 
    children: Vec<u32>,
    vals: Vec<u16>, 
}

impl BTreeNode {
    /// Create a new leaf page (node) with empty lists. 
    pub fn new() -> Self {
        Self {
            id: 0,
            num_keys: 0,
            keys: Vec::new(),
            children: Vec::new(),
            vals: Vec::new(), 
            leaf: 1
        }
    }

    /// Create a new node given all possible parameters. 
    pub fn new_from_params(id:u32, leaf:u8, num_keys:u16, keys: Vec<u16>, 
                                children: Vec<u32>, vals: Vec<u16> ) -> Self {
        Self {
            id,
            leaf,
            num_keys,
            keys,
            children,
            vals, 
        }
    }


    /// Persists the current node to the disk. Borrows the file from the B-Tree itself and 
    /// calculates the file position from using the node's id and the fact that a node 
    /// is limited to be at most 4096 bytes. 
    fn write_node_to_file(&self, file: &mut File) {
        let offset: u64 = (4096*(self.id)).into();
        let buffer = self.serialize();
        file.seek(io::SeekFrom::Start(offset)); 
        file.write_all(&buffer);
    }

    /// Serializes the node into a sequence of bytes since the database is persisted as a binary file. 
    pub fn serialize(&self) -> Vec<u8> {
        let mut buffer = Vec::new();

        buffer.write_all(&self.id.to_le_bytes()).unwrap();
        buffer.write_all(&self.leaf.to_le_bytes()).unwrap();
        buffer.write_all(&self.num_keys.to_le_bytes()).unwrap();

        for key in &self.keys {
            let bytes = key.to_le_bytes();
            buffer.write_all(&bytes).unwrap();
        }

        match self.leaf == 1 {
            true => {
                for val in &self.vals {
                    let bytes = val.to_le_bytes();
                    buffer.write_all(&bytes).unwrap();
                }        
            }, 

            false => {
                for child in &self.children {
                    let bytes = child.to_le_bytes();
                    buffer.write_all(&bytes).unwrap();
                }        

            }
        }

        let padding_len = 4096 - buffer.len();
        buffer.extend_from_slice(&vec![0u8; padding_len]);  

        buffer      
    }

    /// Given an input key, searches through the node's key array for the key and returns the index if found. 
    /// If the key isn't found, returns the index of the first value greater than the key. 
    /// 
    /// Idea is that either key[i] pairs with val[i] OR 
    /// The child node at children[0] points to the nodes storing all keys less than key[i]. 
    pub fn search(&self, key: u16) -> usize {
        if self.keys.len() == 0 {
            return 0
        }

        for i in 0..self.keys.len() {
            if self.keys[i] >= key {
                return i;
            } 
        }

        self.keys.len()
    }
}

/// Main database structure that holds the cache for easy access and the file for disk reads/writes. 
pub struct BTree {
    file : File,
    wal : File, 
    cache : LRUCache,
    dirty_pages : HashMap<u32, BTreeNode>, 
    num_nodes : u64
}

impl BTree{
    /// Creates new BTree by opening the file on disk as well as creating new buffers 
    /// for the write-ahead log (WAL), and cache. 
    pub fn new(file_path: &str, wal_path: &str) -> io::Result<BTree> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true) // <--------- this
            .open(file_path)?;

        let wal = OpenOptions::new()
            .read(true)
            .append(true)
            .open(wal_path)?;

        let cache = LRUCache::new();
        let dirty_pages = HashMap::new();
        let metadata = file.metadata()?;
        let num_nodes = metadata.len()/4096;

        Ok(Self { file, wal, cache, dirty_pages, num_nodes})   
    }

    /// Encodes the input key into a number to handle different input types. This way we can mantain integer values for
    /// all keys and keep the ability to perform quick range queries regardless of the user entegers an integer or some 
    /// other form of data for the key. 
    fn encode_key(&self, key: &str){

    }

    /// Searches the B-Tree for the page based on the node id passed by the user. First checks cache and dirty pages buffer
    /// for most recently accessed pages to avoid performing additional I/O operations. If not found, the system will search
    /// on the disk and newly accessed pages from the disk get moved into the cache. 
    fn get(&mut self, key: u32) -> &BTreeNode {
        if self.cache.contains_key(key){
            return self.cache.get(key);
        } else if self.dirty_pages.contains_key(&key) {
            return self.dirty_pages.get(&key).unwrap();
        } else {
            let node = self.read_node_from_file(key.try_into().unwrap()).unwrap();
            self.cache.insert(key, node);
            return self.cache.get(key);
        }
    }

    /// Same as the get() method but returns a mutable reference to the page/node for write() method. 
    fn get_mut(&mut self, key: u32) -> &mut BTreeNode {
        if self.cache.contains_key(key){
            return self.cache.get_mut(key).unwrap();
        } else if self.dirty_pages.contains_key(&key) {
            return self.dirty_pages.get_mut(&key).unwrap();
        } else {
            let node = self.read_node_from_file(key.try_into().unwrap()).unwrap();
            self.cache.insert(key, node);
            return self.cache.get_mut(key).unwrap();
        }
    }

    //Remove
    fn get_object(&mut self, key: u32) -> BTreeNode {
        if self.cache.contains_key(key) {
            return self.cache.map.remove(&key).unwrap();
        } else if self.dirty_pages.contains_key(&key) {
            return self.dirty_pages.remove(&key).unwrap();
        } else {
            let node = self.read_node_from_file(key.try_into().unwrap()).unwrap();
            return node;           
        }
    }


    /// Loads and deserializes node from the disk into memory from the starting position specified. 
    fn read_node_from_file(&mut self, offset: usize) -> Option<BTreeNode> {
        let mut buf = [0u8; 4096]; //
        self.file.seek(io::SeekFrom::Start(offset.try_into().unwrap())); 
        self.file.read_exact(&mut buf);
        let node = self.deserialize(& buf);
        Some(node)
    }    

    /// Appends write request information to the write-ahead log (WAL). Because appending to a file is 
    /// much quicker than overriding a portion of an existing file, the WAL acts as a countermeasure in case 
    /// the system crashes before it can flush any changes to the disk. Upon re-starting, if there 
    /// are any writes still within the WAL, those requests will be re-executed. 
    fn write_to_wal(&mut self, key: u16, val:u16){
        self.wal.write_all(&key.to_le_bytes()).unwrap();
        self.wal.write_all(&val.to_le_bytes()).unwrap();

        println!("Wrote key and val {} {} to WAL", key, val);
    }   

    /// Deserializes the sequence of bytes retreived from the disk into BTreeNode so that the 
    /// data is usable by the application. 
    fn deserialize(&self, buf: &[u8; 4096]) -> BTreeNode {
        let id = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        let leaf = buf[4];
        let num_keys = u16::from_le_bytes(buf[5..7].try_into().unwrap());

        let mut keys = Vec::new();
        let mut children = Vec::new();
        let mut vals = Vec::new();

        for i in 0..num_keys {
            let offset: usize = (7+i*2).into();
            let key = u16::from_le_bytes(buf[offset..offset+2].try_into().unwrap());
            keys.push(key);
        }

        match leaf == 1 {
            true => {
                for i in 0..num_keys {
                    let offset: usize = (7+num_keys*2+i*2).into();
                    let val = u16::from_le_bytes(buf[offset..offset+2].try_into().unwrap());
                    vals.push(val);
                }
            }
            false => {
                for i in 0..num_keys {
                    let offset: usize = (7+num_keys*2+i*4).into();
                    let child = u32::from_le_bytes(buf[offset..offset+4].try_into().unwrap());
                    children.push(child);
                }        
            }
        }

        let node = BTreeNode::new_from_params(id, leaf, num_keys, keys, children, vals);

        node
    }       

    /// Writes the key-value pair to the WAL and the appropriate B-Tree Node and stores those changes in the dirty pages buffer. 
    /// If the node becomes full, the tree will call the rebalance () function to split the node into two and update the parent 
    /// node. 
    pub fn write(&mut self, key: u16, val: u16) {
        self.write_to_wal(key, val);

        //Load the root node from the file. The root will always be at the start so the offset is 0. 
        let mut offset: u32 = 0;
        let mut stack = vec![];

        loop {
            stack.push(offset);
            let cur_node = self.get_mut(offset);
            let index:usize = cur_node.search(key);

            if cur_node.leaf == 1{
                //No longer just borrowing the object, we need owernship as the obejct moves from cache to buffer. 
                let mut removed_node = self.get_object(offset);
                let numk : usize = removed_node.num_keys.into();
                
                if numk > index && removed_node.keys[index] == key {
                    //Update the key if it already exists. Check that index is valid before trying to acess it. 
                    removed_node.vals[index] = val;
                } else {
                    removed_node.keys.insert(index.try_into().unwrap(), key);
                    removed_node.vals.insert(index.try_into().unwrap(), val);
                    removed_node.num_keys += 1;
                }

                let num_keys = removed_node.num_keys;
                self.dirty_pages.insert(offset, removed_node);

                if num_keys == BRANCHING_FACTOR {
                    self.rebalance(stack);
                }

                break;
            } else {
                offset = cur_node.children[index];
            }
        }
    }

    /// Flushes all the modified nodes to the disk then clears the WAL and dirty pages buffer. 
    pub fn flush(&mut self){
        for node in self.dirty_pages.values_mut() {
            node.write_node_to_file(&mut self.file);
        }

        self.reset_wal();
        self.dirty_pages.clear();
        println!("Successfully flushed changes to disk");
    }

    /// Reset WAL is called upon flushing all changed nodes to the disk. Because the changes have been persisted,
    /// there is no longer a need to keep track of the writes we have made. 
    fn reset_wal(&mut self) {
        self.wal.set_len(0);
        self.wal.rewind();
    }

    fn read_from_wal(&mut self, offset: u64) -> (u16, u16) {
        let mut buf = [0u8; 4]; //
        self.wal.seek(io::SeekFrom::Start(offset)); 
        self.wal.read_exact(&mut buf);
        let key = u16::from_le_bytes(buf[0..2].try_into().unwrap());
        let val = u16::from_le_bytes(buf[2..4].try_into().unwrap());

        println!("Recovered operation write {} {}", key, val);
        (key, val)
    }

    /// Recover is called upon startup. Checks the WAL in case the database had crashed previously. 
    /// If WAL is not empty, then recovers the lost changes by executing all operations written to the WAL. 
    pub fn recover(&mut self) {
        self.print_root();

        let mut wal_len = self.wal.metadata().unwrap().len() ;
        println!("Length of WAL is {}", wal_len);
        
        if wal_len == 0 {
            return 
        } else {
            let mut offset = 0;

            loop {

                if offset + 4 > wal_len {
                    break;
                }

                let (key, val) = self.read_from_wal(offset);
                self.write(key, val);
                offset = offset + 4;
            }

            self.reset_wal();
        }
    }

    pub fn print_root(&mut self) -> () {
        let root = self.get(0);
        println!("root id : {}", root.id);
        println!("root leaf : {}", root.leaf);
        println!("root num_keys : {}", root.num_keys);

        for key in &root.keys {
            println!("root keys : {}", key);
        }
        for child in &root.children {
            println!("root child node id : {}", child);
        }
        for val in &root.vals {
            println!("root val : {}", val);
        }
    }

    /// Called when any node reaches 4096 bytes. The full node gets split into two and the parent node is updated to include
    /// a reference to the new child node created and the key value where it begins. If the parent is full as well, this process
    /// is repeated until all nodes within the tree are below 4096 bytes. 
    fn rebalance (&mut self, mut stack: Vec<u32>) {
        let mut split_nodes: Option<Vec<u32>> = None;
        let mut insert_key:Option<u16> = None;

        loop { 
        
            let remaining_nodes = stack.len();

            if remaining_nodes == 0 && split_nodes.is_none() {
                break;
            } else if remaining_nodes == 0 && !split_nodes.is_none() {
                //Root node reached capacity so it got split into two and a new root node needs to be created. 
                let unwrapped_split_nodes = split_nodes.unwrap();
                let mut new_root = BTreeNode::new();
                new_root.leaf = 0;
                new_root.num_keys = 1;
                new_root.keys.push(insert_key.unwrap());
                new_root.children.push(unwrapped_split_nodes[0]);
                new_root.children.push(unwrapped_split_nodes[1]);

                //swap ids so that new_root is at start of file. 
                new_root.id = 0;
                self.dirty_pages.insert(0, new_root);
                
                break;
            } 

            //Unwrap the next node on the stack
            let offset = stack.pop().unwrap();
            let mut unwrapped_current_node = self.get_object(offset);
            let mut current_node_id = unwrapped_current_node.id;

            //Check if the child node was split into two. If so, then the current (parent) node needs to add a new 
            //key at the point of split. 
            let mut modified = false;

            if !split_nodes.is_none() {
                let index = unwrapped_current_node.search(insert_key.unwrap());
                unwrapped_current_node.keys.insert(index, insert_key.unwrap());
                unwrapped_current_node.children.insert(index+1, split_nodes.unwrap()[1]);
                unwrapped_current_node.num_keys +=1 ;
                modified = true;
            }

            //Check if the current node has reached capacity. 
            if unwrapped_current_node.num_keys == BRANCHING_FACTOR {
                unwrapped_current_node.num_keys = 2;
                let new_node_id :u32 = self.num_nodes.try_into().unwrap();
                let new_node_leaf = unwrapped_current_node.leaf;
                let new_node_keys = unwrapped_current_node.keys.split_off((BRANCHING_FACTOR/2).into());
                let new_node_chilren = match new_node_leaf == 1 {
                    true => Vec::new(),
                    false => unwrapped_current_node.children.split_off((BRANCHING_FACTOR/2).into()),
                };

                let new_node_vals = match new_node_leaf == 0 {
                    true => Vec::new(),
                    false => unwrapped_current_node.vals.split_off((BRANCHING_FACTOR/2).into()),
                };
                
                let new_node = BTreeNode::new_from_params(new_node_id, 
                    new_node_leaf, 
                    3,
                    new_node_keys, 
                    new_node_chilren, 
                    new_node_vals);
                self.num_nodes +=1;

                if current_node_id == 0 {
                    unwrapped_current_node.id = self.num_nodes.try_into().unwrap();
                    current_node_id = self.num_nodes.try_into().unwrap();
                    self.num_nodes +=1;
                }

                split_nodes = Some(vec![current_node_id, new_node_id]);
                insert_key = Some(new_node.keys[0]-1);

                self.dirty_pages.insert(current_node_id, unwrapped_current_node);
                self.dirty_pages.insert(new_node_id, new_node);
            } else {

                if modified {
                    self.dirty_pages.insert(current_node_id, unwrapped_current_node); 
                }

               break;
            }
        }
    }

    /// Searches the B-Tree for the specified key and returns the value found. 
    pub fn read(&mut self, key: u16) -> Option<u16> {
        let mut offset: u32 = 0;

        loop {

            let cur_node: &BTreeNode = self.get(offset);

            let index: usize = cur_node.search(key);

            if cur_node.leaf == 1{
                match cur_node.keys[index] == key {
                    true => return Some(cur_node.vals[index]),
                    false => return None,
                }
            } else {
                offset = cur_node.children[index];
            }
        }
    }   
}