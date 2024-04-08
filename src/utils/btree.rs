const BRANCHING_FACTOR: u16 = 500;
const LEVELS: usize = 3;
const MAX_PAGE_BYTES: usize = 4096;

extern crate linked_hash_map;

use std::collections::HashMap;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::{self, Read, Write, Seek};
use std::convert::TryInto;

use self::linked_hash_map::LinkedHashMap;


//logic. 
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

pub struct BTreeNode {
    id: u32, 
    leaf: u8, 
    num_keys: u16, 

    keys: Vec<u16>, 
    children: Vec<u32>,
    vals: Vec<u16>, 
}

//user starts session
//open the file
//if user asks for a read request
//call read(key, file)

impl BTreeNode {
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

    fn write_node_to_file(&self, file: &mut File) {
        let offset: u64 = (4096*(self.id)).into();
        let buffer = self.serialize();
        file.seek(io::SeekFrom::Start(offset)); 
        file.write_all(&buffer);
    }

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

pub struct BTree {
    file : File,
    wal : File, 
    cache : LRUCache,
    dirty_pages : HashMap<u32, BTreeNode>, 
    num_nodes : u64
}

impl BTree{
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

    //Encodes key into a number to handle different input types. 
    fn encode_key(&self, key: &str){

    }

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

    fn get_object(&mut self, key: u32) -> BTreeNode {
        return self.cache.map.remove(&key).unwrap();
    }


    fn read_node_from_file(&mut self, offset: usize) -> Option<BTreeNode> {
        let mut buf = [0u8; 4096]; //
        self.file.seek(io::SeekFrom::Start(offset.try_into().unwrap())); 
        self.file.read_exact(&mut buf);
        let node = self.deserialize(& buf);
        Some(node)
    }    

    fn write_to_wal(&mut self, key: u16, val:u16){
        self.wal.write_all(&key.to_le_bytes()).unwrap();
        self.wal.write_all(&val.to_le_bytes()).unwrap();
    }   

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
                    removed_node.vals[index] = val;
                } else {
                    removed_node.keys.insert(index.try_into().unwrap(), key);
                    removed_node.vals.insert(index.try_into().unwrap(), val);
                    removed_node.num_keys += 1;
                }

                if removed_node.num_keys == BRANCHING_FACTOR {
                    self.rebalance(stack);
                } else {
                    self.dirty_pages.insert(offset, removed_node);
                }

                break;
            } else {
                offset = cur_node.children[index];
            }
        }
    }

    fn swap(&mut self, new_root : BTreeNode){
        //let old_root = self.get(0);
        //old_root.write_node_to_file(file);
    }

    fn flush(&mut self){
        for node in self.dirty_pages.values_mut() {
            node.write_node_to_file(&mut self.file);
        }
    }

    fn rebalance (&mut self, mut stack: Vec<u32>) {
        let mut split_nodes: Option<Vec<u32>> = None;
        let mut insert_key:Option<u16> = None;

        loop {
            let offset = stack.pop().unwrap();

            let current_node = self.cache.get_mut(offset);

            if current_node.is_none() && split_nodes.is_none() {
                break;
            } else if current_node.is_none() && !split_nodes.is_none() {
                //Root node reached capacity so it got split into two and a new root node needs to be created. 
                let unwrapped_split_nodes = split_nodes.unwrap();
                let mut new_root = BTreeNode::new();

                new_root.keys.push(insert_key.unwrap());

                new_root.children.push(unwrapped_split_nodes[0]);
                new_root.children.push(unwrapped_split_nodes[1]);
                self.swap(new_root);
                break;
            } 

            let unwrapped_current_node = current_node.unwrap();

            if !split_nodes.is_none() {
                //Node reached capacity and was split so the the range of keys in the parent 
                //needs to update with an additional key. 

                //Find the index of the old node and insert newNode id at index+1
                //insert new key at 

                //old child node has key ranging from i to k before split. 
                //now it's i to j and j+1 to k for each child. 

                //in current node locate where i is in keys. insert j+1 at index+1. 
                //in children, insert new child at index+2. 
                let index = unwrapped_current_node.search(insert_key.unwrap());
                unwrapped_current_node.keys.insert(index, insert_key.unwrap());
                unwrapped_current_node.children.insert(index+1, split_nodes.unwrap()[1]);
                unwrapped_current_node.num_keys +=1 ;
                unwrapped_current_node.write_node_to_file(&mut self.file);
            }

            if unwrapped_current_node.num_keys == BRANCHING_FACTOR {
                //Current node reached capacity and needs to be split. 
                let new_node_id :u32 = self.num_nodes.try_into().unwrap();
                let new_node_leaf = unwrapped_current_node.leaf;
                let new_node_keys = unwrapped_current_node.keys.split_off((BRANCHING_FACTOR/2).into());
                let new_node_chilren = match new_node_leaf == 1 {
                    true => Vec::new(),
                    false => unwrapped_current_node.children.split_off((BRANCHING_FACTOR/2).into()),
                };

                let new_node_vals = match new_node_leaf == 1 {
                    true => Vec::new(),
                    false => unwrapped_current_node.vals.split_off((BRANCHING_FACTOR/2).into()),
                };
                
                let new_node = BTreeNode::new_from_params(new_node_id, 
                    new_node_leaf, 
                    BRANCHING_FACTOR/2,
                    new_node_keys, 
                    new_node_chilren, 
                    new_node_vals);

                self.num_nodes +=1;
                unwrapped_current_node.write_node_to_file(&mut self.file);
                new_node.write_node_to_file(&mut self.file);

                split_nodes = Some(vec![unwrapped_current_node.id, new_node.id]);
                insert_key = Some(unwrapped_current_node.keys[0]);

            } else {
                split_nodes = None;
                insert_key = None;
            }
        }
    }

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