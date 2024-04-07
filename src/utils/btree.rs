const BRANCHING_FACTOR: u16 = 500;
const LEVELS: usize = 3;
const MAX_PAGE_BYTES: usize = 4096;

extern crate linked_hash_map;

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
        /*let old_node = self.map.get(&key).unwrap();
        let old_keys = Vec::new();
        let old_children = Vec::new();
        let old_vals = Vec::new();


        let new_node = BTreeNode::new_from_params(old_node.id, 
            old_node.leaf, 
            old_node.num_keys, 
            old_node.keys,
            old_node.children, 
            old_node.vals); */

        self.map.get(&key).unwrap()
        
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

pub struct BTree {
     file : File,
     wal : File, 
     cache : LRUCache,
     num_nodes : u64
}

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
        let metadata = file.metadata()?;
        let num_nodes = metadata.len()/4096;

        Ok(Self { file, wal, cache, num_nodes})   
    }

    //Encodes key into a number to handle different input types. 
    fn encode_key(&self, key: &str){

    }

    fn read_node_from_file(&mut self, offset: usize) -> Option<BTreeNode> {
        let mut buf = [0u8; 4096]; //
        self.file.seek(io::SeekFrom::Start(0)); 
        self.file.read_exact(&mut buf);
        let node = self.deserialize(& buf);
        Some(node)
    }    

    fn write_node_to_file(&mut self, node: &BTreeNode) {
        let offset: u64 = (4096*(node.id)).into();
        let buffer = self.serialize(&node);
        self.file.seek(io::SeekFrom::Start(offset)); 
        self.file.write_all(&buffer);
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

    pub fn serialize(&self, node: &BTreeNode) -> Vec<u8> {
        let mut buffer = Vec::new();

        buffer.write_all(&node.id.to_le_bytes()).unwrap();
        buffer.write_all(&node.leaf.to_le_bytes()).unwrap();
        buffer.write_all(&node.num_keys.to_le_bytes()).unwrap();

        for &key in &node.keys {
            let bytes = key.to_le_bytes();
            buffer.write_all(&bytes).unwrap();
        }

        match node.leaf == 1 {
            true => {
                for &val in &node.vals {
                    let bytes = val.to_le_bytes();
                    buffer.write_all(&bytes).unwrap();
                }        
            }, 

            false => {
                for &child in &node.children {
                    let bytes = child.to_le_bytes();
                    buffer.write_all(&bytes).unwrap();
                }        

            }
        }

        let padding_len = 4096 - buffer.len();
        buffer.extend_from_slice(&vec![0u8; padding_len]);  

        buffer      
    }


    pub fn write(&mut self, key: u16, val: u16) {
        self.write_to_wal(key, val);

        //Load the root node from the file. The root will always be at the start so the offset is 0. 
        let mut offset: u32 = 0;
        let mut stack = vec![];

        loop {
            stack.push(offset);
            let mut cur_node = self.read_node_from_file(offset.try_into().unwrap()).unwrap();
            let index:usize = cur_node.search(key);

            if cur_node.leaf == 1{
                let numk : usize = cur_node.num_keys.into();
                if numk > index && cur_node.keys[index] == key {
                    cur_node.vals[index] = val;
                } else {
                    cur_node.keys.insert(index.try_into().unwrap(), key);
                    cur_node.vals.insert(index.try_into().unwrap(), val);
                    cur_node.num_keys += 1;
                }

                if cur_node.num_keys == BRANCHING_FACTOR {
                    self.rebalance(stack);
                } else {
                    self.write_node_to_file(&cur_node);
                }

                break;
            } else {
                offset = cur_node.children[index];
            }
        }
    }

    fn swap(&mut self, new_root : BTreeNode){
        let old_root = self.read_node_from_file(0).unwrap();
        self.write_node_to_file(&new_root);
        self.write_node_to_file(&old_root);
    }

    fn rebalance (&mut self, mut stack: Vec<u32>) {
        let mut split_nodes: Option<Vec<u32>> = None;
        let mut insert_key:Option<u16> = None;

        loop {
            let offset = stack.pop().unwrap();
            let current_node = self.read_node_from_file(offset.try_into().unwrap());

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

            let mut unwrapped_current_node = current_node.unwrap();

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
                self.write_node_to_file(&unwrapped_current_node);
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
                self.write_node_to_file(&unwrapped_current_node);
                self.write_node_to_file(&new_node);

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
            
            let cur_node = self.read_node_from_file(offset.try_into().unwrap()).unwrap();
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