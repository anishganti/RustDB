const BRANCHING_FACTOR: u16 = 5;
const LEVELS: usize = 3;
const MAX_PAGE_BYTES: u16 = 4096;
const MIN_PAGE_BYTES: u16 = 4096;


extern crate linked_hash_map;

use btree::cache::LRUCache;
use btree::node::{NodeInfo, BTreeNode};

use std::collections::HashMap;
use std::fs::File;
use std::fs::OpenOptions;
use std::hash::Hash;
use std::io::{self, Read, Write, Seek};
use std::convert::TryInto;

/// Main database structure that holds the cache for easy access and the file for disk reads/writes. 
pub struct BTree {
    file : File,
    wal : File, 
    cache : LRUCache,
    dirty_pages : HashMap<u32, BTreeNode>, 
    num_nodes : u32
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
        let num_nodes:u32 = (metadata.len()/4096).try_into().expect(
            "Conversion error: u64 to u32. There are more than 
            4294967295 nodes meaning the file on disk was externally modified.
            Please pass in a valid database file.");

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
            return self.cache.remove(key);
        } else if self.dirty_pages.contains_key(&key) {
            return self.dirty_pages.remove(&key).unwrap();
        } else {
            let node = self.read_node_from_file(key.try_into().unwrap()).unwrap();
            return node;           
        }
    }

    /// Returns a map containing information about a node depending on requested fields. 
    /// 
    /// This method is preferable to calling .get() since the latter returns a reference, meaning that the entire BTree 
    /// class was locked until the node reference was released. This class will return information about a node to the user
    /// and release the node reference so other class methods can be called. Additionally, by passing in a parameter of fields
    /// needed from the start, the user can avoid having to re-search every time an additional field is needed. 
    fn get_node_info(&mut self, node_id: u32, fields: Vec<&str>, index: Option<usize>) -> HashMap<String, NodeInfo> {
        let node = self.get(node_id);
        let mut node_info = HashMap::<String, NodeInfo>::new();
 
        for field in fields {
            if let Some(field_value) = node.get_field_info(field, index.unwrap()) {
                node_info.insert(field.to_string(), field_value);
            } else {
                println!("Invalid field: {}", field);
            }        
        }

        node_info
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
                    self.handle_overflow(stack);
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
        let wal_len = self.wal.metadata().unwrap().len() ;
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

    /// Rebalances the B-tree when any node reaches 4096 bytes. 
    /// 
    /// The full node gets split into two and the parent node is updated to include
    /// a reference to the new child node created and the key value where it begins. 
    /// 
    /// If the parent is also full, this process is repeated until all nodes are less than 4096 bytes. 
    fn handle_overflow (&mut self, mut stack: Vec<u32>) {
        let mut split_nodes: Option<Vec<u32>> = None;
        let mut insert_key:Option<u16> = None;

        loop { 
        
            if stack.is_empty() {
                if !split_nodes.is_none() {
                    self.create_new_root(split_nodes, insert_key);        
                }
                break;        
            } 

            let offset = stack.pop().unwrap();
            let mut cur_node = self.get_object(offset);

            let modified = self.update_parent_node(&mut cur_node, insert_key, split_nodes);

            //Check if the current node has reached capacity. 
            if cur_node.num_keys == BRANCHING_FACTOR {
                let (split_nodes_cpy, 
                    insert_key_cpy, 
                    new_node) = self.split(&mut cur_node);

                split_nodes = split_nodes_cpy;
                insert_key = insert_key_cpy;

                self.dirty_pages.insert(cur_node.id, cur_node);
                self.dirty_pages.insert(new_node.id, new_node);
            } else {

                if modified {
                    self.dirty_pages.insert(cur_node.id, cur_node); 
                }

               break;
            }
        }
    }

    fn create_new_root(&mut self, split_nodes : Option<Vec<u32>>, insert_key : Option<u16>) -> () {
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
    }

    fn update_parent_node(&mut self, node : &mut BTreeNode, insert_key : Option<u16>, split_nodes : Option<Vec<u32>>) -> bool {
        let modified = !split_nodes.is_none();

        if modified {
            let index = node.search(insert_key.unwrap());
            node.keys.insert(index, insert_key.unwrap());
            node.children.insert(index+1, split_nodes.unwrap()[1]);
            node.num_keys +=1; 

        }
        modified
    }

    fn split(&mut self, cur_node: &mut BTreeNode) -> (Option<Vec<u32>>, Option<u16>, BTreeNode) {
        let is_leaf = cur_node.leaf == 1;
        let split_index: usize = (BRANCHING_FACTOR/2).try_into().unwrap();
        cur_node.num_keys = BRANCHING_FACTOR/2;
    
        // Split keys, children, and values
        let (new_node_keys, new_node_children, new_node_vals) = {
            let (keys, children, vals) = (&mut cur_node.keys, &mut cur_node.children, &mut cur_node.vals);
            (
                keys.split_off(split_index),
                if is_leaf { Vec::new() } else { children.split_off(split_index) },
                if is_leaf { vals.split_off(split_index) } else { Vec::new() },
            )
        };
    
        // Create a new node
        let new_node_id = self.num_nodes;
        let new_node = BTreeNode::new_from_params(
            new_node_id,
            if is_leaf {1} else {0},
            new_node_keys.len().try_into().unwrap(),
            new_node_keys,
            new_node_children,
            new_node_vals,
        );
    
        // Update node IDs
        self.num_nodes += 1;
        if cur_node.id == 0 {
            cur_node.id = self.num_nodes;
            self.num_nodes += 1;
        }
    
        // Prepare return values
        let split_nodes = Some(vec![cur_node.id, new_node_id]);
        let insert_key = Some(new_node.keys[0] - 1);
    
        (split_nodes, insert_key, new_node)
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

    /// Searches the B-Tree for the specified key and removes the key-value pair if found. 
    pub fn delete(&mut self, key: u16) ->  () {
        //Look to locate the deleted key in the leaf nodes.
        // Delete the key and its associated value if the key is discovered in a leaf node.
        // One of the following steps should be taken if the node underflows (number of keys is less than half the maximum allowed):
        //     Get a key by borrowing it from a sibling node if it contains more keys than the required minimum.
        //     If the minimal number of keys is met by all of the sibling nodes, merge the underflow node with one of its siblings and modify the parent node as necessary.
        // Remove all references to the deleted leaf node from the internal nodes of the tree.
        // Remove the old root node and update the new one if the root node is empty.

        //The root will always be at the start of the file so the offset is 0. 
        let mut offset: u32 = 0;
        let mut stack = vec![];

        loop {
            let cur_node = self.get_mut(offset);

            let index: usize = cur_node.search(key);

            stack.push((offset, index));

            if cur_node.leaf == 1{
                if cur_node.keys[index] == key {
                    cur_node.keys.remove(index);
                    cur_node.vals.remove(index);
                    cur_node.num_keys -= 1;

                    if cur_node.num_keys < MIN_PAGE_BYTES {
                        self.handle_underflow(stack);
                        break;
                    }
                }   
            } else {
                offset = cur_node.children[index];
            }
        }

    }

    fn check_underflow(&mut self, node_id: u32) -> bool {
        let mut underflow = false;

        let node = self.get(node_id);

        if node.num_keys < MIN_PAGE_BYTES {
            underflow = true;
        }

        underflow
    }

    fn handle_underflow (&mut self, mut stack: Vec<(u32, usize)>) -> () {
        //Starting from the deepest node. The algorithm will look something like this: 
        //if the current node has an underflow, go to the parent node. From the parent node
        // check if any of the child's adjacent nodes has more nodes/keys than minimum. If so, 
        // then move exactly one key . Remove the extra key
        // from the parent's list and also remove the node count. The physical node removed also should be
        // swapped from the lsat node on the disk in order to avoid fragmentation. If the parent is now underflowing, 
        // repeat the process. If the root is underflowing, that is ok, so we can break if stack is empty after popping. 

        let mut underflow = true;

        loop { 
        
            let (parent_id, index)  = stack.pop().unwrap();
            let parent_info = self.get_node_info(parent_id, vec!["str"], Some(index));
            let parent_is_leaf = parent_info.get("is_leaf").expect("Boolean").as_bool();

            if stack.is_empty() || underflow == false {
                break;        
            } else if parent_is_leaf {
                continue;
            } 

            let has_siblings = parent_info.get("has_siblings").expect("msg").as_bool();
            let child_id = parent_info.get("child_id").expect("msg").as_u32();
            let child = self.get_object(child_id);

            if has_siblings {
                let (sibling, has_extra_keys, dir) = self.select_sibling(&parent_info);

                if has_extra_keys == false {
                    self.merge(sibling, child);
                } else {
                    self.shift(sibling, child, dir);
                }

            } else {
                let parent = self.get_object(parent_id);
                self.merge(parent, child);
            }

            underflow = self.check_underflow(parent_id);
        }
            
    }
    /// Returns the sibling node with most extra-keys to remove keys from. If no sibling has extra keys
    /// to give up to the child node, then a sibling node to merge child node into. 
    fn select_sibling(&mut self, parent_info : &HashMap<String, NodeInfo>) -> (BTreeNode, bool, char) {
        let mut has_extra_keys = true;
        let mut sibling_id = None;
        let mut dir = 'r'; 

        if parent_info.contains_key("right_child") && !parent_info.contains_key("left_child") {
            let right_sibling_id = parent_info.get("right_child_id").expect("msg").as_u32();
            let right_sibling_info = self.get_node_info(right_sibling_id, vec!["num_keys"], None);
            let right_sibling_num_keys = right_sibling_info.get("num_keys").expect("msg").as_u16();
            has_extra_keys = right_sibling_num_keys > MAX_PAGE_BYTES;
            sibling_id = Some(right_sibling_id);
        } else if !parent_info.contains_key("right_child") && parent_info.contains_key("left_child")  {
            let left_sibling_id = parent_info.get("left_child_id").expect("msg").as_u32();
            let left_sibling_info = self.get_node_info(left_sibling_id, vec!["num_keys"], None);
            let left_sibling_num_keys = left_sibling_info.get("num_keys").expect("msg").as_u16();
            has_extra_keys = left_sibling_num_keys > MAX_PAGE_BYTES;
            sibling_id = Some(left_sibling_id);
            dir = 'l';
        } else {
            let right_sibling_id = parent_info.get("right_child_id").expect("msg").as_u32();
            let right_sibling_info = self.get_node_info(right_sibling_id, vec!["num_keys"], None);
            let right_sibling_num_keys = right_sibling_info.get("num_keys").expect("msg").as_u16();

            let left_sibling_id = parent_info.get("left_child_id").expect("msg").as_u32();
            let left_sibling_info = self.get_node_info(left_sibling_id, vec!["num_keys"], None);
            let left_sibling_num_keys = left_sibling_info.get("num_keys").expect("msg").as_u16();

            if left_sibling_num_keys > right_sibling_num_keys {
                sibling_id = Some(left_sibling_id);
                has_extra_keys = left_sibling_num_keys > MAX_PAGE_BYTES;
                dir = 'l';
            } else {
                sibling_id = Some(right_sibling_id);
                has_extra_keys = right_sibling_num_keys > MAX_PAGE_BYTES;
            }


        }
        let sibling = self.get_object(sibling_id.unwrap());
        (sibling, has_extra_keys, dir)
    }

    /// Merge node since it has less keys than the MIN value and cannot borrow from sibling. 
    ///
    /// It can either merge with a sibling node, or if none exist, the parent node. 
    /// This will convert the parent node into a leaf node. 
    fn merge(&mut self, mut sibling : BTreeNode, mut child: BTreeNode){

    }

    /// Shift value or child node from sibling to child. Parent needs to be updated to 
    /// reflect in change in key-range. 
    fn shift(&mut self, mut sibling : BTreeNode, mut child: BTreeNode, dir : char){
        
        let (key_to_shift, insert_key) = match dir {
            'l' => (sibling.keys.pop().unwrap(), 0),
            'r' => (sibling.keys.remove(0), child.num_keys),
            _ => (sibling.keys.pop().unwrap(), 0),
        };

        let insert_key : usize = insert_key.into();

        sibling.num_keys = sibling.num_keys - 1;
        child.num_keys = child.num_keys + 1; 
        child.keys.insert(insert_key, key_to_shift);

        if sibling.is_leaf() {
            let val_to_shift = match dir {
                'l' => sibling.vals.pop().unwrap(),
                'r' => sibling.vals.remove(0),
                _ => sibling.vals.pop().unwrap(),
            };

            child.vals.insert(insert_key, val_to_shift);
            
        } else {
            let node_to_shift = match dir {
                'l' => sibling.children.pop().unwrap(),
                'r' => sibling.children.remove(0),
                _ => sibling.children.pop().unwrap(),
            };

            child.children.insert(insert_key, node_to_shift);
        }

        self.dirty_pages.insert(sibling.id, sibling);
        self.dirty_pages.insert(child.id,  child);
    }


}

#[cfg(test)]
mod tests {
    // Note this useful idiom: importing names from outer (for mod tests) scope.

    #[test]
    fn test_add() {
        assert_eq!(true, true)
    }

    #[test]
    fn test_bad_add() {
        // This assert would fire and test will fail.
        // Please note, that private functions can be tested too!
        assert_eq!(true, false)
    }
}