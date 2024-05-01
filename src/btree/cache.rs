extern crate linked_hash_map;
use self::linked_hash_map::LinkedHashMap;

use btree::node::BTreeNode;

/// An in-memory cache to hold the most recently accessed pages. 
/// and reduce the number of I/O operations. Uses Least Recently Used (LRU)
/// method to evict old pages if the cache is full. 


pub struct LRUCache {
    pub map: LinkedHashMap<u32, BTreeNode>
}

impl LRUCache {
    pub fn new() -> Self {
       Self { 
        map : LinkedHashMap::new()
       }
    }

    pub fn insert(&mut self, key: u32, val: BTreeNode){
        if(self.map.contains_key(&key)){
            self.map.remove(&key);
        } else if (self.map.len() == 10){
            self.map.pop_back();
        } 

        self.map.insert(key, val);
    }

    pub fn get(&self, key: u32) -> &BTreeNode{
        self.map.get(&key).unwrap()
    }

    pub fn get_mut(&mut self, key: u32) -> Option<&mut BTreeNode>{
        self.map.get_mut(&key)
    }


    pub fn contains_key(&self, key: u32) -> bool {
        self.map.contains_key(&key)
    }

    pub fn remove(&mut self, key: u32) -> BTreeNode {
        self.map.remove(&key).unwrap()
    }

}

