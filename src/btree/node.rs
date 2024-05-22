use std::io::{self, Write, Seek};
use std::fs::File;

/// The structure of the a single node (page) within the overall B-Tree. 
/// Contains either the key-value stores (if leaf node) or key ranges with locations to the child nodes. 
pub struct BTreeNode {
    pub id: u32, 
    pub leaf: u8, 
    pub num_keys: u16, 

    pub keys: Vec<u16>, 
    pub children: Vec<u32>,
    pub vals: Vec<u16>, 
}

/// Enum to list different value-types a field in Node can contain. Used in BTree's get_node_info() method. 
pub enum NodeInfo {
    U16(u16),
    U32(u32),
    Bool(bool),
}

impl NodeInfo {
    pub fn as_u32(&self) -> u32 {
        if let NodeInfo::U32(val) = *self {
            val
        } else {
            panic!("Expected a u32 value")
        }
    }

    pub fn as_u16(&self) -> u16 {
        if let NodeInfo::U16(val) = *self {
            val
        } else {
            panic!("Expected a u16 value")
        }
    }

    pub fn as_bool(&self) -> bool {
        if let NodeInfo::Bool(bool) = *self {
            bool
        } else {
            panic!("Expected a boolean value")
        }
    }
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

    ///         
    pub fn get_field_info(&self, field: &str, index: usize) -> Option<NodeInfo> {
        match field {
            "is_leaf"        => Some(NodeInfo::Bool(self.is_leaf())),
            "has_siblings"   => Some(NodeInfo::Bool(self.children.len() > 1)),
            "num_keys"       => Some(NodeInfo::U16(self.num_keys)),
            "node_id"        => Some(NodeInfo::U32(self.id)),
            "child_id"       => Some(NodeInfo::U32(self.children[index])),
            "left_child_id"  => Some(NodeInfo::U32(self.children[index-1])),
            "right_child_id" => Some(NodeInfo::U32(self.children[index+1])),
            _ => None, 
        }
    }



    /// Persists the current node to the disk. Borrows the file from the B-Tree itself and 
    /// calculates the file position from using the node's id and the fact that a node 
    /// is limited to be at most 4096 bytes. 
    pub fn write_node_to_file(&self, file: &mut File) {
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

    pub fn is_leaf(&self) -> bool {
        let mut is_leaf = false; 

        if self.leaf == 1 {
            is_leaf = true;
        }
        
        is_leaf
    }

}
