use std::rc::Rc;
use std::cell::RefCell;
use std::vec;

const BRANCHING_FACTOR: usize = 100;
const LEVELS: usize = 3;
const MAX_PAGE_BYTES: usize = 4096;

//logic. 
pub struct BTreeNode {
    keys: Vec<usize>, 
    children: Vec<BTreeNode>,
    values: Vec<usize>, 
    leaf: bool
}

pub struct BTree {
    root : BTreeNode
}

impl BTreeNode {
    pub fn new() -> Self {
        Self {
            keys: Vec::new(),
            children: Vec::new(),
            values: Vec::new(), 
            leaf: true
        }
    }    
}

impl BTree {

    pub fn new() -> Self {
        Self {
            root: BTreeNode::new(),
        }
    }    

    fn rebalance_tree (mut root : BTreeNode, key: usize) -> BTreeNode{
        let mut split_nodes: Option<Vec<BTreeNode>> = None;
        let mut stack: Vec<&mut BTreeNode> = Vec::new();
        let mut newr = BTreeNode::new();

        stack.push(&mut root);

        loop {
            let current_node: Option<&mut BTreeNode> = stack.pop();

            if current_node.is_none() && split_nodes.is_none() {
                break;
            } else if !split_nodes.is_none() && current_node.is_none() {
                //Root node reached capacity so it got split into two and a new root node needs to be created. 
                let mut new_root = BTreeNode::new();
                let mut split_nodes = split_nodes.unwrap();
                let key1 = split_nodes[0].keys[0];
                let key3 = split_nodes[1].keys[124]+1;
                let key2 = (key1+key3)/2;

                new_root.keys.push(key1);
                new_root.keys.push(key2);
                new_root.keys.push(key3);

                new_root.children.push(split_nodes.swap_remove(0));
                new_root.children.push(split_nodes.swap_remove(1));
                newr = new_root;                 
            } else if !split_nodes.is_none() && !current_node.is_none() {
                //Node reached capacity and was split so the the range of keys in the parent 
                //needs to update with an additional key. 

            }
            if current_node.unwrap().keys.len() == BRANCHING_FACTOR {
                split_nodes = Some(Vec::<BTreeNode>::new());
            } else {
                split_nodes = None;
            }
        }
        root = newr;
        root
    }

    pub fn read(& self, key: usize) -> Option<usize> {
        let mut value : Option<usize> = None;
        let mut stack = Vec::<& BTreeNode>::new();
        stack.push(&self.root);

        loop {
            if stack.is_empty() {
                break;
            } else {
                let current_node = stack.pop().unwrap();

                if key < current_node.keys[0] || key >  current_node.keys[current_node.keys.len()-1] {
                    break;
                }
        
                let (index)  = Self::binary_search(key, &current_node.keys);   
                
                if !current_node.leaf {
                    stack.push(&current_node.children[index-1]);
                } else {
                    if key == current_node.keys[index] {
                        value = Some(current_node.values[index]);
                    } 
                }
                      
            }
        }

        value
    }

    fn binary_search(key: usize, keys: &Vec<usize>) -> usize {
        if keys.len() == 0 {
            return 0
        }

        let mut left = 0;
        let mut right = keys.len()-1;
        let mut mid = 0;

        loop {
            mid = (left+right)/2;

            if left > right {
                mid = left;
                break;
            } else if keys[mid] == key {
                break;
            } else if key > keys[mid] {
                left = mid+1; 
            } else {
                right = mid-1;
            }
        }

        mid
    }

    pub fn write(mut self, key: usize, value: usize) -> BTree {
        let mut current_node = Some(& mut self.root);

        loop {
            if current_node.is_none(){
                break;
            } 
    
            let node = current_node.unwrap();
            let mut num_keys = node.keys.len();
            
            if num_keys == 0 || node.leaf{
                let (index) = Self::binary_search(key, &node.keys);

                node.keys.insert(index, key);
                node.values.insert(index, value);
                num_keys += 1;

                if num_keys == BRANCHING_FACTOR {
                    self.root = Self::rebalance_tree(self.root, key);
                }   
                break;
            } else{
                let index = Self::binary_search(key, &node.keys);
                //TODO FIX INDEXING ISSUES
                if key < node.keys[0] {
                    node.keys[0] = key;
                } else if key >= node.keys[num_keys-1] {
                    node.keys[num_keys-1] = key+1;
                }
                
                current_node = Some(& mut (node.children[index-1]));
            }
        }

        self
    }

}