const BRANCHING_FACTOR: i32 = 250;
const LEVELS: i32 = 3;
const MAX_PAGE_BYTES: i32 = 4000;

//logic. 
pub struct BTreeNode {
    keys: Vec<usize>, 
    children: Vec<BTreeNode>, 
    values: Vec<usize>, 
    leaf: bool
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
    
    pub fn write(&mut self, key: usize, value: usize) -> Option<usize> {
        if self.keys.len() == 0 {
            self.keys.push(key);
            self.values.push(value);
        }

        else if key < self.keys[0] || key > self.keys[self.keys.len()-1] {
            if self.leaf { 
                self.keys.push(key);
                self.values.push(value);                   
            } else {
                let index = 1;
                //add int (key/100) * 100 to [200, 300, ... Nth Key] -> [100, 200, ... .N+1th key]
                //create and init new leaf node and add to cihldren [Child 0, ... Child N-2] -> [Child, 0Child 1 (0), ... Child N-1 {n-2}] if not leaf 
                let child = BTreeNode::new();
                self.children.insert(index, child);
                Self::write(&mut self.children[0], key, value);
            }
        } else {
            let index = Self::binary_search(key, &self.keys);

            if self.leaf {
                self.keys.insert(index, key);
                self.values.insert(index, value);
            } else {
                Self::write(&mut self.children[index-1], key, value);
            }
        }

        Some(key)
    }

    pub fn read(& self, key: usize) -> Option<usize> {
        println!("reader: {} {}", key, self.keys[0]);

        if key < self.keys[0] || key >  self.keys[self.keys.len()-1] {
            println!("read: {} {}", key, self.keys[0]);
            return None;
        }

        let index = Self::binary_search(key, &self.keys);   
        
        if !self.leaf {
            Self::read(&self.children[index-1], key)
        } else {
            if key == self.keys[index] {Some(self.values[index])} else {None}
        }
    }

    fn binary_search(key: usize, keys: &Vec<usize>) -> usize {
        let mut left = 0;
        let mut right = keys.len();
        let mut mid = (left+right)/2;

        while left < right {
            if keys[mid] == key {
                break;
            } else if (key > keys[mid]) {
                left = mid; 
            } else {
                right = mid-1;
            }
        }

        mid
    }
}
