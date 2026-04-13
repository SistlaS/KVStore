use crate::kvstore::{KVStoreError, KVStoreTrait};
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug)]
pub struct BTreeKVStore {
    store: Arc<RwLock<BTreeMap<String, String>>>,
}

impl BTreeKVStore {
    pub fn new() -> Self {
        BTreeKVStore {
            store: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }
}

#[async_trait::async_trait]
impl KVStoreTrait for BTreeKVStore {
    async fn put(&self, key: String, value: String) -> Result<bool, KVStoreError> {
        let mut store = self.store.write().await;
        let exists = store.insert(key, value).is_some();
        Ok(exists)
    }

    async fn swap(&self, key: String, value: String) -> Result<Option<String>, KVStoreError> {
        let mut store = self.store.write().await;
        let old_value = store.insert(key, value);
        Ok(old_value)
    }

    async fn get(&self, key: String) -> Result<Option<String>, KVStoreError> {
        let store = self.store.read().await;
        Ok(store.get(&key).cloned()) // Cloning is necessary to return owned data
    }

    async fn scan(
        &self,
        start_key: String,
        end_key: String,
    ) -> Result<Vec<(String, String)>, KVStoreError> {
        if start_key > end_key {
            return Ok(Vec::new());
        }
        let mut result = Vec::new();
        let store = self.store.read().await;
        let range = store.range(start_key..=end_key);
        for (key, value) in range {
            result.push((key.clone(), value.clone()));
        }
        Ok(result)
    }

    async fn delete(&self, key: String) -> Result<bool, KVStoreError> {
        let mut store = self.store.write().await;
        let exists = store.remove(&key).is_some();
        Ok(exists)
    }

    async fn len(&self) -> Result<usize, KVStoreError> {
        let store = self.store.read().await;
        Ok(store.len())
    }

    async fn is_empty(&self) -> Result<bool, KVStoreError> {
        let store = self.store.read().await;
        Ok(store.is_empty())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_create_new_kvstore() {
        let kvstore = BTreeKVStore::new();
        assert!(
            kvstore.is_empty().await.unwrap(),
            "KVStore should be empty upon creation"
        );
        assert_eq!(kvstore.len().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn test_put_and_get() {
        let store = BTreeKVStore::new();

        // PUT a key-value pair
        let ret = store
            .put("a".to_string(), "letter".to_string())
            .await
            .unwrap();
        assert!(!ret, "Key should not exist");

        // GET an existing key
        assert_eq!(
            store.get("a".to_string()).await.unwrap(),
            Some("letter".to_string())
        );

        // GET a non-existing key
        assert_eq!(store.get("b".to_string()).await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_swap() {
        let store = BTreeKVStore::new();
        let ret = store
            .put("a".to_string(), "letter".to_string())
            .await
            .unwrap();
        assert!(!ret, "Key should not exist");

        // SWAP an existing key and GET it
        let old_value = store
            .swap("a".to_string(), "vowel".to_string())
            .await
            .unwrap();
        assert_eq!(old_value, Some("letter".to_string()));
        assert_eq!(
            store.get("a".to_string()).await.unwrap(),
            Some("vowel".to_string())
        );

        // SWAP an non-existing key and GET it
        let old_value = store
            .swap("b".to_string(), "letter".to_string())
            .await
            .unwrap();
        assert_eq!(old_value, None); // No previous value
        assert_eq!(
            store.get("b".to_string()).await.unwrap(),
            Some("letter".to_string())
        );
    }

    #[tokio::test]
    async fn test_scan() {
        let store = BTreeKVStore::new();

        let mut vec = Vec::new();
        vec.push(("berry".to_string(), "blue".to_string()));
        vec.push(("date".to_string(), "brown".to_string()));
        vec.push(("apple".to_string(), "red".to_string()));
        vec.push(("carrot".to_string(), "orange".to_string()));
        vec.push(("banana".to_string(), "yellow".to_string()));
        vec.push(("1234".to_string(), "number".to_string()));

        // PUT a few key-value pairs in unsorted order
        for (key, value) in &vec {
            let _ = store.put(key.clone(), value.clone()).await;
        }

        // SCAN a partial range
        let scanned = store
            .scan("be".to_string(), "date".to_string())
            .await
            .unwrap();
        let expected = vec![
            ("berry".to_string(), "blue".to_string()),
            ("carrot".to_string(), "orange".to_string()),
            ("date".to_string(), "brown".to_string()),
        ];
        assert_eq!(scanned, expected);

        // SCAN a full range
        let scanned = store.scan("0".to_string(), "z".to_string()).await.unwrap();
        let mut expected = vec.clone();
        expected.sort_by_key(|k| k.0.clone()); // Result should be sorted
        assert_eq!(scanned, expected);

        // SCAN non-existing range
        let scanned = store.scan("2".to_string(), "99".to_string()).await.unwrap();
        let expected = vec![]; // Result empty
        assert_eq!(scanned, expected);

        // Edge cases: start_key > end_key
        let scanned = store.scan("z".to_string(), "a".to_string()).await.unwrap();
        let expected = vec![];
        assert_eq!(scanned, expected);

        // Edge cases: start_key == end_key
        let scanned = store
            .scan("carrot".to_string(), "carrot".to_string())
            .await
            .unwrap();
        let expected = vec![("carrot".to_string(), "orange".to_string())];
        assert_eq!(scanned, expected);
    }

    #[tokio::test]
    async fn test_delete() {
        let store = BTreeKVStore::new();
        store
            .put("a".to_string(), "letter".to_string())
            .await
            .unwrap();

        // DELETE existing key
        assert!(store.delete("a".to_string()).await.unwrap());
        assert_eq!(store.get("a".to_string()).await.unwrap(), None); // Should return None

        // DELETE non-existing key
        assert!(!store.delete("b".to_string()).await.unwrap()); // Should return false
    }
}
