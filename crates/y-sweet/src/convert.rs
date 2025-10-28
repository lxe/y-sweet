use anyhow::Result;
use std::sync::Arc;
use y_sweet_core::{doc_connection::DOC_NAME, store::{Store, memory::MemoryStore}, sync_kv::SyncKv};
use yrs_kvstore::DocOps;
use yrs::{ Doc, Transact, StateVector, ReadTxn};

/// Convert a Yjs document (encoded as a v1 update) to a .ysweet store.
pub async fn convert(store: Box<dyn Store>, doc_as_update: &[u8], doc_id: &str) -> Result<()> {
    let store = Some(Arc::new(store));

    let sync_kv = SyncKv::new(store, doc_id, || ()).await?;

    let sync_kv = Arc::new(sync_kv);

    sync_kv
        .push_update(DOC_NAME, doc_as_update)
        .map_err(|_| anyhow::anyhow!("Failed to push update"))?;

    sync_kv
        .flush_doc_with(DOC_NAME, yrs::Options::default())
        .map_err(|err| anyhow::anyhow!("Failed to flush doc {:?}", err))?;

    sync_kv
        .persist()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to persist: {:?}", e))?;

    Ok(())
}

pub async fn convert_snapshot_to_update(snapshot: &[u8], doc_id: &str) -> Result<Vec<u8>> {
    let store = MemoryStore::default();
    store.set(&format!("{}/data.ysweet", doc_id), snapshot.to_vec()).await?;
    let sync_kv = SyncKv::new(Some(Arc::new(Box::new(store.clone()))), doc_id, || ())
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create SyncKv: {:?}", e))?;

    let sync_kv = Arc::new(sync_kv);
    let doc = Doc::new();
    let mut txn = doc.transact_mut();
    sync_kv
        .load_doc(DOC_NAME, &mut txn)
        .map_err(|_| anyhow::anyhow!("Failed to load doc"))?;
    Ok(txn.encode_state_as_update_v1(&StateVector::default()))
}