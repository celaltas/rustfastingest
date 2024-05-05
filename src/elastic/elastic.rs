use crate::config::config::ElasticSearchConfig;
use elasticsearch::{
    http::transport::{SingleNodeConnectionPool, TransportBuilder},
    indices::{IndicesCreateParts, IndicesDeleteParts, IndicesExistsParts},
    BulkOperation, BulkParts, Elasticsearch,
};
use eyre::{eyre, Result};
use serde_json::json;
use serde_json::Value;
use std::sync::Arc;
use tracing::{error, info};

use super::model::IndexNode;

pub struct IndexConfig {
    name: String,
    mapping: serde_json::Value,
    overwrite: bool,
}

pub struct ElasticService {
    client: Arc<Elasticsearch>,
    num_shards: usize,
    refresh_interval: String,
    source_enabled: bool,
    batch_size: usize,
}

impl ElasticService {
    async fn new(url: String) -> Result<Self> {
        let url = url.parse()?;
        let connection_pool = SingleNodeConnectionPool::new(url);
        let builder = TransportBuilder::new(connection_pool);
        let transport = builder.build()?;
        let client = Elasticsearch::new(transport);
        Ok(Self {
            client: Arc::new(client),
            num_shards: 1,
            refresh_interval: "20s".to_string(),
            source_enabled: true,
            batch_size: 1000,
        })
    }

    async fn configure(&self, index_configs: Vec<IndexConfig>) -> Result<()> {
        for config in index_configs {
            self.create_index(&config.name, &config.mapping, config.overwrite)
                .await?;
        }
        Ok(())
    }

    pub async fn initialize(config: &ElasticSearchConfig) -> Result<Self> {
        let url = config.url.to_owned();
        let mut instance = Self::new(url).await?;
        let indexes = instance.get_indexes();
        info!("Initializing ElasticSearch instance");
        instance.configure(indexes).await?;
        instance.num_shards = config.num_shards;
        instance.refresh_interval = config.refresh_interval.to_owned();
        instance.source_enabled = config.source_enabled;
        instance.batch_size = config.batch_size;
        Ok(instance)
    }

    fn get_indexes(&self) -> Vec<IndexConfig> {
        let mut indexes = vec![];
        indexes.push(IndexConfig {
            name: "graph".to_string(),
            mapping: self.get_node_index_mapping(),
            overwrite: false,
        });
        indexes
    }

    async fn create_index(
        &self,
        index_name: &str,
        mapping: &serde_json::Value,
        overwrite: bool,
    ) -> Result<()> {
        info!("Creating index '{}'", index_name);
        let response = self
            .client
            .indices()
            .exists(IndicesExistsParts::Index(&[index_name]))
            .send()
            .await?;
        if response.status_code() == 404 {
            info!("Index '{}' does not exist, creating it", index_name);
            let res = self
                .client
                .indices()
                .create(IndicesCreateParts::Index(index_name))
                .body(mapping)
                .send()
                .await?;
        } else if overwrite {
            info!(
                "Index '{}' already exists, deleting and recreating it",
                index_name
            );
            self.client
                .indices()
                .delete(IndicesDeleteParts::Index(&[index_name]))
                .send()
                .await?;
            self.client
                .indices()
                .create(IndicesCreateParts::Index(index_name))
                .body(mapping)
                .send()
                .await?;
        } else {
            info!("Index '{}' already exists, skipping creation", index_name);
        }
        Ok(())
    }

    fn get_node_index_mapping(&self) -> serde_json::Value {
        let mapping = json!({
            "settings": {
                "index.number_of_shards": self.num_shards,
                "index.number_of_replicas": 0,
                "index.refresh_interval": self.refresh_interval
            },
            "mappings": {
                "_source": {
                    "enabled": self.source_enabled
                },
                "dynamic": "strict",
                "properties": {
                    "type": {
                        "analyzer": "english",
                        "type": "text"
                    },
                    "name": {
                        "analyzer": "english",
                        "type": "text",
                        "fields": {
                            "keyword": {
                                "type": "keyword"
                            }
                        }
                    },
                    "uuid": {
                        "type": "text",
                        "index": "false"
                    },
                    "tags": {
                        "type": "nested",
                        "properties": {
                            "type": {
                                "analyzer": "english",
                                "type": "text"
                            },
                            "value": {
                                "analyzer": "english",
                                "type": "text",
                                "fields": {
                                    "keyword": {
                                        "type": "keyword"
                                    }
                                }
                            }
                        }
                    }
                }
            }
        });
        mapping
    }

    pub async fn index_nodes(&self, nodes: Vec<IndexNode>, index_name: &str) -> Result<()> {
        let mut handles = vec![];
        for batch in nodes.chunks(self.batch_size) {
            let client = self.client.clone();
            let batch = batch.to_vec();
            let index_name = index_name.to_string();
            let handle = tokio::spawn(async move { index_batch(client, batch, index_name).await });
            handles.push(handle);
        }
        for handle in handles {
            let res = handle.await?;
            match res {
                Ok(_) => info!("Node indexed successfully"),
                Err(err) => error!("Error indexing node: {}", err),
            }
        }
        Ok(())
    }
}

async fn index_batch(
    client: Arc<Elasticsearch>,
    nodes: Vec<IndexNode>,
    index_name: String,
) -> Result<()> {
    info!("Indexing batch of {} nodes for {}", nodes.len(), index_name);
    let body: Vec<BulkOperation<IndexNode>> = nodes
        .into_iter()
        .map(|node| {
            let id = node.uuid.to_string();
            BulkOperation::index(node).id(id).into()
        })
        .collect();
    let response = client
        .bulk(BulkParts::Index("graph"))
        .body(body)
        .send()
        .await?;
    let json: Value = response.json().await?;
    if json["errors"].as_bool().unwrap_or(false) {
        return Err(eyre!("Error indexing nodes"));
    }
    Ok(())
}
