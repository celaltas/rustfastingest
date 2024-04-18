use super::model::{NodeModel, RelationModel};
use crate::config::config::DatabaseConfig;
use eyre::{eyre, Result};
use scylla::{frame::Compression, prepared_statement::PreparedStatement, Session, SessionBuilder};
use std::{collections::HashMap, fs, path::Path, sync::Arc, time::Duration};
use tokio::sync::Mutex;
use tracing::{error, info};
use uuid::Uuid;

const INSERT_NODE_QUERY: &str = "INSERT INTO graph.nodes (id, direction, relation, relates_to, name, ingestion_id, url, item_type, tags) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
const GET_NODE_BY_ID_QUERY: &str = "SELECT id, name, item_type, url, ingestion_id FROM graph.nodes WHERE id = ? AND direction = '' AND relation = ''";
const GET_NODE_BY_ID_WITH_TAGS_QUERY: &str = "SELECT id, direction, relation, relates_to, name, ingestion_id, url, item_type, tags FROM graph.nodes WHERE id = ? AND direction = '' AND relation = ''";
const GET_NODE_BY_ID_WITH_RELATIONS_QUERY: &str = "SELECT id, direction, relation, relates_to, name, ingestion_id, url, item_type, tags FROM graph.nodes WHERE id = ?";
const GET_NODE_BY_ID_AND_DIRECTION_QUERY: &str = "SELECT id, direction, relation, relates_to, name, item_type FROM graph.nodes WHERE id = ? AND direction IN ('', ?)";
const GET_NODE_BY_ID_DIRECTION_AND_RELATION_QUERY: &str = "SELECT id, direction, relation, relates_to, name, item_type FROM graph.nodes WHERE id = ? AND direction IN ('', ?) AND relation IN ('', ?)";

pub struct ScyllaService {
    pub client: Session,
    pub prepared_statements: HashMap<String, PreparedStatement>,
    pub concurrency_limit: usize,
}

impl ScyllaService {
    pub async fn new_session(db_url: String) -> Result<Session> {
        let session: Session = SessionBuilder::new()
            .known_node(db_url)
            .compression(Some(Compression::Lz4))
            .build()
            .await?;
        Ok(session)
    }
    pub async fn init(config: &DatabaseConfig) -> Result<ScyllaService> {
        let session = ScyllaService::new_session(config.connection_url.clone()).await?;
        info!("Scylla service: Connection established.");
        ScyllaService::load_schema(&session, config.schema_file.clone()).await?;
        info!("Scylla service: Loading initial schema successfully.");
        let prepared_statements = ScyllaService::create_prepared_statements(&session).await?;
        info!("Scylla service: All prepared statements are created.");
        Ok(ScyllaService {
            client: session,
            prepared_statements,
            concurrency_limit: config.concurrency_limit,
        })
    }

    async fn read_queries_from_schema<P>(schema_path: P) -> Result<Vec<String>>
    where
        P: AsRef<Path>,
    {
        println!("Reading schema file: {}", schema_path.as_ref().display());
        let schema = fs::read_to_string(schema_path)
            .map_err(|err| eyre!("Failed to read schema file: {}", err))?;
        let queries: Vec<String> = schema
            .split(';')
            .map(|query| format!("{};", query.trim()))
            .filter(|query| query.len() > 1)
            .collect();
        Ok(queries)
    }

    async fn run_queries(session: &Session, queries: Vec<String>) -> Result<()> {
        for query in queries {
            session.query(query, &[]).await?;
        }
        Ok(())
    }

    async fn load_schema<P: AsRef<Path>>(session: &Session, schema_file: P) -> Result<()> {
        let queries = Self::read_queries_from_schema(schema_file).await?;
        Self::run_queries(session, queries).await?;
        let _res = session
            .await_timed_schema_agreement(Duration::from_secs(10))
            .await?;

        Ok(())
    }

    async fn create_prepared_statements(
        session: &Session,
    ) -> Result<HashMap<String, PreparedStatement>, eyre::Error> {
        let mut prepared_statements = HashMap::new();

        let insert_node_prepared_statement = session.prepare(INSERT_NODE_QUERY).await?;
        prepared_statements.insert(
            "INSERT_NODE_QUERY".to_owned().to_lowercase(),
            insert_node_prepared_statement,
        );

        let get_node_prepared_statement =
            session.prepare(GET_NODE_BY_ID_AND_DIRECTION_QUERY).await?;
        prepared_statements.insert(
            "GET_NODE_BY_ID_AND_DIRECTION_QUERY"
                .to_owned()
                .to_lowercase(),
            get_node_prepared_statement,
        );

        let get_node_direction_and_relation_prepared_statement = session
            .prepare(GET_NODE_BY_ID_DIRECTION_AND_RELATION_QUERY)
            .await?;
        prepared_statements.insert(
            "GET_NODE_BY_ID_DIRECTION_AND_RELATION_QUERY"
                .to_owned()
                .to_lowercase(),
            get_node_direction_and_relation_prepared_statement,
        );

        Ok(prepared_statements)
    }

    pub async fn insert_nodes(service: ScyllaService, nodes: Vec<NodeModel>) -> Result<()> {
        let service = Arc::new(Mutex::new(service));
        let mut handles = vec![];
        for node in nodes {
            let service = Arc::clone(&service);
            let handle = tokio::spawn(async move {
                let service = service.lock().await;
                service.insert_node(node).await
            });
            handles.push(handle);
        }

        for handle in handles {
            let res = handle.await?;
            match res {
                Ok(_) => info!("The record save successfully"),
                Err(_) => error!("Error occured when record saved"),
            }
        }

        Ok(())
    }
    pub async fn insert_node(&self, node: NodeModel) -> Result<()> {
        let ps = self
            .prepared_statements
            .get("insert_node_query")
            .ok_or_else(|| eyre!("insert node prepared statement not found"))?;
        let _res = self
            .client
            .execute(
                &ps,
                (
                    node.uuid,
                    node.direction,
                    node.relation,
                    node.relates_to,
                    node.name,
                    node.ingestion_id,
                    node.url,
                    node.node_type,
                    node.tags,
                ),
            )
            .await?;
        Ok(())
    }

    pub async fn get_node_by_id(&self, id: &str) -> Result<NodeModel> {
        let uuid = Uuid::parse_str(id)?;
        let res = self
            .client
            .query(GET_NODE_BY_ID_WITH_RELATIONS_QUERY, (uuid,))
            .await?;
        let node = res
            .single_row_typed::<NodeModel>()
            .map_err(|err| eyre!("Node not found: {}", err));
        node
    }

    pub async fn get_node_relations_traversal(
        &self,
        id: &str,
        direction: &str,
        relation_type: &Option<String>,
    ) -> Result<Vec<RelationModel>> {
        let mut rels = vec![];

        let result = match relation_type {
            Some(rel) => {
                let ps = self
                    .prepared_statements
                    .get(
                        "GET_NODE_BY_ID_DIRECTION_AND_RELATION_QUERY"
                            .to_lowercase()
                            .as_str(),
                    )
                    .ok_or_else(|| eyre!("insert node prepared statement not found"))?;
                self.client.execute(&ps, (id, direction, rel)).await?
            }
            None => {
                let ps = self
                    .prepared_statements
                    .get("GET_NODE_BY_ID_AND_DIRECTION_QUERY".to_lowercase().as_str())
                    .ok_or_else(|| eyre!("insert node prepared statement not found"))?;
                self.client.execute(&ps, (id, direction)).await?
            }
        };

        if let Some(rows) = result.rows {
            for r in rows {
                let node = r.into_typed::<RelationModel>()?;
                rels.push(node);
            }
        }

        Ok(rels)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn test_read_queries_from_schema() {
        let text = "
        CREATE KEYSPACE IF NOT EXISTS my_keyspace WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};
        USE my_keyspace;
        CREATE TABLE IF NOT EXISTS users (
            user_id UUID PRIMARY KEY,
            email TEXT,
            name TEXT,
            created_at TIMESTAMP
        );
        INSERT INTO users (user_id, email, name, created_at) VALUES (uuid(), 'john@example.com', 'John Doe', toTimestamp(now()));
        SELECT * FROM users;
        ";
        let mut sql_file = NamedTempFile::new().expect("failed create temp file");
        sql_file
            .write_all(text.as_bytes())
            .expect("failed write to file");

        let queries = ScyllaService::read_queries_from_schema(sql_file.path())
            .await
            .unwrap();

        assert_eq!(queries.len(), 5);
    }
}