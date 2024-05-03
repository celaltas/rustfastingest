use super::model::{NodeModel, RelationModel};
use crate::config::config::DatabaseConfig;
use eyre::{eyre, Result};
use scylla::{
    frame::Compression, prepared_statement::PreparedStatement,
    transport::query_result::SingleRowTypedError, Session, SessionBuilder,
};
use std::{collections::HashMap, fs, path::Path, sync::Arc, time::Duration};
use tracing::{error, info};
use uuid::Uuid;

const INSERT_NODE_QUERY: &str = "INSERT INTO graph.nodes (id, direction, relation, relates_to, name, ingestion_id, url, item_type, tags) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
const GET_NODE_BY_ID: &str = "SELECT id, name, item_type, url, ingestion_id FROM graph.nodes WHERE id = ? AND direction = '' AND relation = ''";
const GET_NODE_BY_ID_WITH_TAGS: &str = "SELECT id, direction, relation, relates_to, name, ingestion_id, url, item_type, tags FROM graph.nodes WHERE id = ? AND direction = '' AND relation = ''";
const GET_NODE_BY_ID_WITH_ALL: &str = "SELECT id, direction, relation, relates_to, name, ingestion_id, url, item_type, tags FROM graph.nodes WHERE id = ?";
const GET_NODE_BY_ID_AND_DIRECTION: &str = "SELECT id, direction, relation, relates_to, name, item_type FROM graph.nodes WHERE id = ? AND direction IN ('', ?)";
const GET_NODE_BY_ID_DIRECTION_AND_RELATION: &str = "SELECT id, direction, relation, relates_to, name, item_type FROM graph.nodes WHERE id = ? AND direction IN ('', ?) AND relation IN ('', ?)";

#[derive(Debug)]
pub struct ScyllaService {
    pub client: Arc<Session>,
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
            client: Arc::new(session),
            prepared_statements,
            concurrency_limit: config.concurrency_limit,
        })
    }

    async fn read_queries_from_schema<P>(schema_path: P) -> Result<Vec<String>>
    where
        P: AsRef<Path>,
    {
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

        let get_node_prepared_statement = session.prepare(GET_NODE_BY_ID_AND_DIRECTION).await?;
        prepared_statements.insert(
            "GET_NODE_BY_ID_AND_DIRECTION".to_owned().to_lowercase(),
            get_node_prepared_statement,
        );

        let get_node_direction_and_relation_prepared_statement = session
            .prepare(GET_NODE_BY_ID_DIRECTION_AND_RELATION)
            .await?;
        prepared_statements.insert(
            "GET_NODE_BY_ID_DIRECTION_AND_RELATION"
                .to_owned()
                .to_lowercase(),
            get_node_direction_and_relation_prepared_statement,
        );

        Ok(prepared_statements)
    }

    pub async fn insert_nodes(&self, nodes: Vec<NodeModel>) -> Result<()> {
        let mut handles = vec![];
        let ps = self
            .prepared_statements
            .get("insert_node_query")
            .ok_or_else(|| eyre!("insert node prepared statement not found"))?;

        for node in nodes {
            let session = self.client.clone();
            let ps = ps.clone();

            let handle = tokio::spawn(async move {
                session
                    .execute(
                        &ps,
                        (
                            node.uuid,
                            node.direction,
                            node.direction.unwrap_or_default(),
                            node.relation.unwrap_or_default(),
                            node.relates_to.unwrap_or_default(),
                            node.name,
                            node.ingestion_id,
                            node.path,
                            node.node_type,
                            node.tags.unwrap_or_default(),
                        ),
                    )
                    .await
            });
            handles.push(handle);
        }

        for handle in handles {
            let res = handle.await?;
            match res {
                Ok(_) => info!("Node inserted successfully"),
                Err(err) => error!("Error inserting node: {}", err),
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
                    node.path,
                    node.node_type,
                    node.tags,
                ),
            )
            .await?;
        Ok(())
    }

    pub async fn get_node_by_id(&self, id: &str) -> Result<NodeModel> {
        let uuid = Uuid::parse_str(id)?;
        let res = self.client.query(GET_NODE_BY_ID_WITH_ALL, (uuid,)).await?;
        let node = res
            .single_row_typed::<NodeModel>()
            .map_err(|err| eyre!("Node not found: {}", err));
        node
    }

    pub async fn get_node(
        &self,
        uuid: Uuid,
        include_tags: bool,
        include_relations: bool,
    ) -> Result<Option<NodeModel>> {
        let query = if include_relations {
            GET_NODE_BY_ID_WITH_ALL
        } else if include_tags {
            GET_NODE_BY_ID_WITH_TAGS
        } else {
            GET_NODE_BY_ID
        };
        let res = self.client.query(query, (uuid,)).await?;
        let node = match res.single_row_typed::<NodeModel>() {
            Ok(node) => Some(node),
            Err(err) => match err {
                SingleRowTypedError::BadNumberOfRows(_) => None,
                _ => return Err(eyre!("ScyllaDB query failed: {}", err)),
            },
        };
        Ok(node)
    }

    pub async fn get_node_traversal(
        &self,
        uuid: Uuid,
        direction: String,
        relation_type: Option<String>,
    ) -> Result<Option<Vec<RelationModel>>> {
        let mut rels = vec![];

        let result = match relation_type {
            Some(rel) => {
                let ps = self
                    .prepared_statements
                    .get(
                        "GET_NODE_BY_ID_DIRECTION_AND_RELATION"
                            .to_lowercase()
                            .as_str(),
                    )
                    .ok_or_else(|| eyre!("insert node prepared statement not found"))?;
                self.client.execute(&ps, (uuid, direction, rel)).await?
            }
            None => {
                let ps = self
                    .prepared_statements
                    .get("GET_NODE_BY_ID_AND_DIRECTION".to_lowercase().as_str())
                    .ok_or_else(|| eyre!("insert node prepared statement not found"))?;
                self.client.execute(&ps, (uuid, direction)).await?
            }
        };

        if let Some(rows) = result.rows {
            for r in rows {
                let node = r.into_typed::<RelationModel>()?;
                rels.push(node);
            }
        }

        if rels.is_empty() {
            Ok(None)
        } else {
            Ok(Some(rels))
        }
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
