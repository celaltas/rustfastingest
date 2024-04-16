use eyre::{eyre, Result};
use scylla::{
    frame::{response::result::Prepared, Compression},
    prepared_statement::PreparedStatement,
    Session, SessionBuilder,
};
use std::{collections::HashMap, fs, path::Path, sync::Arc, time::Duration};
use tracing::info;

const INSERT_NODE_QUERY: &str = "INSERT INTO graph.nodes (id, direction, relation, relates_to, name, ingestion_id, url, item_type, tags) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
const GET_NODE_BY_ID_QUERY: &str = "SELECT id, name, item_type, url, ingestion_id FROM graph.nodes WHERE id = ? AND direction = '' AND relation = ''";
const GET_NODE_BY_ID_WITH_TAGS_QUERY: &str = "SELECT id, direction, relation, relates_to, name, ingestion_id, url, item_type, tags FROM graph.nodes WHERE id = ? AND direction = '' AND relation = ''";
const GET_NODE_BY_ID_WITH_RELATIONS_QUERY: &str = "SELECT id, direction, relation, relates_to, name, ingestion_id, url, item_type, tags FROM graph.nodes WHERE id = ?";
const GET_NODE_BY_ID_AND_DIRECTION_QUERY: &str = "SELECT id, direction, relation, relates_to, name, item_type FROM graph.nodes WHERE id = ? AND direction IN ('', ?)";
const GET_NODE_BY_ID_DIRECTION_AND_RELATION_QUERY: &str = "SELECT id, direction, relation, relates_to, name, item_type FROM graph.nodes WHERE id = ? AND direction IN ('', ?) AND relation IN ('', ?)";

pub struct ScyllaService {
    client: Arc<Session>,
    prepared_statements: Arc<HashMap<String, PreparedStatement>>,
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
    pub async fn init<P: AsRef<Path>>(db_url: String, schema_file: P) -> Result<ScyllaService> {
        let session = ScyllaService::new_session(db_url).await?;
        info!("Scylla service: Connection established.");
        ScyllaService::load_schema(&session, schema_file).await?;
        info!("Scylla service: Loading initial schema successfully.");
        let prepared_statements = ScyllaService::create_prepared_statements(&session).await?;
        info!("Scylla service: All prepared statements are created.");
        Ok(ScyllaService {
            client: Arc::new(session),
            prepared_statements: Arc::new(prepared_statements),
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
            info!("Running Query {}", query);
            println!("Running Query {}", query);
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

        let get_node_prepared_statement = session.prepare(GET_NODE_BY_ID_QUERY).await?;
        prepared_statements.insert(
            "GET_NODE_BY_ID_QUERY".to_owned().to_lowercase(),
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn test_init() {
        let db_url = "127.0.0.1:9042".to_string();
        let mut schema_file = NamedTempFile::new().expect("failed create temp file");
        let cql = "
        CREATE KEYSPACE IF NOT EXISTS test_keyspace WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};
        USE test_keyspace;
        CREATE TABLE IF NOT EXISTS test_users (
            user_id UUID PRIMARY KEY,
            email TEXT,
            name TEXT,
            created_at TIMESTAMP
        );
        INSERT INTO test_users (user_id, email, name, created_at) VALUES (uuid(), 'john@example.com', 'John Doe', toTimestamp(now()));
        SELECT * FROM test_users LIMIT 1;
        ";
        schema_file
            .write_all(cql.as_bytes())
            .expect("failed write to file");
        let service = ScyllaService::init(db_url, schema_file.path())
            .await
            .expect("Initialization database failed:");
        let res = service
            .client
            .query("SELECT email FROM test_keyspace.test_users LIMIT 1", &[])
            .await
            .expect("Failed to query database");
        println!("{:#?}", res);
        if let Some(rows) = res.rows {
            for row in rows {
                let (mail,): (String,) = row
                    .into_typed::<(String,)>()
                    .expect("parsing error query result");
                assert_eq!(mail, "john@example.com".to_string());
            }
        }

        service
            .client
            .query("DROP TABLE IF EXISTS test_keyspace.test_users;", &[])
            .await
            .expect("Failed to drop table");
        service
            .client
            .query("DROP KEYSPACE IF EXISTS test_keyspace;", &[])
            .await
            .expect("Failed to drop keyspace");
    }

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

        println!("{:#?}", queries);
        assert_eq!(queries.len(), 5);
    }
}
