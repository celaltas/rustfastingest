use rustfastingest::{config::config::DatabaseConfig, db::model::NodeModel};
use scylla::Session;
use uuid::Uuid;

pub fn create_test_database_config() -> DatabaseConfig {
    let schema_file_path = std::env::current_dir().unwrap().join("schema/ddl.cql");
    let schema_file = schema_file_path.to_str().unwrap().to_string();

    DatabaseConfig {
        connection_url: "127.0.0.1:9042".to_string(),
        schema_file,
        concurrency_limit: 1,
        datacenter: "dt".to_string(),
        parallel_files: 2,
    }
}

pub fn create_sample_node() -> NodeModel {
    let uuid = Uuid::new_v4();
    let direction = Some("Outgoing".to_string());
    let relation = Some("parent".to_string());
    let relates_to = Some("123456".to_string());
    let name = "Example Node".to_string();
    let ingestion_id = "ABC123".to_string();
    let url = "https://example.com".to_string();
    let node_type = "Page".to_string();
    let tags = Some(vec![
        ("tag1".to_string(), "value1".to_string()),
        ("tag2".to_string(), "value2".to_string()),
    ]);

    NodeModel {
        uuid,
        direction,
        relation,
        relates_to,
        name,
        ingestion_id,
        url,
        node_type,
        tags,
    }
}

pub fn create_node_vector(count: usize) -> Vec<NodeModel> {
    let mut nodes = Vec::with_capacity(count);
    for i in 0..count {
        let uuid = Uuid::new_v4();
        let direction = if i % 2 == 0 {
            Some("Outgoing".to_string())
        } else {
            Some("Incoming".to_string())
        };
        let relation = Some(format!("relation_{}", i));
        let relates_to = Some(format!("relates_to_{}", i));
        let name = format!("Node {}", i);
        let ingestion_id = format!("ID {}", i);
        let url = format!("https://example.com/node/{}", i);
        let node_type = if i % 3 == 0 {
            "Type A".to_string()
        } else {
            "Type B".to_string()
        };
        let tags = Some(vec![
            ("tag1".to_string(), format!("value{}", i)),
            ("tag2".to_string(), format!("value{}", i * 2)),
        ]);
        let node = NodeModel {
            uuid,
            direction,
            relation,
            relates_to,
            name,
            ingestion_id,
            url,
            node_type,
            tags,
        };
        nodes.push(node);
    }
    nodes
}

pub async fn cleanup_database(session: &Session) -> eyre::Result<()> {
    session
        .query("DROP TABLE IF EXISTS graph.nodes;", &[])
        .await?;
    session.query("DROP KEYSPACE IF EXISTS graph;", &[]).await?;
    Ok(())
}
