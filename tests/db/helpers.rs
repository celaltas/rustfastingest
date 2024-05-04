use rand::Rng;
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
    }
}

pub fn create_sample_node() -> NodeModel {
    let uuid = Uuid::new_v4();
    let direction = Some("Outgoing".to_string());
    let relation = Some("parent".to_string());
    let relates_to = Some("123456".to_string());
    let name = "Example Node".to_string();
    let ingestion_id = "ABC123".to_string();
    let path = "https://example.com".to_string();
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
        path,
        node_type,
        tags,
    }
}

pub fn create_test_nodes(count: usize) -> Vec<NodeModel> {
    let mut nodes: Vec<NodeModel> = Vec::with_capacity(count);
    for i in 0..count {
        let uuid = Uuid::new_v4();
        let direction = if i == 0 {
            Some("Out".to_string())
        } else {
            Some("In".to_string())
        };
        let relation = if i == 0 {
            Some("Parent".to_string())
        } else {
            Some("Child".to_string())
        };
        let relates_to = if i == 0 {
            None
        } else {
            Some(nodes[i - 1].uuid.to_string())
        };
        let name = format!("Node {}", i);
        let ingestion_id = "XXXA-1".to_string();
        let path = if i == 0 {
            "/".to_string()
        } else {
            format!("{}/Node_{}", nodes[i - 1].path, i)
        };
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
            path,
            node_type,
            tags,
        };
        nodes.push(node);
    }
    nodes
}

pub fn get_random_node(nodes: &Vec<NodeModel>) -> Option<&NodeModel> {
    if nodes.is_empty() {
        return None;
    }
    let idx = rand::thread_rng().gen_range(0..nodes.len());
    nodes.get(idx)
}

pub async fn cleanup_database(session: &Session) -> eyre::Result<()> {
    session
        .query("DROP TABLE IF EXISTS graph.nodes;", &[])
        .await?;
    session.query("DROP KEYSPACE IF EXISTS graph;", &[]).await?;
    Ok(())
}
