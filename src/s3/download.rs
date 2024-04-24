use crate::domain::data::GraphData;
use async_trait::async_trait;
use eyre::Result;
use s3::{creds::Credentials, Bucket, Region};
use std::time::{Duration, Instant};
use tracing::info;

#[async_trait]
pub trait BucketOps {
    async fn get_object(&self, path: &str) -> Result<GraphData>;
}

struct S3Bucket {
    bucket: Bucket,
}

impl S3Bucket {
    fn new(bucket: Bucket) -> Self {
        Self { bucket }
    }
}

#[async_trait]
impl BucketOps for S3Bucket {
    async fn get_object(&self, key: &str) -> Result<GraphData> {
        let response = self.bucket.get_object(key).await?;
        let graph_structure: GraphData = serde_json::from_slice(&response.bytes())?;
        Ok(graph_structure)
    }
}

pub fn create_bucket_ops(region: &str, bucket_name: &str) -> Result<Box<dyn BucketOps>> {
    let creds = Credentials::from_env()?;
    let region: Region = region.parse()?;
    let mut bucket = Bucket::new(bucket_name, region, creds)?;
    bucket.set_request_timeout(Some(Duration::new(290, 0)));
    Ok(Box::new(S3Bucket::new(bucket)))
}

pub async fn read_graph_from_s3(
    bucket_ops: Box<dyn BucketOps>,
    object_key: &str,
) -> Result<GraphData> {
    let now = Instant::now();
    let data = bucket_ops.get_object(object_key).await?;
    let elapsed = now.elapsed();
    info!("Read graph from S3. Took {:.2?}", elapsed);

    Ok(data)
}

#[cfg(test)]
mod tests {
    use std::{fs::File, io::Read};

    use super::*;

    #[tokio::test]
    async fn test_read_file_by_region() {
        struct MockBucketOps {}

        #[async_trait]
        impl BucketOps for MockBucketOps {
            async fn get_object(&self, _path: &str) -> Result<GraphData> {
                let data = GraphData {
                    nodes: vec![],
                    relations: vec![],
                };
                Ok(data)
            }
        }

        let mock_bucket_ops = MockBucketOps {};
        let object_key = "test.json";
        let result = read_graph_from_s3(Box::new(mock_bucket_ops), object_key).await;
        assert!(result.is_ok());
    }
    #[tokio::test]
    async fn test_read_graph_from_file() {
        struct MockBucketOps {}

        #[async_trait]
        impl BucketOps for MockBucketOps {
            async fn get_object(&self, path: &str) -> Result<GraphData> {
                let current_dir = std::env::current_dir()?;
                let example_file_path = current_dir.join(format!("tests/data/{}", path));
                println!("{:?}", example_file_path);
                let mut file = File::open(example_file_path)?;
                let mut buffer = Vec::new();
                file.read_to_end(&mut buffer)?;
                let data: GraphData = serde_json::from_slice(&buffer)?;
                Ok(data)
            }
        }

        let mock_bucket_ops = MockBucketOps {};
        let object_key = "example.json";
        let result = read_graph_from_s3(Box::new(mock_bucket_ops), object_key).await;
        assert!(result.is_ok());
        let data = result.unwrap();
        assert_eq!(data.nodes.len(), 2);
    }
}
