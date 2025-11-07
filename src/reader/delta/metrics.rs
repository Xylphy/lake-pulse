use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Delta Lake specific metrics extracted from table metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeltaMetrics {
    /// Table version
    pub version: i64,

    /// Protocol version information
    pub protocol: ProtocolInfo,

    /// Table metadata
    pub metadata: TableMetadata,

    /// Table configuration/properties
    pub table_properties: HashMap<String, String>,

    /// File statistics
    pub file_stats: FileStatistics,

    /// Partition information
    pub partition_info: PartitionMetrics,
}

/// Delta Lake protocol information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProtocolInfo {
    /// Minimum reader version required to read the table
    pub min_reader_version: i32,

    /// Minimum writer version required to write to the table
    pub min_writer_version: i32,

    /// Reader features (for protocol version 3+)
    pub reader_features: Option<Vec<String>>,

    /// Writer features (for protocol version 7+)
    pub writer_features: Option<Vec<String>>,
}

/// Delta Lake table metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableMetadata {
    /// Table ID
    pub id: String,

    /// Table name (if available)
    pub name: Option<String>,

    /// Table description (if available)
    pub description: Option<String>,

    /// Schema as JSON string
    pub schema_string: String,

    /// Number of fields in the schema
    pub field_count: usize,

    /// Partition columns
    pub partition_columns: Vec<String>,

    /// Table creation time (milliseconds since epoch)
    pub created_time: Option<i64>,

    /// Table format provider
    pub format_provider: String,

    /// Table format options
    pub format_options: HashMap<String, String>,
}

/// File-level statistics from Delta table
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileStatistics {
    /// Total number of active files
    pub num_files: usize,

    /// Total size of active files in bytes
    pub total_size_bytes: u64,

    /// Average file size in bytes
    pub avg_file_size_bytes: f64,

    /// Minimum file size in bytes
    pub min_file_size_bytes: u64,

    /// Maximum file size in bytes
    pub max_file_size_bytes: u64,

    /// Number of files with deletion vectors
    pub files_with_deletion_vectors: usize,
}

/// Partition-level metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionMetrics {
    /// Number of partition columns
    pub num_partition_columns: usize,

    /// Partition column names
    pub partition_columns: Vec<String>,

    /// Estimated number of unique partitions (if available)
    pub estimated_partition_count: Option<usize>,
}

impl DeltaMetrics {
    /// Create a new DeltaMetrics instance with default values
    pub fn new() -> Self {
        Self {
            version: 0,
            protocol: ProtocolInfo {
                min_reader_version: 0,
                min_writer_version: 0,
                reader_features: None,
                writer_features: None,
            },
            metadata: TableMetadata {
                id: String::new(),
                name: None,
                description: None,
                schema_string: String::new(),
                field_count: 0,
                partition_columns: Vec::new(),
                created_time: None,
                format_provider: String::new(),
                format_options: HashMap::new(),
            },
            table_properties: HashMap::new(),
            file_stats: FileStatistics {
                num_files: 0,
                total_size_bytes: 0,
                avg_file_size_bytes: 0.0,
                min_file_size_bytes: 0,
                max_file_size_bytes: 0,
                files_with_deletion_vectors: 0,
            },
            partition_info: PartitionMetrics {
                num_partition_columns: 0,
                partition_columns: Vec::new(),
                estimated_partition_count: None,
            },
        }
    }
}

impl Default for DeltaMetrics {
    fn default() -> Self {
        Self::new()
    }
}
