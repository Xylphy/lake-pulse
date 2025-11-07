use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Iceberg table specific metrics extracted from table metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IcebergMetrics {
    /// Current snapshot ID
    pub current_snapshot_id: Option<i64>,

    /// Table format version
    pub format_version: i32,

    /// Table UUID
    pub table_uuid: String,

    /// Table metadata
    pub metadata: TableMetadata,

    /// Table properties
    pub table_properties: HashMap<String, String>,

    /// Snapshot information
    pub snapshot_info: SnapshotMetrics,

    /// Schema information
    pub schema_info: SchemaMetrics,

    /// Partition spec information
    pub partition_spec: PartitionSpecMetrics,

    /// Sort order information
    pub sort_order: SortOrderMetrics,

    /// File statistics
    pub file_stats: FileStatistics,

    /// Manifest statistics
    pub manifest_stats: ManifestStatistics,
}

/// Iceberg table metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableMetadata {
    /// Table location (base path)
    pub location: String,

    /// Last updated timestamp (milliseconds since epoch)
    pub last_updated_ms: Option<i64>,

    /// Last column ID assigned
    pub last_column_id: i32,

    /// Current schema ID
    pub current_schema_id: i32,

    /// Number of schemas
    pub schema_count: usize,

    /// Default spec ID
    pub default_spec_id: i32,

    /// Number of partition specs
    pub partition_spec_count: usize,

    /// Default sort order ID
    pub default_sort_order_id: i32,

    /// Number of sort orders
    pub sort_order_count: usize,

    /// Last sequence number
    pub last_sequence_number: Option<i64>,
}

/// Snapshot-level metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotMetrics {
    /// Total number of snapshots
    pub total_snapshots: usize,

    /// Current snapshot ID
    pub current_snapshot_id: Option<i64>,

    /// Current snapshot timestamp (milliseconds since epoch)
    pub current_snapshot_timestamp_ms: Option<i64>,

    /// Parent snapshot ID (if available)
    pub parent_snapshot_id: Option<i64>,

    /// Snapshot operation (append, replace, overwrite, delete)
    pub operation: Option<String>,

    /// Snapshot summary statistics
    pub summary: HashMap<String, String>,

    /// Manifest list location
    pub manifest_list: Option<String>,

    /// Schema ID at snapshot time
    pub schema_id: Option<i32>,

    /// Sequence number
    pub sequence_number: Option<i64>,
}

/// Schema-level metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaMetrics {
    /// Current schema ID
    pub schema_id: i32,

    /// Number of fields in current schema
    pub field_count: usize,

    /// Schema as JSON string
    pub schema_string: String,

    /// List of field names
    pub field_names: Vec<String>,

    /// Number of nested fields (structs, lists, maps)
    pub nested_field_count: usize,

    /// Number of required fields
    pub required_field_count: usize,

    /// Number of optional fields
    pub optional_field_count: usize,
}

/// Partition spec metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionSpecMetrics {
    /// Partition spec ID
    pub spec_id: i32,

    /// Number of partition fields
    pub partition_field_count: usize,

    /// Partition field names
    pub partition_fields: Vec<String>,

    /// Partition transforms (identity, bucket, truncate, year, month, day, hour)
    pub partition_transforms: Vec<String>,

    /// Whether the table is partitioned
    pub is_partitioned: bool,

    /// Estimated number of unique partitions (if available)
    pub estimated_partition_count: Option<usize>,
}

/// Sort order metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SortOrderMetrics {
    /// Sort order ID
    pub order_id: i32,

    /// Number of sort fields
    pub sort_field_count: usize,

    /// Sort field names
    pub sort_fields: Vec<String>,

    /// Sort directions (asc, desc)
    pub sort_directions: Vec<String>,

    /// Null orders (nulls-first, nulls-last)
    pub null_orders: Vec<String>,

    /// Whether the table has a sort order defined
    pub is_sorted: bool,
}

/// File-level statistics from Iceberg table
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileStatistics {
    /// Total number of data files
    pub num_data_files: usize,

    /// Total size of data files in bytes
    pub total_data_size_bytes: u64,

    /// Average data file size in bytes
    pub avg_data_file_size_bytes: f64,

    /// Minimum data file size in bytes
    pub min_data_file_size_bytes: u64,

    /// Maximum data file size in bytes
    pub max_data_file_size_bytes: u64,

    /// Total number of records across all data files
    pub total_records: Option<i64>,

    /// Number of delete files (position deletes and equality deletes)
    pub num_delete_files: usize,

    /// Total size of delete files in bytes
    pub total_delete_size_bytes: u64,

    /// Number of position delete files
    pub num_position_delete_files: usize,

    /// Number of equality delete files
    pub num_equality_delete_files: usize,
}

/// Manifest file statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestStatistics {
    /// Total number of manifest files
    pub num_manifest_files: usize,

    /// Total size of manifest files in bytes
    pub total_manifest_size_bytes: u64,

    /// Average manifest file size in bytes
    pub avg_manifest_file_size_bytes: f64,

    /// Number of data manifests
    pub num_data_manifests: usize,

    /// Number of delete manifests
    pub num_delete_manifests: usize,

    /// Total number of manifest lists
    pub num_manifest_lists: usize,

    /// Total size of manifest lists in bytes
    pub total_manifest_list_size_bytes: u64,
}

impl IcebergMetrics {
    /// Create a new IcebergMetrics instance with default values
    pub fn new() -> Self {
        Self {
            current_snapshot_id: None,
            format_version: 1,
            table_uuid: String::new(),
            metadata: TableMetadata {
                location: String::new(),
                last_updated_ms: None,
                last_column_id: 0,
                current_schema_id: 0,
                schema_count: 0,
                default_spec_id: 0,
                partition_spec_count: 0,
                default_sort_order_id: 0,
                sort_order_count: 0,
                last_sequence_number: None,
            },
            table_properties: HashMap::new(),
            snapshot_info: SnapshotMetrics {
                total_snapshots: 0,
                current_snapshot_id: None,
                current_snapshot_timestamp_ms: None,
                parent_snapshot_id: None,
                operation: None,
                summary: HashMap::new(),
                manifest_list: None,
                schema_id: None,
                sequence_number: None,
            },
            schema_info: SchemaMetrics {
                schema_id: 0,
                field_count: 0,
                schema_string: String::new(),
                field_names: Vec::new(),
                nested_field_count: 0,
                required_field_count: 0,
                optional_field_count: 0,
            },
            partition_spec: PartitionSpecMetrics {
                spec_id: 0,
                partition_field_count: 0,
                partition_fields: Vec::new(),
                partition_transforms: Vec::new(),
                is_partitioned: false,
                estimated_partition_count: None,
            },
            sort_order: SortOrderMetrics {
                order_id: 0,
                sort_field_count: 0,
                sort_fields: Vec::new(),
                sort_directions: Vec::new(),
                null_orders: Vec::new(),
                is_sorted: false,
            },
            file_stats: FileStatistics {
                num_data_files: 0,
                total_data_size_bytes: 0,
                avg_data_file_size_bytes: 0.0,
                min_data_file_size_bytes: 0,
                max_data_file_size_bytes: 0,
                total_records: None,
                num_delete_files: 0,
                total_delete_size_bytes: 0,
                num_position_delete_files: 0,
                num_equality_delete_files: 0,
            },
            manifest_stats: ManifestStatistics {
                num_manifest_files: 0,
                total_manifest_size_bytes: 0,
                avg_manifest_file_size_bytes: 0.0,
                num_data_manifests: 0,
                num_delete_manifests: 0,
                num_manifest_lists: 0,
                total_manifest_list_size_bytes: 0,
            },
        }
    }
}

impl Default for IcebergMetrics {
    fn default() -> Self {
        Self::new()
    }
}
