use crate::delta::metrics::DeltaMetrics;
use serde::{Deserialize, Serialize};
use serde_json::{Error as JsonError, Value, json};
use std::collections::{HashMap, LinkedList};
use std::error::Error;
use std::fmt::{Display, Formatter, Result as FmtResult};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileInfo {
    pub path: String,
    pub size_bytes: u64,
    pub last_modified: Option<String>,
    pub is_referenced: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionInfo {
    pub partition_values: HashMap<String, String>,
    pub file_count: usize,
    pub total_size_bytes: u64,
    pub avg_file_size_bytes: f64,
    pub files: Vec<FileInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusteringInfo {
    pub clustering_columns: Vec<String>,
    pub cluster_count: usize,
    pub avg_files_per_cluster: f64,
    pub avg_cluster_size_bytes: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeletionVectorMetrics {
    pub deletion_vector_count: usize,
    pub total_deletion_vector_size_bytes: u64,
    pub avg_deletion_vector_size_bytes: f64,
    pub deletion_vector_age_days: f64,
    pub deleted_rows_count: u64,
    pub deletion_vector_impact_score: f64, // 0.0 = no impact, 1.0 = high impact
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaEvolutionMetrics {
    pub total_schema_changes: usize,
    pub breaking_changes: usize,
    pub non_breaking_changes: usize,
    pub schema_stability_score: f64, // 0.0 = unstable, 1.0 = very stable
    pub days_since_last_change: f64,
    pub schema_change_frequency: f64, // changes per day
    pub current_schema_version: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeTravelMetrics {
    pub total_snapshots: usize,
    pub oldest_snapshot_age_days: f64,
    pub newest_snapshot_age_days: f64,
    pub total_historical_size_bytes: u64,
    pub avg_snapshot_size_bytes: f64,
    pub storage_cost_impact_score: f64, // 0.0 = low cost, 1.0 = high cost
    pub retention_efficiency_score: f64, // 0.0 = inefficient, 1.0 = very efficient
    pub recommended_retention_days: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableConstraintsMetrics {
    pub total_constraints: usize,
    pub check_constraints: usize,
    pub not_null_constraints: usize,
    pub unique_constraints: usize,
    pub foreign_key_constraints: usize,
    pub constraint_violation_risk: f64, // 0.0 = low risk, 1.0 = high risk
    pub data_quality_score: f64,        // 0.0 = poor quality, 1.0 = excellent quality
    pub constraint_coverage_score: f64, // 0.0 = no coverage, 1.0 = full coverage
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileCompactionMetrics {
    pub compaction_opportunity_score: f64, // 0.0 = no opportunity, 1.0 = high opportunity
    pub small_files_count: usize,
    pub small_files_size_bytes: u64,
    pub potential_compaction_files: usize,
    pub estimated_compaction_savings_bytes: u64,
    pub recommended_target_file_size_bytes: u64,
    pub compaction_priority: String, // "low", "medium", "high", "critical"
    pub z_order_opportunity: bool,
    pub z_order_columns: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthMetrics {
    pub total_files: usize,
    pub total_size_bytes: u64,
    pub unreferenced_files: Vec<FileInfo>,
    pub unreferenced_size_bytes: u64,
    pub partition_count: usize,
    pub partitions: Vec<PartitionInfo>,
    pub clustering: Option<ClusteringInfo>,
    pub avg_file_size_bytes: f64,
    pub file_size_distribution: FileSizeDistribution,
    pub recommendations: Vec<String>,
    pub health_score: f64,
    pub data_skew: DataSkewMetrics,
    pub metadata_health: MetadataHealth,
    pub snapshot_health: SnapshotHealth,
    pub deletion_vector_metrics: Option<DeletionVectorMetrics>,
    pub schema_evolution: Option<SchemaEvolutionMetrics>,
    pub time_travel_metrics: Option<TimeTravelMetrics>,
    pub table_constraints: Option<TableConstraintsMetrics>,
    pub file_compaction: Option<FileCompactionMetrics>,
    pub delta_table_specific_metrics: Option<DeltaMetrics>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileSizeDistribution {
    pub small_files: usize,      // < 16MB
    pub medium_files: usize,     // 16MB - 128MB
    pub large_files: usize,      // 128MB - 1GB
    pub very_large_files: usize, // > 1GB
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataSkewMetrics {
    pub partition_skew_score: f64, // 0.0 (perfect) to 1.0 (highly skewed)
    pub file_size_skew_score: f64, // 0.0 (perfect) to 1.0 (highly skewed)
    pub largest_partition_size: u64,
    pub smallest_partition_size: u64,
    pub avg_partition_size: u64,
    pub partition_size_std_dev: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetadataHealth {
    pub metadata_file_count: usize,
    pub metadata_total_size_bytes: u64,
    pub avg_metadata_file_size: f64,
    pub metadata_growth_rate: f64,  // bytes per day (estimated)
    pub manifest_file_count: usize, // For Iceberg
    pub first_file_name: Option<String>,
    pub last_file_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotHealth {
    pub snapshot_count: usize,
    pub oldest_snapshot_age_days: f64,
    pub newest_snapshot_age_days: f64,
    pub avg_snapshot_age_days: f64,
    pub snapshot_retention_risk: f64, // 0.0 (good) to 1.0 (high risk)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthReport {
    pub table_path: String,
    pub table_type: String, // "delta" or "iceberg"
    pub analysis_timestamp: String,
    pub metrics: HealthMetrics,
    pub health_score: f64, // 0.0 to 1.0
    pub timed_metrics: TimedLikeMetrics,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimedLikeMetrics {
    pub start_duration_collection: LinkedList<(String, u128, u128)>,
}

impl TimedLikeMetrics {
    pub fn to_chrome_tracing(&self) -> Result<Vec<Value>, Box<dyn Error + Send + Sync>> {
        let mut events = Vec::new();
        for (name, start, duration) in &self.start_duration_collection {
            events.push(json!({
                "name": name,
                "cat": "PERF",
                "pid": "1",
                "ph": "B",
                "ts": start * 1000,
            }));
            events.push(json!({
                "name": name,
                "cat": "PERF",
                "pid": "1",
                "ph": "E",
                "ts": start * 1000 + duration * 1000,
            }));
        }
        Ok(events)
    }
}

impl Display for HealthReport {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        let report = self;

        writeln!(f, "\n{}", "=".repeat(60))?;
        writeln!(f, "Table Health Report: {}", report.table_type)?;
        writeln!(f, "Type: {}", report.table_type)?;
        writeln!(f, "Analysis Time: {}", report.analysis_timestamp)?;
        writeln!(f, "{}\n", "=".repeat(60))?;

        // Overall health score
        let health_emoji = if report.health_score > 0.8 {
            "🟢"
        } else if report.health_score > 0.6 {
            "🟡"
        } else {
            "🔴"
        };
        writeln!(
            f,
            "{} Overall Health Score: {:.1}%",
            health_emoji,
            report.health_score * 100.0
        )?;

        // Key metrics
        writeln!(f, "\n📊 Key Metrics:")?;
        writeln!(f, "{}", "─".repeat(60))?;
        writeln!(f, "  Total Files:         {}", report.metrics.total_files)?;

        // Format size in GB or MB
        let size_gb = report.metrics.total_size_bytes as f64 / (1024.0 * 1024.0 * 1024.0);
        if size_gb >= 1.0 {
            writeln!(f, "  Total Size:          {:.2} GB", size_gb)?;
        } else {
            let size_mb = report.metrics.total_size_bytes as f64 / (1024.0 * 1024.0);
            writeln!(f, "  Total Size:          {:.2} MB", size_mb)?;
        }

        // Average file size
        let avg_mb = report.metrics.avg_file_size_bytes / (1024.0 * 1024.0);
        writeln!(f, "  Average File Size:   {:.2} MB", avg_mb)?;
        writeln!(
            f,
            "  Partition Count:     {}",
            report.metrics.partition_count
        )?;

        // File size distribution
        writeln!(f, "\n📦 File Size Distribution:")?;
        writeln!(f, "{}", "─".repeat(60))?;
        let dist = &report.metrics.file_size_distribution;
        let total_files =
            (dist.small_files + dist.medium_files + dist.large_files + dist.very_large_files)
                as f64;

        if total_files > 0.0 {
            writeln!(
                f,
                "  Small (<16MB):       {:>6} files ({:>5.1}%)",
                dist.small_files,
                dist.small_files as f64 / total_files * 100.0
            )?;
            writeln!(
                f,
                "  Medium (16-128MB):   {:>6} files ({:>5.1}%)",
                dist.medium_files,
                dist.medium_files as f64 / total_files * 100.0
            )?;
            writeln!(
                f,
                "  Large (128MB-1GB):   {:>6} files ({:>5.1}%)",
                dist.large_files,
                dist.large_files as f64 / total_files * 100.0
            )?;
            writeln!(
                f,
                "  Very Large (>1GB):   {:>6} files ({:>5.1}%)",
                dist.very_large_files,
                dist.very_large_files as f64 / total_files * 100.0
            )?;
        }

        // Clustering information (Iceberg only)
        if let Some(ref clustering) = report.metrics.clustering {
            writeln!(f, "\n🎯 Clustering Information:")?;
            writeln!(f, "{}", "─".repeat(60))?;
            writeln!(
                f,
                "  Clustering Columns:  {}",
                clustering.clustering_columns.join(", ")
            )?;
            writeln!(f, "  Cluster Count:       {}", clustering.cluster_count)?;
            writeln!(
                f,
                "  Avg Files/Cluster:   {:.2}",
                clustering.avg_files_per_cluster
            )?;
            let cluster_size_mb = clustering.avg_cluster_size_bytes / (1024.0 * 1024.0);
            writeln!(f, "  Avg Cluster Size:    {:.2} MB", cluster_size_mb)?;
        }

        // Data skew analysis
        writeln!(f, "\n📊 Data Skew Analysis:")?;
        writeln!(f, "{}", "─".repeat(60))?;
        let skew = &report.metrics.data_skew;
        writeln!(
            f,
            "  Partition Skew Score: {:.2} (0=perfect, 1=highly skewed)",
            skew.partition_skew_score
        )?;
        writeln!(
            f,
            "  File Size Skew:       {:.2} (0=perfect, 1=highly skewed)",
            skew.file_size_skew_score
        )?;
        if skew.avg_partition_size > 0 {
            let largest_mb = skew.largest_partition_size as f64 / (1024.0 * 1024.0);
            let smallest_mb = skew.smallest_partition_size as f64 / (1024.0 * 1024.0);
            let avg_mb = skew.avg_partition_size as f64 / (1024.0 * 1024.0);
            writeln!(f, "  Largest Partition:   {:.2} MB", largest_mb)?;
            writeln!(f, "  Smallest Partition:  {:.2} MB", smallest_mb)?;
            writeln!(f, "  Avg Partition Size:  {:.2} MB", avg_mb)?;
        }

        // Metadata health
        writeln!(f, "\n📋 Metadata Health:")?;
        writeln!(f, "{}", "─".repeat(60))?;
        let meta = &report.metrics.metadata_health;
        writeln!(f, "  Metadata Files:       {}", meta.metadata_file_count)?;
        let meta_size_mb = meta.metadata_total_size_bytes as f64 / (1024.0 * 1024.0);
        writeln!(f, "  Metadata Size:        {:.2} MB", meta_size_mb)?;
        if meta.metadata_file_count > 0 {
            writeln!(
                f,
                "  Avg Metadata File:    {:.2} MB",
                meta.avg_metadata_file_size / (1024.0 * 1024.0)
            )?;
        }
        if meta.manifest_file_count > 0 {
            writeln!(f, "  Manifest Files:       {}", meta.manifest_file_count)?;
        }
        if meta.first_file_name.is_some() {
            writeln!(
                f,
                "  First File:           {}",
                meta.first_file_name.as_ref().unwrap()
            )?;
        }
        if meta.last_file_name.is_some() {
            writeln!(
                f,
                "  Last File:            {}",
                meta.last_file_name.as_ref().unwrap()
            )?;
        }

        // Snapshot health
        writeln!(f, "\n📸 Snapshot Health:")?;
        writeln!(f, "{}", "─".repeat(60))?;
        let snap = &report.metrics.snapshot_health;
        writeln!(f, "  Snapshot Count:       {}", snap.snapshot_count)?;
        writeln!(
            f,
            "  Retention Risk:       {:.1}%",
            snap.snapshot_retention_risk * 100.0
        )?;
        if snap.oldest_snapshot_age_days > 0.0 {
            writeln!(
                f,
                "  Oldest Snapshot:      {:.1} days",
                snap.oldest_snapshot_age_days
            )?;
            writeln!(
                f,
                "  Newest Snapshot:      {:.1} days",
                snap.newest_snapshot_age_days
            )?;
            writeln!(
                f,
                "  Avg Snapshot Age:     {:.1} days",
                snap.avg_snapshot_age_days
            )?;
        }

        // Unreferenced files warning
        if !report.metrics.unreferenced_files.is_empty() {
            writeln!(f, "\n⚠️  Unreferenced Files:")?;
            writeln!(f, "{}", "─".repeat(60))?;
            writeln!(f, "  Count:  {}", report.metrics.unreferenced_files.len())?;
            let wasted_gb =
                report.metrics.unreferenced_size_bytes as f64 / (1024.0 * 1024.0 * 1024.0);
            if wasted_gb >= 1.0 {
                writeln!(f, "  Wasted: {:.2} GB", wasted_gb)?;
            } else {
                let wasted_mb = report.metrics.unreferenced_size_bytes as f64 / (1024.0 * 1024.0);
                writeln!(f, "  Wasted: {:.2} MB", wasted_mb)?;
            }

            let table_type_name = if report.table_type == "delta" {
                "Delta transaction log"
            } else {
                "Iceberg manifest files"
            };
            writeln!(
                f,
                "\n  These files exist in storage but are not referenced in the"
            )?;
            writeln!(f, "  {}. Consider cleaning them up.", table_type_name)?;
        }

        // Deletion vector metrics (Delta Lake only)
        if let Some(ref dv_metrics) = report.metrics.deletion_vector_metrics {
            writeln!(f, "\n🗑️  Deletion Vector Analysis:")?;
            writeln!(f, "{}", "─".repeat(60))?;
            writeln!(
                f,
                "  Deletion Vectors:      {}",
                dv_metrics.deletion_vector_count
            )?;
            let dv_size_mb = dv_metrics.total_deletion_vector_size_bytes as f64 / (1024.0 * 1024.0);
            if dv_size_mb >= 1.0 {
                writeln!(f, "  Total DV Size:         {:.2} MB", dv_size_mb)?;
            } else {
                let dv_size_kb = dv_metrics.total_deletion_vector_size_bytes as f64 / 1024.0;
                writeln!(f, "  Total DV Size:         {:.2} KB", dv_size_kb)?;
            }
            writeln!(
                f,
                "  Deleted Rows:          {}",
                dv_metrics.deleted_rows_count
            )?;
            writeln!(
                f,
                "  Oldest DV Age:         {:.1} days",
                dv_metrics.deletion_vector_age_days
            )?;
            writeln!(
                f,
                "  Impact Score:          {:.2} (0=no impact, 1=high impact)",
                dv_metrics.deletion_vector_impact_score
            )?;
        }

        // Schema evolution metrics
        if let Some(ref schema_metrics) = report.metrics.schema_evolution {
            writeln!(f, "\n📋 Schema Evolution Analysis:")?;
            writeln!(f, "{}", "─".repeat(60))?;
            writeln!(
                f,
                "  Total Changes:         {}",
                schema_metrics.total_schema_changes
            )?;
            writeln!(
                f,
                "  Breaking Changes:      {}",
                schema_metrics.breaking_changes
            )?;
            writeln!(
                f,
                "  Non-Breaking Changes:  {}",
                schema_metrics.non_breaking_changes
            )?;
            writeln!(
                f,
                "  Stability Score:       {:.2} (0=unstable, 1=very stable)",
                schema_metrics.schema_stability_score
            )?;
            writeln!(
                f,
                "  Days Since Last:       {:.1} days",
                schema_metrics.days_since_last_change
            )?;
            writeln!(
                f,
                "  Change Frequency:      {:.3} changes/day",
                schema_metrics.schema_change_frequency
            )?;
            writeln!(
                f,
                "  Current Version:       {}",
                schema_metrics.current_schema_version
            )?;
        }

        // Time travel analysis
        if let Some(ref tt_metrics) = report.metrics.time_travel_metrics {
            writeln!(f, "\n⏰ Time Travel Analysis:")?;
            writeln!(f, "{}", "─".repeat(60))?;
            writeln!(f, "  Total Snapshots:       {}", tt_metrics.total_snapshots)?;
            writeln!(
                f,
                "  Oldest Snapshot:       {:.1} days",
                tt_metrics.oldest_snapshot_age_days
            )?;
            writeln!(
                f,
                "  Newest Snapshot:       {:.1} days",
                tt_metrics.newest_snapshot_age_days
            )?;
            let historical_gb =
                tt_metrics.total_historical_size_bytes as f64 / (1024.0 * 1024.0 * 1024.0);
            if historical_gb >= 1.0 {
                writeln!(f, "  Historical Size:       {:.2} GB", historical_gb)?;
            } else {
                let historical_mb =
                    tt_metrics.total_historical_size_bytes as f64 / (1024.0 * 1024.0);
                writeln!(f, "  Historical Size:       {:.2} MB", historical_mb)?;
            }
            writeln!(
                f,
                "  Storage Cost Impact:   {:.2} (0=low cost, 1=high cost)",
                tt_metrics.storage_cost_impact_score
            )?;
            writeln!(
                f,
                "  Retention Efficiency:  {:.2} (0=inefficient, 1=very efficient)",
                tt_metrics.retention_efficiency_score
            )?;
            writeln!(
                f,
                "  Recommended Retention: {} days",
                tt_metrics.recommended_retention_days
            )?;
        }

        // Table constraints analysis
        if let Some(ref constraint_metrics) = report.metrics.table_constraints {
            writeln!(f, "\n🔒 Table Constraints Analysis:")?;
            writeln!(f, "{}", "─".repeat(60))?;
            writeln!(
                f,
                "  Total Constraints:     {}",
                constraint_metrics.total_constraints
            )?;
            writeln!(
                f,
                "  Check Constraints:     {}",
                constraint_metrics.check_constraints
            )?;
            writeln!(
                f,
                "  NOT NULL Constraints:  {}",
                constraint_metrics.not_null_constraints
            )?;
            writeln!(
                f,
                "  Unique Constraints:    {}",
                constraint_metrics.unique_constraints
            )?;
            writeln!(
                f,
                "  Foreign Key Constraints: {}",
                constraint_metrics.foreign_key_constraints
            )?;
            writeln!(
                f,
                "  Violation Risk:        {:.2} (0=low risk, 1=high risk)",
                constraint_metrics.constraint_violation_risk
            )?;
            writeln!(
                f,
                "  Data Quality Score:    {:.2} (0=poor quality, 1=excellent quality)",
                constraint_metrics.data_quality_score
            )?;
            writeln!(
                f,
                "  Constraint Coverage:   {:.2} (0=no coverage, 1=full coverage)",
                constraint_metrics.constraint_coverage_score
            )?;
        }

        // File compaction analysis
        if let Some(ref compaction_metrics) = report.metrics.file_compaction {
            writeln!(f, "\n📦 File Compaction Analysis:")?;
            writeln!(f, "{}", "─".repeat(60))?;
            writeln!(
                f,
                "  Compaction Opportunity: {:.2} (0=no opportunity, 1=high opportunity)",
                compaction_metrics.compaction_opportunity_score
            )?;
            writeln!(
                f,
                "  Small Files Count:     {}",
                compaction_metrics.small_files_count
            )?;
            let small_files_mb =
                compaction_metrics.small_files_size_bytes as f64 / (1024.0 * 1024.0);
            writeln!(f, "  Small Files Size:      {:.2} MB", small_files_mb)?;
            writeln!(
                f,
                "  Potential Compaction:  {} files",
                compaction_metrics.potential_compaction_files
            )?;
            let savings_mb =
                compaction_metrics.estimated_compaction_savings_bytes as f64 / (1024.0 * 1024.0);
            if savings_mb >= 1.0 {
                writeln!(f, "  Estimated Savings:     {:.2} MB", savings_mb)?;
            } else {
                let savings_kb =
                    compaction_metrics.estimated_compaction_savings_bytes as f64 / 1024.0;
                writeln!(f, "  Estimated Savings:     {:.2} KB", savings_kb)?;
            }
            let target_mb =
                compaction_metrics.recommended_target_file_size_bytes as f64 / (1024.0 * 1024.0);
            writeln!(f, "  Recommended Target:    {:.0} MB", target_mb)?;
            writeln!(
                f,
                "  Compaction Priority:   {}",
                compaction_metrics.compaction_priority.to_uppercase()
            )?;
            writeln!(
                f,
                "  Z-Order Opportunity:   {}",
                if compaction_metrics.z_order_opportunity {
                    "Yes"
                } else {
                    "No"
                }
            )?;
            if !compaction_metrics.z_order_columns.is_empty() {
                writeln!(
                    f,
                    "  Z-Order Columns:       {}",
                    compaction_metrics.z_order_columns.join(", ")
                )?;
            }
        }

        // Recommendations
        if !report.metrics.recommendations.is_empty() {
            writeln!(f, "\n💡 Recommendations:")?;
            writeln!(f, "{}", "─".repeat(60))?;
            for (i, rec) in report.metrics.recommendations.iter().enumerate() {
                writeln!(f, "  {}. {}", i + 1, rec)?;
            }
        } else {
            writeln!(f, "\n✅ No recommendations - table is in excellent health!")?;
        }

        if !report.metrics.delta_table_specific_metrics.is_none() {
            writeln!(f, "\nDelta Specific Metrics:")?;
            writeln!(f, "{}", "─".repeat(60))?;
            writeln!(
                f,
                "  Version:               {}",
                report
                    .metrics
                    .delta_table_specific_metrics
                    .as_ref()
                    .unwrap()
                    .version
            )?;
            writeln!(
                f,
                "  Min Reader Version:    {}",
                report
                    .metrics
                    .delta_table_specific_metrics
                    .as_ref()
                    .unwrap()
                    .protocol
                    .min_reader_version
            )?;
            writeln!(
                f,
                "  Min Writer Version:    {}",
                report
                    .metrics
                    .delta_table_specific_metrics
                    .as_ref()
                    .unwrap()
                    .protocol
                    .min_writer_version
            )?;
            writeln!(
                f,
                "  Reader Features:       {:?}",
                report
                    .metrics
                    .delta_table_specific_metrics
                    .as_ref()
                    .unwrap()
                    .protocol
                    .reader_features
            )?;
            writeln!(
                f,
                "  Writer Features:       {:?}",
                report
                    .metrics
                    .delta_table_specific_metrics
                    .as_ref()
                    .unwrap()
                    .protocol
                    .writer_features
            )?;
            writeln!(
                f,
                "  Table ID:              {}",
                report
                    .metrics
                    .delta_table_specific_metrics
                    .as_ref()
                    .unwrap()
                    .metadata
                    .id
            )?;
            writeln!(
                f,
                "  Table Name:            {:?}",
                report
                    .metrics
                    .delta_table_specific_metrics
                    .as_ref()
                    .unwrap()
                    .metadata
                    .name
            )?;
            writeln!(
                f,
                "  Table Description:     {:?}",
                report
                    .metrics
                    .delta_table_specific_metrics
                    .as_ref()
                    .unwrap()
                    .metadata
                    .description
            )?;
            writeln!(
                f,
                "  Field Count:           {}",
                report
                    .metrics
                    .delta_table_specific_metrics
                    .as_ref()
                    .unwrap()
                    .metadata
                    .field_count
            )?;
            writeln!(
                f,
                "  Partition Columns:     {:?}",
                report
                    .metrics
                    .delta_table_specific_metrics
                    .as_ref()
                    .unwrap()
                    .metadata
                    .partition_columns
            )?;
            writeln!(
                f,
                "  Created Time:          {:?}",
                report
                    .metrics
                    .delta_table_specific_metrics
                    .as_ref()
                    .unwrap()
                    .metadata
                    .created_time
            )?;
            writeln!(
                f,
                "  Table Properties:      {:?}",
                report
                    .metrics
                    .delta_table_specific_metrics
                    .as_ref()
                    .unwrap()
                    .table_properties
            )?;
            writeln!(
                f,
                "  File Statistics:       {:?}",
                report
                    .metrics
                    .delta_table_specific_metrics
                    .as_ref()
                    .unwrap()
                    .file_stats
            )?;
            writeln!(
                f,
                "  Partition Info:        {:?}",
                report
                    .metrics
                    .delta_table_specific_metrics
                    .as_ref()
                    .unwrap()
                    .partition_info
            )?;
        }

        if !report.timed_metrics.start_duration_collection.is_empty() {
            writeln!(f, "\n⏱️ Timed Metrics:")?;
            writeln!(f, "{}", "─".repeat(60))?;
            for (name, _, dur) in report.timed_metrics.start_duration_collection.iter() {
                writeln!(f, "  {}: {}ms", name, dur)?;
            }
        }

        writeln!(f, "\n{}\n", "=".repeat(60))
    }
}

impl HealthReport {
    pub fn to_json(&self, exclude_files: bool) -> Result<String, JsonError> {
        if exclude_files {
            let mut report = self.clone();
            report.metrics.unreferenced_files = Vec::new();
            report
                .metrics
                .partitions
                .iter_mut()
                .for_each(|p| p.files = Vec::new());
            serde_json::to_string_pretty(&report)
        } else {
            serde_json::to_string_pretty(self)
        }
    }
}

impl Default for HealthMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl HealthMetrics {
    pub fn new() -> Self {
        Self {
            total_files: 0,
            total_size_bytes: 0,
            unreferenced_files: Vec::new(),
            unreferenced_size_bytes: 0,
            partition_count: 0,
            partitions: Vec::new(),
            clustering: None,
            avg_file_size_bytes: 0.0,
            file_size_distribution: FileSizeDistribution {
                small_files: 0,
                medium_files: 0,
                large_files: 0,
                very_large_files: 0,
            },
            recommendations: Vec::new(),
            health_score: 0.0,
            data_skew: DataSkewMetrics {
                partition_skew_score: 0.0,
                file_size_skew_score: 0.0,
                largest_partition_size: 0,
                smallest_partition_size: 0,
                avg_partition_size: 0,
                partition_size_std_dev: 0.0,
            },
            metadata_health: MetadataHealth {
                metadata_file_count: 0,
                metadata_total_size_bytes: 0,
                avg_metadata_file_size: 0.0,
                metadata_growth_rate: 0.0,
                manifest_file_count: 0,
                first_file_name: None,
                last_file_name: None,
            },
            snapshot_health: SnapshotHealth {
                snapshot_count: 0,
                oldest_snapshot_age_days: 0.0,
                newest_snapshot_age_days: 0.0,
                avg_snapshot_age_days: 0.0,
                snapshot_retention_risk: 0.0,
            },
            deletion_vector_metrics: None,
            schema_evolution: None,
            time_travel_metrics: None,
            table_constraints: None,
            file_compaction: None,
            delta_table_specific_metrics: None,
        }
    }

    pub fn calculate_health_score(&self) -> f64 {
        let mut score = 1.0;

        // Penalize unreferenced files
        if self.total_files > 0 {
            let unreferenced_ratio = self.unreferenced_files.len() as f64 / self.total_files as f64;
            score -= unreferenced_ratio * 0.3;
        }

        // Penalize small files (inefficient)
        if self.total_files > 0 {
            let small_file_ratio =
                self.file_size_distribution.small_files as f64 / self.total_files as f64;
            score -= small_file_ratio * 0.2;
        }

        // Penalize very large files (potential performance issues)
        if self.total_files > 0 {
            let very_large_ratio =
                self.file_size_distribution.very_large_files as f64 / self.total_files as f64;
            score -= very_large_ratio * 0.1;
        }

        // Reward good partitioning
        if self.partition_count > 0 && self.total_files > 0 {
            let avg_files_per_partition = self.total_files as f64 / self.partition_count as f64;
            if avg_files_per_partition > 100.0 {
                score -= 0.1; // Too many files per partition
            } else if avg_files_per_partition < 5.0 {
                score -= 0.05; // Too few files per partition
            }
        }

        // Penalize data skew
        score -= self.data_skew.partition_skew_score * 0.15;
        score -= self.data_skew.file_size_skew_score * 0.1;

        // Penalize metadata bloat
        if self.metadata_health.metadata_total_size_bytes > 100 * 1024 * 1024 {
            // > 100MB
            score -= 0.05;
        }

        // Penalize snapshot retention issues
        score -= self.snapshot_health.snapshot_retention_risk * 0.1;

        // Penalize deletion vector impact
        if let Some(ref dv_metrics) = self.deletion_vector_metrics {
            score -= dv_metrics.deletion_vector_impact_score * 0.15;
        }

        // Factor in schema stability
        if let Some(ref schema_metrics) = self.schema_evolution {
            score -= (1.0 - schema_metrics.schema_stability_score) * 0.2;
        }

        // Factor in time travel storage costs
        if let Some(ref tt_metrics) = self.time_travel_metrics {
            score -= tt_metrics.storage_cost_impact_score * 0.1;
            score -= (1.0 - tt_metrics.retention_efficiency_score) * 0.05;
        }

        // Factor in data quality from constraints
        if let Some(ref constraint_metrics) = self.table_constraints {
            score -= (1.0 - constraint_metrics.data_quality_score) * 0.15;
            score -= constraint_metrics.constraint_violation_risk * 0.1;
        }

        // Factor in file compaction opportunities
        if let Some(ref compaction_metrics) = self.file_compaction {
            score -= (1.0 - compaction_metrics.compaction_opportunity_score) * 0.1;
        }

        score.clamp(0.0, 1.0)
    }

    pub fn calculate_data_skew(&mut self) {
        if self.partitions.is_empty() {
            return;
        }

        let partition_sizes: Vec<u64> =
            self.partitions.iter().map(|p| p.total_size_bytes).collect();
        let file_counts: Vec<usize> = self.partitions.iter().map(|p| p.file_count).collect();

        // Calculate partition size skew
        if !partition_sizes.is_empty() {
            let total_size: u64 = partition_sizes.iter().sum();
            let avg_size = total_size as f64 / partition_sizes.len() as f64;

            let variance = partition_sizes
                .iter()
                .map(|&size| (size as f64 - avg_size).powi(2))
                .sum::<f64>()
                / partition_sizes.len() as f64;

            let std_dev = variance.sqrt();
            let coefficient_of_variation = if avg_size > 0.0 {
                std_dev / avg_size
            } else {
                0.0
            };

            self.data_skew.partition_skew_score = coefficient_of_variation.min(1.0);
            self.data_skew.largest_partition_size = *partition_sizes.iter().max().unwrap_or(&0);
            self.data_skew.smallest_partition_size = *partition_sizes.iter().min().unwrap_or(&0);
            self.data_skew.avg_partition_size = avg_size as u64;
            self.data_skew.partition_size_std_dev = std_dev;
        }

        // Calculate file count skew
        if !file_counts.is_empty() {
            let total_files: usize = file_counts.iter().sum();
            let avg_files = total_files as f64 / file_counts.len() as f64;

            let variance = file_counts
                .iter()
                .map(|&count| (count as f64 - avg_files).powi(2))
                .sum::<f64>()
                / file_counts.len() as f64;

            let std_dev = variance.sqrt();
            let coefficient_of_variation = if avg_files > 0.0 {
                std_dev / avg_files
            } else {
                0.0
            };

            self.data_skew.file_size_skew_score = coefficient_of_variation.min(1.0);
        }
    }

    pub fn calculate_snapshot_health(&mut self, snapshot_count: usize) {
        self.snapshot_health.snapshot_count = snapshot_count;

        // Simplified snapshot age calculation (would need actual timestamps)
        self.snapshot_health.oldest_snapshot_age_days = 0.0;
        self.snapshot_health.newest_snapshot_age_days = 0.0;
        self.snapshot_health.avg_snapshot_age_days = 0.0;

        // Calculate retention risk based on snapshot count
        if snapshot_count > 100 {
            self.snapshot_health.snapshot_retention_risk = 0.8;
        } else if snapshot_count > 50 {
            self.snapshot_health.snapshot_retention_risk = 0.5;
        } else if snapshot_count > 20 {
            self.snapshot_health.snapshot_retention_risk = 0.2;
        } else {
            self.snapshot_health.snapshot_retention_risk = 0.0;
        }
    }

    pub fn generate_recommendations(&mut self) {
        // Check for unreferenced files
        if !self.unreferenced_files.is_empty() {
            self.recommendations.push(format!(
                "Found {} unreferenced files ({} bytes). Consider cleaning up orphaned data files.",
                self.unreferenced_files.len(),
                self.unreferenced_size_bytes
            ));
        }

        // Check file size distribution
        let total_files = self.total_files as f64;
        if total_files > 0.0 {
            let small_file_ratio = self.file_size_distribution.small_files as f64 / total_files;
            if small_file_ratio > 0.5 {
                self.recommendations.push(
                    "High percentage of small files detected. Consider compacting to improve query performance.".to_string()
                );
            }

            let very_large_ratio =
                self.file_size_distribution.very_large_files as f64 / total_files;
            if very_large_ratio > 0.1 {
                self.recommendations.push(
                    "Some very large files detected. Consider splitting large files for better parallelism.".to_string()
                );
            }
        }

        // Check partitioning
        if self.partition_count > 0 {
            let avg_files_per_partition = total_files / self.partition_count as f64;
            if avg_files_per_partition > 100.0 {
                self.recommendations.push(
                    "High number of files per partition. Consider repartitioning to reduce file count.".to_string()
                );
            } else if avg_files_per_partition < 5.0 {
                self.recommendations.push(
                    "Low number of files per partition. Consider consolidating partitions."
                        .to_string(),
                );
            }
        }

        // Check for empty partitions
        let empty_partitions = self.partitions.iter().filter(|p| p.file_count == 0).count();
        if empty_partitions > 0 {
            self.recommendations.push(format!(
                "Found {} empty partitions. Consider removing empty partition directories.",
                empty_partitions
            ));
        }

        // Check data skew
        if self.data_skew.partition_skew_score > 0.5 {
            self.recommendations.push(
                "High partition skew detected. Consider repartitioning to balance data distribution.".to_string()
            );
        }

        if self.data_skew.file_size_skew_score > 0.5 {
            self.recommendations.push(
                "High file size skew detected. Consider running OPTIMIZE to balance file sizes."
                    .to_string(),
            );
        }

        // Check metadata health
        if self.metadata_health.metadata_total_size_bytes > 50 * 1024 * 1024 {
            // > 50MB
            self.recommendations.push(
                "Large metadata size detected. Consider running VACUUM to clean up old transaction logs.".to_string()
            );
        }

        // Check snapshot health
        if self.snapshot_health.snapshot_retention_risk > 0.7 {
            self.recommendations.push(
                "High snapshot retention risk. Consider running VACUUM to remove old snapshots."
                    .to_string(),
            );
        }

        // Check clustering
        if let Some(ref clustering) = self.clustering {
            if clustering.avg_files_per_cluster > 50.0 {
                self.recommendations.push(
                    "High number of files per cluster. Consider optimizing clustering strategy."
                        .to_string(),
                );
            }

            if clustering.clustering_columns.len() > 4 {
                self.recommendations.push(
                    "Too many clustering columns detected. Consider reducing to 4 or fewer columns for optimal performance.".to_string()
                );
            }

            if clustering.clustering_columns.is_empty() {
                self.recommendations.push(
                    "No clustering detected. Consider enabling liquid clustering for better query performance.".to_string()
                );
            }
        }

        // Check deletion vectors
        if let Some(ref dv_metrics) = self.deletion_vector_metrics {
            if dv_metrics.deletion_vector_impact_score > 0.7 {
                self.recommendations.push(
                    "High deletion vector impact detected. Consider running VACUUM to clean up old deletion vectors.".to_string()
                );
            }

            if dv_metrics.deletion_vector_count > 50 {
                self.recommendations.push(
                    "Many deletion vectors detected. Consider optimizing delete operations to reduce fragmentation.".to_string()
                );
            }

            if dv_metrics.deletion_vector_age_days > 30.0 {
                self.recommendations.push(
                    "Old deletion vectors detected. Consider running VACUUM to clean up deletion vectors older than 30 days.".to_string()
                );
            }
        }

        // Check schema evolution
        if let Some(ref schema_metrics) = self.schema_evolution {
            if schema_metrics.schema_stability_score < 0.5 {
                self.recommendations.push(
                    "Unstable schema detected. Consider planning schema changes more carefully to improve performance.".to_string()
                );
            }

            if schema_metrics.breaking_changes > 5 {
                self.recommendations.push(
                    "Many breaking schema changes detected. Consider using schema evolution features to avoid breaking changes.".to_string()
                );
            }

            if schema_metrics.schema_change_frequency > 1.0 {
                self.recommendations.push(
                    "High schema change frequency detected. Consider batching schema changes to reduce performance impact.".to_string()
                );
            }

            if schema_metrics.days_since_last_change < 1.0 {
                self.recommendations.push(
                    "Recent schema changes detected. Monitor query performance for potential issues.".to_string()
                );
            }
        }

        // Check time travel storage costs
        if let Some(ref tt_metrics) = self.time_travel_metrics {
            if tt_metrics.storage_cost_impact_score > 0.7 {
                self.recommendations.push(
                    "High time travel storage costs detected. Consider running VACUUM to clean up old snapshots.".to_string()
                );
            }

            if tt_metrics.retention_efficiency_score < 0.5 {
                self.recommendations.push(
                    "Inefficient snapshot retention detected. Consider optimizing retention policy.".to_string()
                );
            }

            if tt_metrics.total_snapshots > 1000 {
                self.recommendations.push(
                    "High snapshot count detected. Consider reducing retention period to improve performance.".to_string()
                );
            }
        }

        // Check table constraints
        if let Some(ref constraint_metrics) = self.table_constraints {
            if constraint_metrics.data_quality_score < 0.5 {
                self.recommendations.push(
                    "Low data quality score detected. Consider adding more table constraints."
                        .to_string(),
                );
            }

            if constraint_metrics.constraint_violation_risk > 0.7 {
                self.recommendations.push(
                    "High constraint violation risk detected. Monitor data quality and consider data validation.".to_string()
                );
            }

            if constraint_metrics.constraint_coverage_score < 0.3 {
                self.recommendations.push(
                    "Low constraint coverage detected. Consider adding check constraints for better data quality.".to_string()
                );
            }
        }

        // Check file compaction opportunities
        if let Some(ref compaction_metrics) = self.file_compaction {
            if compaction_metrics.compaction_opportunity_score > 0.7 {
                self.recommendations.push(
                    "High file compaction opportunity detected. Consider running OPTIMIZE to improve performance.".to_string()
                );
            }

            if compaction_metrics.compaction_priority == "critical" {
                self.recommendations.push(
                    "Critical compaction priority detected. Run OPTIMIZE immediately to improve query performance.".to_string()
                );
            }

            if compaction_metrics.z_order_opportunity {
                self.recommendations.push(
                    format!("Z-ordering opportunity detected. Consider running OPTIMIZE ZORDER BY ({}) to improve query performance.",
                            compaction_metrics.z_order_columns.join(", ")).to_string()
                );
            }

            if compaction_metrics.estimated_compaction_savings_bytes > 100 * 1024 * 1024 {
                // > 100MB
                let savings_mb = compaction_metrics.estimated_compaction_savings_bytes as f64
                    / (1024.0 * 1024.0);
                self.recommendations.push(
                    format!("Significant compaction savings available: {:.1} MB. Consider running OPTIMIZE.", savings_mb).to_string()
                );
            }
        }
    }
}
