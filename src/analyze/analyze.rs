use crate::analyze::metrics::{
    ClusteringInfo, DeletionVectorMetrics, FileCompactionMetrics, FileInfo, HealthMetrics,
    HealthReport, PartitionInfo, SchemaEvolutionMetrics, TableConstraintsMetrics,
    TimeTravelMetrics, TimedLikeMetrics,
};
use crate::delta::reader::DeltaReader;
use crate::storage::{
    FileMetadata, StorageConfig, StorageError, StorageProvider, StorageProviderFactory,
};
use futures::stream::{self, StreamExt};
use serde_json::Value;
use std::collections::{HashMap, HashSet, LinkedList};
use std::error::Error;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::{info, warn};

#[derive(Debug, Clone)]
struct SchemaChange {
    #[allow(dead_code)]
    version: u64,
    timestamp: u64,
    schema: Value,
    is_breaking: bool,
}

// Intermediate structure to hold results from parallel metadata processing
#[derive(Debug, Default)]
struct MetadataProcessingResult {
    clustering_columns: Vec<String>,
    deletion_vector_count: u64,
    deletion_vector_total_size: u64,
    deleted_rows: u64,
    oldest_dv_age: f64,
    total_snapshots: usize,
    total_historical_size: u64,
    oldest_timestamp: u64,
    newest_timestamp: u64,
    total_constraints: usize,
    check_constraints: usize,
    not_null_constraints: usize,
    unique_constraints: usize,
    foreign_key_constraints: usize,
    z_order_columns: Vec<String>,
    z_order_opportunity: bool,
    schema_changes: Vec<SchemaChange>,
}

/// Builder for constructing an `Analyzer` instance.
///
/// This builder provides a fluent API for configuring and creating an `Analyzer`.
/// The builder pattern allows for easy extension with additional configuration options
/// in the future.
///
/// # Examples
///
/// ```no_run
/// use lake_health::analyze::Analyzer;
/// use lake_health::storage::StorageConfig;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
/// let storage_config = StorageConfig::local()
///     .with_option("path", "/path/to/data");
///
/// // Simple case with defaults
/// let analyzer = Analyzer::builder(storage_config.clone())
///     .build()
///     .await?;
///
/// // With custom parallelism
/// let analyzer = Analyzer::builder(storage_config)
///     .with_parallelism(10)
///     .build()
///     .await?;
/// # Ok(())
/// # }
/// ```
pub struct AnalyzerBuilder {
    config: StorageConfig,
    parallelism: Option<usize>,
}

impl AnalyzerBuilder {
    /// Creates a new `AnalyzerBuilder` with the given storage configuration.
    ///
    /// # Arguments
    ///
    /// * `config` - The storage configuration to use for the analyzer
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use lake_health::analyze::AnalyzerBuilder;
    /// use lake_health::storage::StorageConfig;
    ///
    /// let config = StorageConfig::local().with_option("path", "/data");
    /// let builder = AnalyzerBuilder::new(config);
    /// ```
    pub fn new(config: StorageConfig) -> Self {
        Self {
            config,
            parallelism: None,
        }
    }

    /// Sets the desired parallelism for metadata file processing.
    ///
    /// # Arguments
    ///
    /// * `parallelism` - The desired level of parallelism (number of concurrent tasks)
    ///
    /// ```no_run
    /// use lake_health::analyze::AnalyzerBuilder;
    /// use lake_health::storage::StorageConfig;
    ///
    /// let config = StorageConfig::local().with_option("path", "/data");
    /// let builder = AnalyzerBuilder::new(config).with_parallelism(10);
    /// ```

    pub fn with_parallelism(mut self, parallelism: usize) -> Self {
        self.parallelism = Some(parallelism);
        self
    }

    /// Builds the `Analyzer` instance.
    ///
    /// This method performs the async initialization of the storage provider
    /// and constructs the final `Analyzer` instance.
    ///
    /// # Errors
    ///
    /// Returns an error if the storage provider cannot be initialized.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use lake_health::analyze::Analyzer;
    /// use lake_health::storage::StorageConfig;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    /// let config = StorageConfig::local().with_option("path", "/data");
    /// let analyzer = Analyzer::builder(config)
    ///     .with_parallelism(5)
    ///     .build()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn build(self) -> Result<Analyzer, Box<dyn Error + Send + Sync>> {
        let storage_provider = StorageProviderFactory::from_config(self.config).await?;
        Ok(Analyzer {
            storage_provider,
            parallelism: self.parallelism.unwrap_or(1),
        })
    }
}

pub struct Analyzer {
    storage_provider: Arc<dyn StorageProvider>,
    parallelism: usize,
}

// TODO: Check what's specific to Delta Lake and move to delta module.
//       All metrics are based on table's features are available for both
//       Delta Lake and Iceberg. Deletion vectors has been added to Iceberg v3.
impl Analyzer {
    /// Creates a new `AnalyzerBuilder` for constructing an `Analyzer` instance.
    ///
    /// This is the preferred way to create an `Analyzer`. Use the builder pattern
    /// to configure the analyzer before calling `.build().await`.
    ///
    /// # Arguments
    ///
    /// * `config` - The storage configuration to use
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use lake_health::analyze::Analyzer;
    /// use lake_health::storage::StorageConfig;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    /// let config = StorageConfig::local().with_option("path", "/data");
    /// let analyzer = Analyzer::builder(config)
    ///     .with_parallelism(10)
    ///     .build()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn builder(config: StorageConfig) -> AnalyzerBuilder {
        AnalyzerBuilder::new(config)
    }

    pub async fn analyze(
        &self,
        location: &str,
    ) -> Result<HealthReport, Box<dyn Error + Send + Sync>> {
        let mut internal_metrics: LinkedList<(&str, SystemTime, Duration)> = LinkedList::new();
        let mut metrics = HealthMetrics::new();

        let validate_connection_start = SystemTime::now();
        self.storage_provider.validate_connection(location).await?;
        let validate_connection_dur = validate_connection_start.elapsed()?;
        internal_metrics.push_back((
            "validate_connection_dur",
            validate_connection_start,
            validate_connection_dur,
        ));
        info!(
            "Validated connection, took={}",
            validate_connection_dur.as_millis()
        );

        info!(
            "Analyzing, base_path={}/{}",
            self.storage_provider.base_path(),
            location
        );

        let discover_partitions_start = SystemTime::now();
        let partitions = self
            .storage_provider
            .discover_partitions(location, vec![])
            .await?;
        let discover_partitions_dur = discover_partitions_start.elapsed()?;
        internal_metrics.push_back((
            "discover_partitions_dur",
            discover_partitions_start,
            discover_partitions_dur,
        ));

        info!(
            "Found count={} first level partitions at location={}, partitions={:?}, took={}",
            partitions.len(),
            location,
            partitions.clone(),
            discover_partitions_dur.as_millis()
        );

        let list_files_start = SystemTime::now();
        let all_objects = self
            .storage_provider
            .list_files_parallel(location, partitions.clone(), self.parallelism)
            .await?;
        let list_files_dur = list_files_start.elapsed()?;
        internal_metrics.push_back(("list_files_dur", list_files_start, list_files_dur));

        info!(
            "Listed count={} partitions at location={}, found objects={}, took={}",
            partitions.len(),
            location,
            all_objects.len(),
            list_files_dur.as_millis()
        );

        let categorize_files_start = SystemTime::now();
        let (data_files, metadata_files) = self.categorize_files(all_objects.clone())?;
        let categorize_files_dur = categorize_files_start.elapsed()?;
        internal_metrics.push_back((
            "categorize_files_dur",
            categorize_files_start,
            categorize_files_dur,
        ));

        info!(
            "Categorized count={} objects at location={}, took={}",
            all_objects.len(),
            location,
            categorize_files_dur.as_millis()
        );

        let find_referenced_files_start = SystemTime::now();
        let referenced_files = self.find_referenced_files(&metadata_files).await?;
        let find_referenced_files_dur = find_referenced_files_start.elapsed()?;
        internal_metrics.push_back((
            "find_referenced_files_dur",
            find_referenced_files_start,
            find_referenced_files_dur,
        ));

        info!(
            "Found count={} referenced files at location={}, took={}",
            referenced_files.clone().len(),
            location,
            find_referenced_files_dur.as_millis()
        );

        let file_metrics_bytesize_start = SystemTime::now();
        metrics.total_files = data_files.len();
        metrics.total_size_bytes = data_files.iter().map(|f| f.size as u64).sum();

        let referenced_set: HashSet<String> = referenced_files.into_iter().collect();
        for file in &data_files {
            let file_path = format!("{}/{}", self.storage_provider.base_path(), file.path);
            if !referenced_set.contains(&file_path) {
                metrics.unreferenced_files.push(FileInfo {
                    path: file_path,
                    size_bytes: file.size as u64,
                    last_modified: file.last_modified.clone().map(|m| m.to_rfc3339()),
                    is_referenced: false,
                });
            }
        }

        metrics.unreferenced_size_bytes = metrics
            .unreferenced_files
            .iter()
            .map(|f| f.size_bytes)
            .sum();
        let file_metrics_bytesize_dur = file_metrics_bytesize_start.elapsed()?;
        internal_metrics.push_back((
            "file_metrics_bytesize_dur",
            file_metrics_bytesize_start,
            file_metrics_bytesize_dur,
        ));

        info!(
            "Extracted metrics for referenced files count={}, unreferenced files count={}, referenced files, took={}",
            referenced_set.clone().len(),
            metrics.unreferenced_files.len(),
            file_metrics_bytesize_dur.as_millis()
        );

        let analyze_partitioning_start = SystemTime::now();
        metrics.partitions = self.analyze_partitioning(&data_files)?;
        let analyze_partitioning_dur = analyze_partitioning_start.elapsed()?;
        internal_metrics.push_back((
            "analyze_partitioning_dur",
            analyze_partitioning_start,
            analyze_partitioning_dur,
        ));

        info!(
            "Analyzed partitions count={}, took={}",
            metrics.partitions.len(),
            analyze_partitioning_dur.as_millis()
        );

        let data_files_total_size: u64 = data_files.iter().map(|f| f.size).sum();

        let update_metrics_from_metadata_start = SystemTime::now();
        self.update_metrics_from_metadata(
            &metadata_files,
            data_files_total_size,
            data_files.len(),
            &mut metrics,
        )
        .await?;
        let update_metrics_from_metadata_dur = analyze_partitioning_start.elapsed()?;
        internal_metrics.push_back((
            "update_metrics_from_metadata_dur",
            update_metrics_from_metadata_start,
            update_metrics_from_metadata_dur,
        ));

        info!(
            "Updated various metrics, took={}",
            update_metrics_from_metadata_dur.as_millis()
        );

        let calculate_file_size_distribution_start = SystemTime::now();
        let (small_files, medium_files, large_files, very_large_files) =
            self.calculate_file_size_distribution(&data_files);
        let calculate_file_size_distribution_dur =
            calculate_file_size_distribution_start.elapsed()?;
        internal_metrics.push_back((
            "calculate_file_size_distribution_dur",
            calculate_file_size_distribution_start,
            calculate_file_size_distribution_dur,
        ));

        info!(
            "Calculated file distribution, took={}",
            calculate_file_size_distribution_dur.as_millis()
        );

        metrics.file_size_distribution.small_files = small_files;
        metrics.file_size_distribution.medium_files = medium_files;
        metrics.file_size_distribution.large_files = large_files;
        metrics.file_size_distribution.very_large_files = very_large_files;

        if metrics.total_files > 0 {
            metrics.avg_file_size_bytes =
                metrics.total_size_bytes as f64 / metrics.total_files as f64;
        }

        let calculate_metadata_health_start = SystemTime::now();
        let (
            metadata_file_count,
            metadata_total_size_bytes,
            avg_metadata_file_size,
            metadata_growth_rate,
        ) = self.calculate_metadata_health(&metadata_files);
        let calculate_metadata_health_dur = calculate_file_size_distribution_start.elapsed()?;
        internal_metrics.push_back((
            "calculate_metadata_health_dur",
            calculate_metadata_health_start,
            calculate_metadata_health_dur,
        ));

        info!(
            "Calculated metadata health, file_count={}, bytesize={}, avg_bytesize={}, gowth_rate={}, took={}",
            metadata_file_count,
            metadata_total_size_bytes,
            avg_metadata_file_size,
            metadata_growth_rate,
            calculate_metadata_health_dur.as_millis()
        );

        metrics.metadata_health.metadata_file_count = metadata_file_count;
        metrics.metadata_health.metadata_total_size_bytes = metadata_total_size_bytes;
        metrics.metadata_health.avg_metadata_file_size = avg_metadata_file_size;
        metrics.metadata_health.metadata_growth_rate = metadata_growth_rate;

        let calculate_data_skew_start = SystemTime::now();
        metrics.calculate_data_skew();
        let calculate_data_skew_dur = calculate_data_skew_start.elapsed()?;
        internal_metrics.push_back((
            "calculate_data_skew_dur",
            calculate_data_skew_start,
            calculate_data_skew_dur,
        ));

        info!(
            "Calculated data skew, took={}",
            calculate_data_skew_dur.as_millis()
        );

        let calculate_snapshot_health_start = SystemTime::now();
        metrics.calculate_snapshot_health(metadata_files.len());
        let calculate_snapshot_health_dur = calculate_snapshot_health_start.elapsed()?;
        internal_metrics.push_back((
            "calculate_snapshot_health_dur",
            calculate_snapshot_health_start,
            calculate_snapshot_health_dur,
        ));

        info!(
            "Calculated snapshot health, took={}",
            calculate_snapshot_health_dur.as_millis()
        );

        let analyze_file_compaction_start = SystemTime::now();
        let file_compaction = metrics
            .file_compaction
            .ok_or_else(|| "File Compaction Metrics missing".to_string())?;
        metrics.file_compaction = self
            .analyze_file_compaction(
                &data_files,
                file_compaction.z_order_opportunity,
                file_compaction.z_order_columns,
            )
            .await?;
        let analyze_file_compaction_dur = analyze_file_compaction_start.elapsed()?;
        internal_metrics.push_back((
            "analyze_file_compaction_dur",
            analyze_file_compaction_start,
            analyze_file_compaction_dur,
        ));

        info!(
            "Calculated compaction metrics, took={}",
            analyze_file_compaction_dur.as_millis()
        );

        let generate_recommendations_start = SystemTime::now();
        metrics.generate_recommendations();
        let generate_recommendations_dur = generate_recommendations_start.elapsed()?;
        internal_metrics.push_back((
            "generate_recommendations_dur",
            generate_recommendations_start,
            generate_recommendations_dur,
        ));

        info!(
            "Calculated recommendations, took={}",
            generate_recommendations_dur.as_millis()
        );

        // Calculate health score
        let calculate_health_score_start = SystemTime::now();
        metrics.health_score = metrics.calculate_health_score();
        let calculate_health_score_dur = calculate_health_score_start.elapsed()?;
        internal_metrics.push_back((
            "calculate_health_score_dur",
            calculate_health_score_start,
            calculate_health_score_dur,
        ));

        info!(
            "Calculated health score of={}, took={}",
            metrics.health_score,
            calculate_health_score_dur.as_millis()
        );

        let table_detect_start = SystemTime::now();
        let table_path = self.storage_provider.url_from_path(location);
        let table_type = self.detect_table_type(&metadata_files).to_string();
        let table_detect_dur = table_detect_start.elapsed()?;
        internal_metrics.push_back(("table_detect_dur", table_detect_start, table_detect_dur));
        info!(
            "Detected table type={}, took={}",
            table_type,
            table_detect_dur.as_millis()
        );

        let analyze_after_validation_dur = list_files_start.elapsed()?;
        internal_metrics.push_back((
            "analyze_after_validation_dur",
            list_files_start,
            analyze_after_validation_dur,
        ));

        info!("Analysis took={}", analyze_after_validation_dur.as_millis());

        if table_type == "delta" {
            // Best-effort Delta-specific metrics extraction
            // If this fails, we log a warning but don't fail the entire analysis
            let delta_reader_open_start = SystemTime::now();
            match DeltaReader::open(
                self.storage_provider.url_from_path(location).as_str(),
                &self.storage_provider.clean_options(),
            )
            .await
            {
                Ok(delta_reader) => {
                    let delta_reader_open_dur = delta_reader_open_start.elapsed()?;
                    internal_metrics.push_back((
                        "delta_reader_open_dur",
                        delta_reader_open_start,
                        delta_reader_open_dur,
                    ));
                    info!(
                        "Opened Delta reader, location={}, took={}",
                        location,
                        delta_reader_open_dur.as_millis()
                    );

                    let delta_reader_extract_metrics_start = SystemTime::now();
                    match delta_reader.extract_metrics().await {
                        Ok(delta_specific_metrics) => {
                            metrics.delta_table_specific_metrics = Some(delta_specific_metrics);

                            let delta_reader_extract_metrics_dur =
                                delta_reader_extract_metrics_start.elapsed()?;
                            internal_metrics.push_back((
                                "delta_reader_extract_metrics_dur",
                                delta_reader_extract_metrics_start,
                                delta_reader_extract_metrics_dur,
                            ));
                            info!(
                                "Extracted Delta specific metrics for Delta table location={}, took={}",
                                location,
                                delta_reader_extract_metrics_dur.as_millis()
                            );
                        }
                        Err(e) => {
                            warn!(
                                "Failed to extract Delta-specific metrics for location={}: {}. Continuing without Delta-specific metrics.",
                                location, e
                            );
                        }
                    }
                }
                Err(e) => {
                    warn!(
                        "Failed to open Delta reader for location={}: {}. Continuing without Delta-specific metrics.",
                        location, e
                    );
                }
            }
        }

        let timed_metrics: LinkedList<(String, u128, u128)> = internal_metrics
            .iter()
            .map(
                |(k, st, dur)| -> Result<(String, u128, u128), Box<dyn Error + Send + Sync>> {
                    Ok((
                        k.to_string(),
                        st.duration_since(UNIX_EPOCH)?.as_millis(),
                        dur.as_millis(),
                    ))
                },
            )
            .collect::<Result<LinkedList<(_, _, _)>, _>>()?;

        let report = HealthReport {
            table_path,
            table_type,
            analysis_timestamp: chrono::Utc::now().to_rfc3339(),
            metrics: metrics.clone(),
            health_score: metrics.health_score.clone(),
            timed_metrics: TimedLikeMetrics {
                start_duration_collection: timed_metrics,
            },
        };

        Ok(report)
    }

    fn detect_table_type(&self, objects: &Vec<FileMetadata>) -> &str {
        if objects
            .into_iter()
            .find(|f| f.path.contains("_delta_log"))
            .is_some()
        {
            "delta"
        } else if objects
            .into_iter()
            .find(|f| f.path.contains("metadata"))
            .is_some()
        {
            "iceberg"
        } else {
            "unknown"
        }
    }

    fn categorize_files(
        &self,
        objects: Vec<FileMetadata>,
    ) -> Result<(Vec<FileMetadata>, Vec<FileMetadata>), StorageError> {
        let mut data_files = Vec::new();
        let mut metadata_files = Vec::new();

        for obj in objects {
            if obj.path.ends_with(".parquet") {
                data_files.push(obj);
            } else if obj.path.contains("_delta_log/") && obj.path.ends_with(".json") {
                // TODO: Should it contain the checkpoint parquet files that resides in delta_log?
                // TODO: Should we consider `_change_data` files as metadata?
                // TODO: Should we add here the logic for Iceberg categorization?
                metadata_files.push(obj);
            }
        }

        Ok((data_files, metadata_files))
    }

    fn is_ndjson(content: &str) -> bool {
        let non_empty_lines: Vec<&str> = content
            .lines()
            .filter(|line| !line.trim().is_empty())
            .collect();

        // NDJSON typically has multiple lines starting with { or [
        non_empty_lines.len() > 1
            && non_empty_lines.iter().all(|line| {
                let trimmed = line.trim();
                trimmed.starts_with('{') || trimmed.starts_with('[')
            })
    }

    async fn find_referenced_files(
        &self,
        metadata_files: &Vec<FileMetadata>,
    ) -> Result<Vec<String>, Box<dyn Error + Send + Sync>> {
        let storage_provider = Arc::clone(&self.storage_provider);

        let results: Vec<Result<Vec<String>, Box<dyn Error + Send + Sync>>> =
            stream::iter(metadata_files)
                .map(|metadata_file| {
                    info!(
                        "Listing info from metadata file={}",
                        &metadata_file.path.clone()
                    );
                    let storage_provider = Arc::clone(&storage_provider);
                    let path = metadata_file.path.clone();

                    async move {
                        let read_file_start = SystemTime::now();
                        let content = storage_provider
                            .read_file(&path)
                            .await
                            .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)?;
                        info!(
                            "Read file={}, took={}",
                            &path,
                            read_file_start.elapsed()?.as_millis()
                        );

                        info!("Processing content for file={}", &path);
                        let process_content_start = SystemTime::now();
                        let content_str = String::from_utf8_lossy(&content);
                        let is_ndjson = Self::is_ndjson(&content_str);
                        let json: Vec<Value> = if is_ndjson {
                            content_str
                                .lines()
                                .filter_map(|line| serde_json::from_str(line).ok())
                                .collect()
                        } else {
                            vec![
                                serde_json::from_str(&content_str)
                                    .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)?,
                            ]
                        };
                        info!(
                            "Processed content for file={}, count={} entries, took={}",
                            &path,
                            json.len(),
                            process_content_start.elapsed()?.as_millis()
                        );

                        info!("Extracting file references from metadata file={}", &path);
                        let extract_refs_start = SystemTime::now();
                        let mut file_refs = Vec::new();
                        for entry in json {
                            if let Some(add_actions) = entry.get("add") {
                                if let Some(add_array) = add_actions.as_array() {
                                    for add_action in add_array {
                                        if let Some(path) = add_action.get("path") {
                                            if let Some(path_str) = path.as_str() {
                                                file_refs.push(path_str.to_string());
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        info!(
                            "Extracted file references from metadata file={}, count={}, took={}",
                            &path,
                            file_refs.len(),
                            extract_refs_start.elapsed()?.as_millis()
                        );

                        Ok(file_refs)
                    }
                })
                .buffer_unordered(self.parallelism)
                .collect()
                .await;

        // Flatten results and collect errors
        let mut referenced_files = Vec::new();
        for result in results {
            referenced_files.extend(result?);
        }

        Ok(referenced_files)
    }

    // Helper method to process a single metadata file
    async fn process_single_metadata_file(
        &self,
        file_path: &str,
        file_index: usize,
    ) -> Result<MetadataProcessingResult, Box<dyn Error + Send + Sync>> {
        let content = self.storage_provider.read_file(file_path).await?;
        let content_str = String::from_utf8_lossy(&content);

        let mut result = MetadataProcessingResult {
            oldest_timestamp: chrono::Utc::now().timestamp() as u64,
            ..Default::default()
        };

        let is_ndjson = Self::is_ndjson(&content_str);
        let json: Vec<Value> = if is_ndjson {
            content_str
                .lines()
                .filter_map(|line| serde_json::from_str(line).ok())
                .collect()
        } else {
            vec![serde_json::from_str(&content_str)?]
        };

        let mut current_version = file_index as u64;

        for entry in json {
            if let Some(cluster_by) = entry.get("clusterBy") {
                if let Some(cluster_array) = cluster_by.as_array() {
                    let clustering_columns: Vec<String> = cluster_array
                        .iter()
                        .filter_map(|v| v.as_str().map(|s| s.to_string()))
                        .collect();
                    if !clustering_columns.is_empty() {
                        result.z_order_opportunity = true;
                        result.z_order_columns = clustering_columns.clone();
                        if result.clustering_columns.is_empty() {
                            result.clustering_columns = clustering_columns;
                        }
                    }
                }
            }

            if let Some(metadata) = entry.get("metaData") {
                if let Some(cluster_by) = metadata.get("clusterBy") {
                    if let Some(cluster_array) = cluster_by.as_array() {
                        if result.clustering_columns.is_empty() {
                            result.clustering_columns = cluster_array
                                .iter()
                                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                                .collect();
                        }
                    }
                }
            }

            if let Some(configuration) = entry.get("configuration") {
                if let Some(cluster_by) = configuration.get("delta.clustering.columns") {
                    if let Some(cluster_str) = cluster_by.as_str() {
                        if result.clustering_columns.is_empty() {
                            result.clustering_columns = cluster_str
                                .split(',')
                                .map(|s| s.trim().to_string())
                                .filter(|s| !s.is_empty())
                                .collect();
                        }
                    }
                }
            }

            if let Some(remove_actions) = entry.get("remove") {
                if let Some(remove_array) = remove_actions.as_array() {
                    for remove_action in remove_array {
                        if let Some(deletion_vector) = remove_action.get("deletionVector") {
                            result.deletion_vector_count += 1;

                            if let Some(size) = deletion_vector.get("sizeInBytes") {
                                result.deletion_vector_total_size += size.as_u64().unwrap_or(0);
                            }

                            if let Some(rows) = deletion_vector.get("cardinality") {
                                result.deleted_rows += rows.as_u64().unwrap_or(0);
                            }

                            if let Some(timestamp) = remove_action.get("timestamp") {
                                let creation_time = timestamp.as_u64().unwrap_or(0) as i64;
                                let age_days =
                                    (chrono::Utc::now().timestamp() - creation_time / 1000) as f64
                                        / 86400.0;
                                result.oldest_dv_age = result.oldest_dv_age.max(age_days);
                            }
                        }
                    }
                }
            }

            if let Some(timestamp) = entry.get("timestamp") {
                let ts = timestamp.as_u64().unwrap_or(0);
                if ts > 0 {
                    result.total_snapshots += 1;
                    result.oldest_timestamp = result.oldest_timestamp.min(ts);
                    result.newest_timestamp = result.newest_timestamp.max(ts);

                    let snapshot_size = self.estimate_snapshot_size(&entry);
                    result.total_historical_size += snapshot_size;
                }
            }

            if let Some(metadata) = entry.get("metaData") {
                if let Some(schema_string) = metadata.get("schemaString") {
                    if let Ok(schema) =
                        serde_json::from_str::<Value>(schema_string.as_str().unwrap_or(""))
                    {
                        let constraints = self.extract_constraints_from_schema(&schema);
                        result.total_constraints += constraints.0;
                        result.check_constraints += constraints.1;
                        result.not_null_constraints += constraints.2;
                        result.unique_constraints += constraints.3;
                        result.foreign_key_constraints += constraints.4;

                        let is_breaking = self.is_breaking_change(&result.schema_changes, &schema);
                        result.schema_changes.push(SchemaChange {
                            version: current_version,
                            timestamp: entry.get("timestamp").and_then(|t| t.as_u64()).unwrap_or(0),
                            schema,
                            is_breaking,
                        });
                    }
                }
            }

            if let Some(protocol) = entry.get("protocol") {
                if let Some(reader_version) = protocol.get("minReaderVersion") {
                    let new_version = reader_version.as_u64().unwrap_or(0);
                    if new_version > current_version {
                        result.schema_changes.push(SchemaChange {
                            version: current_version,
                            timestamp: entry.get("timestamp").and_then(|t| t.as_u64()).unwrap_or(0),
                            schema: Value::Null,
                            is_breaking: true,
                        });
                        current_version = new_version;
                    }
                }
            }
        }

        Ok(result)
    }

    async fn update_metrics_from_metadata(
        &self,
        metadata_files: &Vec<FileMetadata>,
        data_files_total_size: u64,
        data_files_total_files: usize,
        metrics: &mut HealthMetrics,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let storage_provider = Arc::clone(&self.storage_provider);

        let parallelism = self.parallelism.max(1);

        let min_file_name = metadata_files
            .iter()
            .min_by_key(|f| f.path.clone())
            .map(|f| f.path.clone());
        let max_file_name = metadata_files
            .iter()
            .max_by_key(|f| f.path.clone())
            .map(|f| f.path.clone());
        metrics.metadata_health.first_file_name = min_file_name.clone();
        metrics.metadata_health.last_file_name = max_file_name.clone();
        info!(
            "Updating the metrics from metadata files, parallelism={}, metadata_files_count={}, \
            data_files_count={}, min_file={:?}, max_file={:?}",
            parallelism,
            metadata_files.len(),
            data_files_total_files,
            min_file_name,
            max_file_name
        );
        let update_metrics_start = SystemTime::now();
        let results: Vec<Result<MetadataProcessingResult, Box<dyn Error + Send + Sync>>> =
            stream::iter(metadata_files.iter().enumerate())
                .map(|(index, metadata_file)| {
                    info!(
                        "Starting processing metadata file={}, index={}",
                        &metadata_file.path.clone(),
                        index
                    );
                    let path = metadata_file.path.clone();
                    let analyzer = Analyzer {
                        storage_provider: Arc::clone(&storage_provider),
                        parallelism,
                    };

                    let result = async move {
                        let process_single_start = SystemTime::now();
                        let r = analyzer.process_single_metadata_file(&path, index).await;
                        info!(
                            "Processed metadata file={} in async, took={}",
                            &path,
                            process_single_start
                                .elapsed()
                                .unwrap_or_default()
                                .as_millis(),
                        );
                        r
                    };
                    result
                })
                .buffer_unordered(parallelism)
                .collect()
                .await;

        info!(
            "Finished the metrics from metadata files, parallelism={}, metadata_files_count={}, \
            data_files_count={}, min_file={:?}, max_file={:?}, took={}",
            parallelism,
            metadata_files.len(),
            data_files_total_files,
            min_file_name,
            max_file_name,
            update_metrics_start
                .elapsed()
                .unwrap_or_default()
                .as_millis(),
        );

        // Aggregate results from all metadata files
        let mut clustering_columns: Vec<String> = vec![];
        let mut deletion_vector_count = 0u64;
        let mut total_size = 0u64;
        let mut deleted_rows = 0u64;
        let mut oldest_dv_age: f64 = 0.0;
        let mut total_snapshots = 0;
        let mut total_historical_size = 0u64;
        let mut oldest_timestamp = chrono::Utc::now().timestamp() as u64;
        let mut newest_timestamp = 0u64;
        let mut total_constraints = 0;
        let mut check_constraints = 0;
        let mut not_null_constraints = 0;
        let mut unique_constraints = 0;
        let mut foreign_key_constraints = 0;
        let mut z_order_columns: Vec<String> = vec![];
        let mut z_order_opportunity = false;
        let mut schema_changes = Vec::new();
        let mut current_version = 0u64;

        for result in results {
            let r = result?;

            if clustering_columns.is_empty() && !r.clustering_columns.is_empty() {
                clustering_columns = r.clustering_columns;
            }

            deletion_vector_count += r.deletion_vector_count;
            total_size += r.deletion_vector_total_size;
            deleted_rows += r.deleted_rows;
            oldest_dv_age = oldest_dv_age.max(r.oldest_dv_age);

            total_snapshots += r.total_snapshots;
            total_historical_size += r.total_historical_size;
            oldest_timestamp = oldest_timestamp.min(r.oldest_timestamp);
            newest_timestamp = newest_timestamp.max(r.newest_timestamp);

            total_constraints += r.total_constraints;
            check_constraints += r.check_constraints;
            not_null_constraints += r.not_null_constraints;
            unique_constraints += r.unique_constraints;
            foreign_key_constraints += r.foreign_key_constraints;

            if r.z_order_opportunity {
                z_order_opportunity = true;
                if z_order_columns.is_empty() {
                    z_order_columns = r.z_order_columns;
                }
            }

            schema_changes.extend(r.schema_changes);
            current_version = current_version.max(schema_changes.len() as u64);
        }

        if deletion_vector_count != 0 {
            let avg_size = total_size as f64 / deletion_vector_count as f64;
            let impact_score = self.calculate_deletion_vector_impact(
                deletion_vector_count as usize,
                total_size,
                oldest_dv_age,
            );

            metrics.deletion_vector_metrics = Some(DeletionVectorMetrics {
                deletion_vector_count: deletion_vector_count as usize,
                total_deletion_vector_size_bytes: total_size,
                avg_deletion_vector_size_bytes: avg_size,
                deletion_vector_age_days: oldest_dv_age,
                deleted_rows_count: deleted_rows,
                deletion_vector_impact_score: impact_score,
            });
        }

        if !schema_changes.is_empty() {
            metrics.schema_evolution =
                self.calculate_schema_metrics(schema_changes, current_version)?;
        }

        if total_snapshots != 0 {
            let now = chrono::Utc::now().timestamp() as u64;
            let oldest_age_days = (now - oldest_timestamp / 1000) as f64 / 86400.0;
            let newest_age_days = (now - newest_timestamp / 1000) as f64 / 86400.0;
            let avg_snapshot_size = total_historical_size as f64 / total_snapshots as f64;

            let storage_cost_impact = self.calculate_storage_cost_impact(
                total_historical_size,
                total_snapshots,
                oldest_age_days,
            );
            let retention_efficiency = self.calculate_retention_efficiency(
                total_snapshots,
                oldest_age_days,
                newest_age_days,
            );
            let recommended_retention =
                self.calculate_recommended_retention(total_snapshots, oldest_age_days);

            metrics.time_travel_metrics = Some(TimeTravelMetrics {
                total_snapshots,
                oldest_snapshot_age_days: oldest_age_days,
                newest_snapshot_age_days: newest_age_days,
                total_historical_size_bytes: total_historical_size,
                avg_snapshot_size_bytes: avg_snapshot_size,
                storage_cost_impact_score: storage_cost_impact,
                retention_efficiency_score: retention_efficiency,
                recommended_retention_days: recommended_retention,
            });
        }

        if total_constraints != 0 {
            let constraint_violation_risk =
                self.calculate_constraint_violation_risk(total_constraints, check_constraints);
            let data_quality_score =
                self.calculate_data_quality_score(total_constraints, constraint_violation_risk);
            let constraint_coverage_score =
                self.calculate_constraint_coverage_score(total_constraints, check_constraints);

            metrics.table_constraints = Some(TableConstraintsMetrics {
                total_constraints,
                check_constraints,
                not_null_constraints,
                unique_constraints,
                foreign_key_constraints,
                constraint_violation_risk,
                data_quality_score,
                constraint_coverage_score,
            });
        }

        // For Delta Lake clustering, we analyze the distribution of files
        // Since clustering is more about data layout than explicit clusters,
        // we use partition-like analysis but call it clustering
        let partition_count = metrics.partitions.len();

        // Calculate clustering metrics
        let cluster_count = partition_count.max(1); // Use partition count as proxy for cluster count
        let avg_files_per_cluster = if cluster_count > 0 {
            data_files_total_files as f64 / cluster_count as f64
        } else {
            0.0
        };

        let avg_cluster_size_bytes = if cluster_count > 0 {
            data_files_total_size as f64 / cluster_count as f64
        } else {
            0.0
        };

        metrics.clustering = Some(ClusteringInfo {
            clustering_columns: clustering_columns.to_vec(),
            cluster_count,
            avg_files_per_cluster,
            avg_cluster_size_bytes,
        });

        metrics.file_compaction = Some(FileCompactionMetrics {
            compaction_opportunity_score: 0.0,
            small_files_count: 0,
            small_files_size_bytes: 0,
            potential_compaction_files: 0,
            estimated_compaction_savings_bytes: 0,
            recommended_target_file_size_bytes: 0,
            compaction_priority: "".to_string(),
            z_order_opportunity,
            z_order_columns,
        });

        Ok(())
    }

    fn analyze_partitioning(
        &self,
        data_files: &Vec<FileMetadata>,
    ) -> Result<Vec<PartitionInfo>, Box<dyn Error + Send + Sync>> {
        let mut partition_map: HashMap<String, PartitionInfo> = HashMap::new();

        for file in data_files {
            // Extract partition information from file path
            // Delta Lake typically uses partition columns in the path like: col1=value1/col2=value2/file.parquet
            let path_parts: Vec<&str> = file.path.split('/').collect();
            let mut partition_values = HashMap::new();
            let mut _file_name = "";

            for part in &path_parts {
                if part.contains('=') {
                    let kv: Vec<&str> = part.split('=').collect();
                    if kv.len() == 2 {
                        partition_values.insert(kv[0].to_string(), kv[1].to_string());
                    }
                } else if part.ends_with(".parquet") {
                    _file_name = part;
                }
            }

            let partition_key = serde_json::to_string(&partition_values).unwrap_or_default();

            let partition_info =
                partition_map
                    .entry(partition_key)
                    .or_insert_with(|| PartitionInfo {
                        partition_values: partition_values.clone(),
                        file_count: 0,
                        total_size_bytes: 0,
                        avg_file_size_bytes: 0.0,
                        files: Vec::new(),
                    });

            partition_info.file_count += 1;
            partition_info.total_size_bytes += file.size as u64;
            partition_info.files.push(FileInfo {
                path: format!("{}/{}", self.storage_provider.base_path(), file.path),
                size_bytes: file.size as u64,
                last_modified: file.last_modified.clone().map(|m| m.to_rfc3339()),
                is_referenced: true, // We'll update this later
            });
        }

        // Calculate averages for each partition
        for partition in partition_map.values_mut() {
            if partition.file_count > 0 {
                partition.avg_file_size_bytes =
                    partition.total_size_bytes as f64 / partition.file_count as f64;
            }
        }

        let partitions = partition_map.into_values().collect::<Vec<PartitionInfo>>();

        Ok(partitions)
    }

    fn calculate_file_size_distribution(
        &self,
        data_files: &Vec<FileMetadata>,
    ) -> (usize, usize, usize, usize) {
        let mut small_files = 0;
        let mut medium_files = 0;
        let mut large_files = 0;
        let mut very_large_files = 0;

        for file in data_files {
            let size_mb = file.size as f64 / (1024.0 * 1024.0);

            if size_mb < 16.0 {
                small_files += 1;
            } else if size_mb < 128.0 {
                medium_files += 1;
            } else if size_mb < 1024.0 {
                large_files += 1;
            } else {
                very_large_files += 1;
            }
        }
        (small_files, medium_files, large_files, very_large_files)
    }

    pub fn calculate_metadata_health(
        &self,
        metadata_files: &Vec<FileMetadata>,
    ) -> (usize, u64, f64, f64) {
        let mut metadata_health: f64 = 0.0;
        let metadata_file_count = metadata_files.len();
        let metadata_total_size_bytes: u64 = metadata_files.iter().map(|f| f.size as u64).sum();

        if !metadata_files.is_empty() {
            metadata_health = metadata_total_size_bytes as f64 / metadata_files.len() as f64;
        }

        // Estimate growth rate (simplified - would need historical data for accuracy)
        let metadata_growth_rate = 0.0; // Placeholder

        (
            metadata_file_count,
            metadata_total_size_bytes,
            metadata_health,
            metadata_growth_rate,
        )
    }

    fn calculate_deletion_vector_impact(&self, count: usize, size: u64, age: f64) -> f64 {
        let mut impact: f64 = 0.0;

        // Impact from count (more DVs = higher impact)
        if count > 100 {
            impact += 0.3;
        } else if count > 50 {
            impact += 0.2;
        } else if count > 10 {
            impact += 0.1;
        }

        // Impact from size (larger DVs = higher impact)
        let size_mb = size as f64 / (1024.0 * 1024.0);
        if size_mb > 100.0 {
            impact += 0.3;
        } else if size_mb > 50.0 {
            impact += 0.2;
        } else if size_mb > 10.0 {
            impact += 0.1;
        }

        // Impact from age (older DVs = higher impact)
        if age > 30.0 {
            impact += 0.4;
        } else if age > 7.0 {
            impact += 0.2;
        }

        impact.min(1.0_f64)
    }

    fn detect_breaking_schema_changes(&self, old_schema: &Value, new_schema: &Value) -> bool {
        // Simplified breaking change detection
        // In a real implementation, this would be more sophisticated
        if let (Some(old_fields), Some(new_fields)) =
            (old_schema.get("fields"), new_schema.get("fields"))
        {
            if let (Some(old_fields_array), Some(new_fields_array)) =
                (old_fields.as_array(), new_fields.as_array())
            {
                // Check if any fields were removed
                let old_field_names: HashSet<String> = old_fields_array
                    .iter()
                    .filter_map(|f| {
                        f.get("name")
                            .and_then(|n| n.as_str())
                            .map(|s| s.to_string())
                    })
                    .collect();
                let new_field_names: HashSet<String> = new_fields_array
                    .iter()
                    .filter_map(|f| {
                        f.get("name")
                            .and_then(|n| n.as_str())
                            .map(|s| s.to_string())
                    })
                    .collect();

                // If any old fields are missing, it's a breaking change
                if !old_field_names.is_subset(&new_field_names) {
                    return true;
                }

                // Check for type changes in existing fields
                for old_field in old_fields_array {
                    if let Some(field_name) = old_field.get("name").and_then(|n| n.as_str()) {
                        if let Some(new_field) = new_fields_array
                            .iter()
                            .find(|f| f.get("name").and_then(|n| n.as_str()) == Some(field_name))
                        {
                            let old_type = old_field.get("type").and_then(|t| t.as_str());
                            let new_type = new_field.get("type").and_then(|t| t.as_str());

                            // If types changed, it's a breaking change
                            if old_type != new_type {
                                return true;
                            }

                            // Check if nullable changed from false to true (breaking)
                            let old_nullable = old_field
                                .get("nullable")
                                .and_then(|n| n.as_bool())
                                .unwrap_or(true);
                            let new_nullable = new_field
                                .get("nullable")
                                .and_then(|n| n.as_bool())
                                .unwrap_or(true);

                            if !old_nullable && new_nullable {
                                return true;
                            }
                        }
                    }
                }
            }
        }

        false
    }

    fn calculate_schema_metrics(
        &self,
        changes: Vec<SchemaChange>,
        current_version: u64,
    ) -> Result<Option<SchemaEvolutionMetrics>, Box<dyn Error + Send + Sync>> {
        let total_changes = changes.len();
        let breaking_changes = changes.iter().filter(|c| c.is_breaking).count();
        let non_breaking_changes = total_changes - breaking_changes;

        // Calculate time-based metrics
        let now = chrono::Utc::now().timestamp() as u64;
        let days_since_last = if let Some(last_change) = changes.last() {
            (now - last_change.timestamp / 1000) as f64 / 86400.0
        } else {
            365.0 // No changes in a year = very stable
        };

        // Calculate change frequency (changes per day)
        let total_days = if changes.len() > 1 {
            let first_change = changes.first().unwrap().timestamp / 1000;
            let last_change = changes.last().unwrap().timestamp / 1000;
            ((last_change - first_change) as f64 / 86400.0).max(1.0_f64)
        } else {
            1.0
        };

        let change_frequency = total_changes as f64 / total_days;

        // Calculate stability score
        let stability_score = self.calculate_schema_stability_score(
            total_changes,
            breaking_changes,
            change_frequency,
            days_since_last,
        );

        Ok(Some(SchemaEvolutionMetrics {
            total_schema_changes: total_changes,
            breaking_changes,
            non_breaking_changes,
            schema_stability_score: stability_score,
            days_since_last_change: days_since_last,
            schema_change_frequency: change_frequency,
            current_schema_version: current_version,
        }))
    }

    fn is_breaking_change(&self, previous_changes: &[SchemaChange], new_schema: &Value) -> bool {
        if previous_changes.is_empty() {
            return false;
        }

        let last_schema = &previous_changes.last().unwrap().schema;

        // Check for breaking changes:
        // 1. Column removal
        // 2. Column type changes
        // 3. Required field changes
        self.detect_breaking_schema_changes(last_schema, new_schema)
    }

    fn calculate_schema_stability_score(
        &self,
        total_changes: usize,
        breaking_changes: usize,
        frequency: f64,
        days_since_last: f64,
    ) -> f64 {
        let mut score: f64 = 1.0;

        // Penalize total changes
        if total_changes > 50 {
            score -= 0.3;
        } else if total_changes > 20 {
            score -= 0.2;
        } else if total_changes > 10 {
            score -= 0.1;
        }

        // Penalize breaking changes heavily
        if breaking_changes > 10 {
            score -= 0.4;
        } else if breaking_changes > 5 {
            score -= 0.3;
        } else if breaking_changes > 0 {
            score -= 0.2;
        }

        // Penalize high frequency changes
        if frequency > 1.0 {
            // More than 1 change per day
            score -= 0.3;
        } else if frequency > 0.5 {
            // More than 1 change every 2 days
            score -= 0.2;
        } else if frequency > 0.1 {
            // More than 1 change every 10 days
            score -= 0.1;
        }

        // Reward stability (no recent changes)
        if days_since_last > 30.0 {
            score += 0.1;
        } else if days_since_last > 7.0 {
            score += 0.05;
        }

        score.clamp(0.0_f64, 1.0_f64)
    }

    fn estimate_snapshot_size(&self, json: &Value) -> u64 {
        let mut size = 0u64;

        // Estimate size based on actions in the transaction log
        if let Some(add_actions) = json.get("add") {
            if let Some(add_array) = add_actions.as_array() {
                for add_action in add_array {
                    if let Some(file_size) = add_action.get("sizeInBytes") {
                        size += file_size.as_u64().unwrap_or(0);
                    }
                }
            }
        }

        // Add metadata overhead (estimated)
        size + 1024 // 1KB overhead per snapshot
    }

    fn calculate_storage_cost_impact(
        &self,
        total_size: u64,
        snapshot_count: usize,
        oldest_age: f64,
    ) -> f64 {
        let mut impact: f64 = 0.0;

        // Impact from total size
        let size_gb = total_size as f64 / (1024.0 * 1024.0 * 1024.0);
        if size_gb > 100.0 {
            impact += 0.4;
        } else if size_gb > 50.0 {
            impact += 0.3;
        } else if size_gb > 10.0 {
            impact += 0.2;
        } else if size_gb > 1.0 {
            impact += 0.1;
        }

        // Impact from snapshot count
        if snapshot_count > 1000 {
            impact += 0.3;
        } else if snapshot_count > 500 {
            impact += 0.2;
        } else if snapshot_count > 100 {
            impact += 0.1;
        }

        // Impact from age (older snapshots = higher cost)
        if oldest_age > 365.0 {
            impact += 0.3;
        } else if oldest_age > 90.0 {
            impact += 0.2;
        } else if oldest_age > 30.0 {
            impact += 0.1;
        }

        impact.min(1.0_f64)
    }

    fn calculate_retention_efficiency(
        &self,
        snapshot_count: usize,
        oldest_age: f64,
        newest_age: f64,
    ) -> f64 {
        let mut efficiency: f64 = 1.0;

        // Penalize too many snapshots
        if snapshot_count > 1000 {
            efficiency -= 0.4;
        } else if snapshot_count > 500 {
            efficiency -= 0.3;
        } else if snapshot_count > 100 {
            efficiency -= 0.2;
        } else if snapshot_count > 50 {
            efficiency -= 0.1;
        }

        // Reward appropriate retention period
        let retention_days = oldest_age - newest_age;
        if retention_days > 365.0 {
            efficiency -= 0.2; // Too long retention
        } else if retention_days < 7.0 {
            efficiency -= 0.1; // Too short retention
        }

        efficiency.clamp(0.0_f64, 1.0_f64)
    }

    fn calculate_recommended_retention(&self, snapshot_count: usize, oldest_age: f64) -> u64 {
        // Simple heuristic: recommend retention based on snapshot count and age
        if snapshot_count > 1000 || oldest_age > 365.0 {
            30 // 30 days for high snapshot count or very old data
        } else if snapshot_count > 500 || oldest_age > 90.0 {
            60 // 60 days for medium snapshot count or old data
        } else if snapshot_count > 100 || oldest_age > 30.0 {
            90 // 90 days for moderate snapshot count or recent data
        } else {
            180 // 180 days for low snapshot count and recent data
        }
    }

    fn extract_constraints_from_schema(
        &self,
        schema: &Value,
    ) -> (usize, usize, usize, usize, usize) {
        let mut total = 0;
        let mut check = 0;
        let mut not_null = 0;
        let mut unique = 0;
        let mut foreign_key = 0;

        if let Some(fields) = schema.get("fields") {
            if let Some(fields_array) = fields.as_array() {
                for field in fields_array {
                    total += 1;

                    // Check for NOT NULL constraint
                    if let Some(nullable) = field.get("nullable") {
                        if !nullable.as_bool().unwrap_or(true) {
                            not_null += 1;
                        }
                    }

                    // Check for other constraints (simplified)
                    if let Some(metadata) = field.get("metadata") {
                        if let Some(metadata_obj) = metadata.as_object() {
                            for (key, _) in metadata_obj {
                                if key.contains("constraint") || key.contains("check") {
                                    check += 1;
                                }
                                if key.contains("unique") {
                                    unique += 1;
                                }
                                if key.contains("foreign") || key.contains("reference") {
                                    foreign_key += 1;
                                }
                            }
                        }
                    }
                }
            }
        }

        (total, check, not_null, unique, foreign_key)
    }

    fn calculate_constraint_violation_risk(
        &self,
        total_constraints: usize,
        check_constraints: usize,
    ) -> f64 {
        if total_constraints == 0 {
            return 0.0;
        }

        // Higher risk with more complex constraints
        let complexity_ratio = check_constraints as f64 / total_constraints as f64;
        if complexity_ratio > 0.5 {
            0.8
        } else if complexity_ratio > 0.3 {
            0.6
        } else if complexity_ratio > 0.1 {
            0.4
        } else {
            0.2
        }
    }

    fn calculate_data_quality_score(&self, total_constraints: usize, violation_risk: f64) -> f64 {
        let mut score = 1.0;

        // Reward having constraints
        if total_constraints > 10 {
            score += 0.2;
        } else if total_constraints > 5 {
            score += 0.1;
        }

        // Penalize violation risk
        score -= violation_risk * 0.5;

        score.clamp(0.0_f64, 1.0_f64)
    }

    fn calculate_constraint_coverage_score(
        &self,
        total_constraints: usize,
        check_constraints: usize,
    ) -> f64 {
        if total_constraints == 0 {
            return 0.0;
        }

        let coverage_ratio = check_constraints as f64 / total_constraints as f64;
        if coverage_ratio > 0.5 {
            1.0
        } else if coverage_ratio > 0.3 {
            0.7
        } else if coverage_ratio > 0.1 {
            0.4
        } else {
            0.1
        }
    }

    async fn analyze_file_compaction(
        &self,
        data_files: &Vec<FileMetadata>,
        z_order_opportunity: bool,
        z_order_columns: Vec<String>,
    ) -> Result<Option<FileCompactionMetrics>, Box<dyn Error + Send + Sync>> {
        let mut small_files_count = 0;
        let mut small_files_size = 0u64;
        let mut potential_compaction_files = 0;
        let mut estimated_savings = 0u64;

        // Analyze file sizes for compaction opportunities
        for file in data_files {
            let file_size = file.size as u64;
            if file_size < 16 * 1024 * 1024 {
                // < 16MB
                small_files_count += 1;
                small_files_size += file_size;
                potential_compaction_files += 1;
            }
        }

        // Calculate potential savings
        if small_files_count > 1 {
            let target_size = 128 * 1024 * 1024; // 128MB target
            let files_per_target = (target_size as f64
                / (small_files_size as f64 / small_files_count as f64))
                .ceil() as usize;
            let target_files = (small_files_count as f64 / files_per_target as f64).ceil() as usize;
            let estimated_target_size = target_files as u64 * target_size / 2; // Conservative estimate
            estimated_savings = small_files_size.saturating_sub(estimated_target_size);
        }

        let compaction_opportunity = self.calculate_compaction_opportunity(
            small_files_count,
            small_files_size,
            data_files.len(),
        );
        let recommended_target_size = self.calculate_recommended_target_size(data_files);
        let compaction_priority =
            self.calculate_compaction_priority(compaction_opportunity, small_files_count);

        Ok(Some(FileCompactionMetrics {
            compaction_opportunity_score: compaction_opportunity,
            small_files_count,
            small_files_size_bytes: small_files_size,
            potential_compaction_files,
            estimated_compaction_savings_bytes: estimated_savings,
            recommended_target_file_size_bytes: recommended_target_size,
            compaction_priority,
            z_order_opportunity,
            z_order_columns,
        }))
    }

    fn calculate_compaction_opportunity(
        &self,
        small_files: usize,
        small_files_size: u64,
        total_files: usize,
    ) -> f64 {
        if total_files == 0 {
            return 0.0;
        }

        let small_file_ratio = small_files as f64 / total_files as f64;
        let _size_ratio = small_files_size as f64 / (small_files_size as f64 + 1.0); // Avoid division by zero

        if small_file_ratio > 0.8 {
            1.0
        } else if small_file_ratio > 0.6 {
            0.8
        } else if small_file_ratio > 0.4 {
            0.6
        } else if small_file_ratio > 0.2 {
            0.4
        } else {
            0.2
        }
    }

    fn calculate_recommended_target_size(&self, data_files: &Vec<FileMetadata>) -> u64 {
        if data_files.is_empty() {
            return 128 * 1024 * 1024; // 128MB default
        }

        let total_size = data_files.iter().map(|f| f.size as u64).sum::<u64>();
        let avg_size = total_size as f64 / data_files.len() as f64;

        // Recommend target size based on current average
        if avg_size < 16.0 * 1024.0 * 1024.0 {
            128 * 1024 * 1024 // 128MB for small files
        } else if avg_size < 64.0 * 1024.0 * 1024.0 {
            256 * 1024 * 1024 // 256MB for medium files
        } else {
            512 * 1024 * 1024 // 512MB for large files
        }
    }

    fn calculate_compaction_priority(&self, opportunity_score: f64, small_files: usize) -> String {
        if opportunity_score > 0.8 || small_files > 100 {
            "critical".to_string()
        } else if opportunity_score > 0.6 || small_files > 50 {
            "high".to_string()
        } else if opportunity_score > 0.4 || small_files > 20 {
            "medium".to_string()
        } else {
            "low".to_string()
        }
    }
}
