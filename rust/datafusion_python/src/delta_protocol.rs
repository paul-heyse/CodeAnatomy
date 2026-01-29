use std::collections::{BTreeMap, HashMap, HashSet};

use deltalake::errors::DeltaTableError;
use deltalake::DeltaTable;

#[derive(Debug, Clone, Default)]
pub struct DeltaFeatureGate {
    pub min_reader_version: Option<i32>,
    pub min_writer_version: Option<i32>,
    pub required_reader_features: HashSet<String>,
    pub required_writer_features: HashSet<String>,
}

#[derive(Debug, Clone)]
pub struct DeltaSnapshotInfo {
    pub table_uri: String,
    pub version: i64,
    pub snapshot_timestamp: Option<i64>,
    pub min_reader_version: i32,
    pub min_writer_version: i32,
    pub reader_features: Vec<String>,
    pub writer_features: Vec<String>,
    pub table_properties: BTreeMap<String, String>,
    pub schema_json: String,
    pub partition_columns: Vec<String>,
}

fn feature_strings<T>(features: Option<&[T]>) -> Vec<String>
where
    T: ToString,
{
    let mut names: Vec<String> = match features {
        Some(items) => items.iter().map(ToString::to_string).collect(),
        None => Vec::new(),
    };
    names.sort();
    names
}

pub async fn delta_snapshot_info(
    table_uri: &str,
    table: &DeltaTable,
) -> Result<DeltaSnapshotInfo, DeltaTableError> {
    let snapshot = table.snapshot()?.snapshot().clone();
    let protocol = snapshot.protocol();
    let metadata = snapshot.metadata();
    let version = snapshot.version();
    let mut table_properties: BTreeMap<String, String> = BTreeMap::new();
    for (key, value) in metadata.configuration() {
        table_properties.insert(key.clone(), value.clone());
    }
    let schema = metadata.parse_schema().map_err(|err| {
        DeltaTableError::Generic(format!("Failed to parse Delta schema: {err:?}"))
    })?;
    let schema_json = serde_json::to_string(&schema)
        .map_err(|err| DeltaTableError::Generic(format!("Failed to encode Delta schema: {err}")))?;
    let mut history = table.history(Some(1)).await?;
    let snapshot_timestamp = history.next().and_then(|commit| commit.timestamp);
    Ok(DeltaSnapshotInfo {
        table_uri: table_uri.to_owned(),
        version,
        snapshot_timestamp,
        min_reader_version: protocol.min_reader_version(),
        min_writer_version: protocol.min_writer_version(),
        reader_features: feature_strings(protocol.reader_features()),
        writer_features: feature_strings(protocol.writer_features()),
        table_properties,
        schema_json,
        partition_columns: metadata.partition_columns().to_vec(),
    })
}

pub fn protocol_gate(
    snapshot: &DeltaSnapshotInfo,
    gate: &DeltaFeatureGate,
) -> Result<(), DeltaTableError> {
    if let Some(min_reader) = gate.min_reader_version {
        if snapshot.min_reader_version < min_reader {
            return Err(DeltaTableError::Generic(
                "Delta reader protocol gate failed.".to_owned(),
            ));
        }
    }
    if let Some(min_writer) = gate.min_writer_version {
        if snapshot.min_writer_version < min_writer {
            return Err(DeltaTableError::Generic(
                "Delta writer protocol gate failed.".to_owned(),
            ));
        }
    }
    let reader_features: HashSet<String> = snapshot.reader_features.iter().cloned().collect();
    if !gate.required_reader_features.is_subset(&reader_features) {
        return Err(DeltaTableError::Generic(
            "Delta reader feature gate failed.".to_owned(),
        ));
    }
    let writer_features: HashSet<String> = snapshot.writer_features.iter().cloned().collect();
    if !gate.required_writer_features.is_subset(&writer_features) {
        return Err(DeltaTableError::Generic(
            "Delta writer feature gate failed.".to_owned(),
        ));
    }
    Ok(())
}

pub fn gate_from_parts(
    min_reader_version: Option<i32>,
    min_writer_version: Option<i32>,
    required_reader_features: Option<Vec<String>>,
    required_writer_features: Option<Vec<String>>,
) -> DeltaFeatureGate {
    let mut reader: HashSet<String> = HashSet::new();
    if let Some(features) = required_reader_features {
        reader.extend(features);
    }
    let mut writer: HashSet<String> = HashSet::new();
    if let Some(features) = required_writer_features {
        writer.extend(features);
    }
    DeltaFeatureGate {
        min_reader_version,
        min_writer_version,
        required_reader_features: reader,
        required_writer_features: writer,
    }
}

pub fn snapshot_payload(snapshot: &DeltaSnapshotInfo) -> HashMap<String, String> {
    let mut payload: HashMap<String, String> = HashMap::new();
    payload.insert("table_uri".to_owned(), snapshot.table_uri.clone());
    payload.insert("version".to_owned(), snapshot.version.to_string());
    payload.insert(
        "min_reader_version".to_owned(),
        snapshot.min_reader_version.to_string(),
    );
    payload.insert(
        "min_writer_version".to_owned(),
        snapshot.min_writer_version.to_string(),
    );
    if let Some(ts) = snapshot.snapshot_timestamp {
        payload.insert("snapshot_timestamp".to_owned(), ts.to_string());
    }
    payload
}
