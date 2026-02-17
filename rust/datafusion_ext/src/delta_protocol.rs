use std::collections::{BTreeMap, HashMap, HashSet};

use deltalake::errors::DeltaTableError;
use deltalake::DeltaTable;
use deltalake::DeltaTableBuilder;
use rmp_serde::from_slice;
use serde::{Deserialize, Serialize};

use crate::delta_common::eager_snapshot;
use crate::DeltaFeatureGate;

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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum TableVersion {
    #[default]
    Latest,
    Version(i64),
    Timestamp(String),
}

impl TableVersion {
    pub fn from_options(
        version: Option<i64>,
        timestamp: Option<String>,
    ) -> Result<Self, DeltaTableError> {
        match (version, timestamp) {
            (Some(_), Some(_)) => Err(DeltaTableError::Generic(
                "cannot specify both version and timestamp".to_string(),
            )),
            (Some(version), None) => Ok(Self::Version(version)),
            (None, Some(timestamp)) => Ok(Self::Timestamp(timestamp)),
            (None, None) => Ok(Self::Latest),
        }
    }

    pub fn apply(self, builder: DeltaTableBuilder) -> Result<DeltaTableBuilder, DeltaTableError> {
        match self {
            Self::Latest => Ok(builder),
            Self::Version(version) => Ok(builder.with_version(version)),
            Self::Timestamp(timestamp) => builder.with_datestring(timestamp),
        }
    }
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
    let snapshot = eager_snapshot(table)?;
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
    let required_reader: HashSet<String> = gate.required_reader_features.iter().cloned().collect();
    if !required_reader.is_subset(&reader_features) {
        return Err(DeltaTableError::Generic(
            "Delta reader feature gate failed.".to_owned(),
        ));
    }
    let writer_features: HashSet<String> = snapshot.writer_features.iter().cloned().collect();
    let required_writer: HashSet<String> = gate.required_writer_features.iter().cloned().collect();
    if !required_writer.is_subset(&writer_features) {
        return Err(DeltaTableError::Generic(
            "Delta writer feature gate failed.".to_owned(),
        ));
    }
    Ok(())
}

#[derive(Debug, Clone, Deserialize)]
pub struct DeltaProtocolSnapshotPayload {
    pub min_reader_version: Option<i32>,
    pub min_writer_version: Option<i32>,
    pub reader_features: Vec<String>,
    pub writer_features: Vec<String>,
}

pub fn protocol_gate_snapshot(
    snapshot: &DeltaProtocolSnapshotPayload,
    gate: &DeltaFeatureGate,
) -> Result<(), DeltaTableError> {
    if let Some(min_reader) = gate.min_reader_version {
        match snapshot.min_reader_version {
            Some(reader_version) if reader_version >= min_reader => {}
            _ => {
                return Err(DeltaTableError::Generic(
                    "Delta reader protocol gate failed.".to_owned(),
                ));
            }
        }
    }
    if let Some(min_writer) = gate.min_writer_version {
        match snapshot.min_writer_version {
            Some(writer_version) if writer_version >= min_writer => {}
            _ => {
                return Err(DeltaTableError::Generic(
                    "Delta writer protocol gate failed.".to_owned(),
                ));
            }
        }
    }
    let reader_features: HashSet<String> = snapshot.reader_features.iter().cloned().collect();
    let required_reader: HashSet<String> = gate.required_reader_features.iter().cloned().collect();
    if !required_reader.is_subset(&reader_features) {
        return Err(DeltaTableError::Generic(
            "Delta reader feature gate failed.".to_owned(),
        ));
    }
    let writer_features: HashSet<String> = snapshot.writer_features.iter().cloned().collect();
    let required_writer: HashSet<String> = gate.required_writer_features.iter().cloned().collect();
    if !required_writer.is_subset(&writer_features) {
        return Err(DeltaTableError::Generic(
            "Delta writer feature gate failed.".to_owned(),
        ));
    }
    Ok(())
}

pub fn validate_protocol_gate_payload(
    snapshot_msgpack: &[u8],
    gate_msgpack: &[u8],
) -> Result<(), DeltaTableError> {
    let snapshot: DeltaProtocolSnapshotPayload = from_slice(snapshot_msgpack).map_err(|err| {
        DeltaTableError::Generic(format!("Failed to decode Delta snapshot: {err}"))
    })?;
    let gate: DeltaFeatureGate = from_slice(gate_msgpack).map_err(|err| {
        DeltaTableError::Generic(format!("Failed to decode Delta feature gate: {err}"))
    })?;
    protocol_gate_snapshot(&snapshot, &gate)
}

pub fn gate_from_parts(
    min_reader_version: Option<i32>,
    min_writer_version: Option<i32>,
    required_reader_features: Option<Vec<String>>,
    required_writer_features: Option<Vec<String>>,
) -> DeltaFeatureGate {
    let mut reader: Vec<String> = Vec::new();
    if let Some(features) = required_reader_features {
        reader.extend(features);
    }
    let mut writer: Vec<String> = Vec::new();
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

pub fn snapshot_info_as_strings(snapshot: &DeltaSnapshotInfo) -> HashMap<String, String> {
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
