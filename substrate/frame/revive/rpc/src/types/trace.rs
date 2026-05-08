// This file is part of Substrate.

// Copyright (C) Parity Technologies (UK) Ltd.
// SPDX-License-Identifier: Apache-2.0

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//! Ethereum debug-tracing request types owned by the Ethereum RPC crate.

use pallet_revive::evm::{
	CallTracerConfig, ExecutionTracerConfig, PrestateTracerConfig, StateOverrideSet, TracerType,
};
use serde::{
	Deserialize, Serialize,
	de::{DeserializeOwned, Error},
};
use serde_json::Value;

/// Tracer configuration used by Ethereum debug trace RPC methods.
///
/// Ethereum clients accept two shapes here: an explicit tracer shape with
/// `tracer` and `tracerConfig`, and an inline execution-tracer shape where
/// execution options such as `enableMemory` live at the top level.
#[derive(Debug, Clone, Default, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TracerConfig {
	/// The tracer type requested by the caller.
	#[serde(flatten, default)]
	pub config: TracerType,

	/// Timeout for the tracing operation.
	#[serde(with = "humantime_serde", default)]
	pub timeout: Option<core::time::Duration>,
}

impl<'de> Deserialize<'de> for TracerConfig {
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: serde::Deserializer<'de>,
	{
		let raw = RawTracerConfig::deserialize(deserializer)?;
		let config = match raw.tracer {
			Some(TracerKind::CallTracer) => TracerType::CallTracer(
				decode_optional_tracer_config::<CallTracerConfig, D::Error>(raw.tracer_config)?,
			),
			Some(TracerKind::PrestateTracer) => TracerType::PrestateTracer(
				decode_optional_tracer_config::<PrestateTracerConfig, D::Error>(
					raw.tracer_config,
				)?,
			),
			Some(TracerKind::ExecutionTracer) => TracerType::ExecutionTracer(
				decode_optional_tracer_config::<ExecutionTracerConfig, D::Error>(
					raw.tracer_config,
				)?,
			),
			None => TracerType::ExecutionTracer(Some(raw.execution_tracer_config)),
		};

		Ok(Self { config, timeout: raw.timeout })
	}
}

/// Raw tracer request fields used to preserve the accepted debug RPC shapes.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawTracerConfig {
	/// Explicit tracer requested by the caller.
	#[serde(default)]
	tracer: Option<TracerKind>,
	/// Optional explicit tracer-specific configuration object.
	#[serde(default)]
	tracer_config: Option<Value>,
	/// Timeout for the tracing operation.
	#[serde(with = "humantime_serde", default)]
	timeout: Option<core::time::Duration>,
	/// Inline execution-tracer options used when no explicit tracer is present.
	#[serde(flatten, default)]
	execution_tracer_config: ExecutionTracerConfig,
}

/// Supported tracer names accepted by the debug tracing RPC.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
enum TracerKind {
	/// Geth-compatible call tracer.
	CallTracer,
	/// Geth-compatible prestate tracer.
	PrestateTracer,
	/// Revive execution tracer for opcodes and syscalls.
	ExecutionTracer,
}

/// Decode an optional `tracerConfig` object for an explicit tracer.
fn decode_optional_tracer_config<T, E>(value: Option<Value>) -> Result<Option<T>, E>
where
	T: DeserializeOwned,
	E: Error,
{
	value.map(serde_json::from_value::<T>).transpose().map_err(E::custom)
}

/// Configuration for `debug_traceCall`.
///
/// This extends [`TracerConfig`] with state overrides. Geth accepts this as a
/// superset of the base tracer config. `blockOverrides` and `txIndex` are not
/// supported yet.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TraceCallConfig {
	/// The base tracer configuration.
	#[serde(flatten)]
	pub tracer_config: TracerConfig,

	/// Optional state overrides to apply before executing the traced call.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub state_overrides: Option<StateOverrideSet>,
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn deserializes_explicit_and_inline_tracer_config_shapes() {
		// Arrange
		let cases = vec![
			(
				r#"{ "enableMemory": true, "disableStack": false, "disableStorage": false,
		"enableReturnData": true }"#,
				TracerConfig {
					config: TracerType::ExecutionTracer(Some(ExecutionTracerConfig {
						enable_memory: true,
						disable_stack: false,
						disable_storage: false,
						enable_return_data: true,
						disable_syscall_details: false,
						limit: None,
						memory_word_limit: 16,
					})),
					timeout: None,
				},
			),
			(
				r#"{  }"#,
				TracerConfig {
					config: TracerType::ExecutionTracer(Some(ExecutionTracerConfig::default())),
					timeout: None,
				},
			),
			(
				r#"{"tracer": null, "enableMemory": true}"#,
				TracerConfig {
					config: ExecutionTracerConfig { enable_memory: true, ..Default::default() }
						.into(),
					timeout: None,
				},
			),
			(
				r#"{"tracer": "callTracer"}"#,
				TracerConfig { config: TracerType::CallTracer(None), timeout: None },
			),
			(
				r#"{"tracer": "callTracer", "tracerConfig": { "withLogs": false }}"#,
				TracerConfig {
					config: CallTracerConfig { with_logs: false, only_top_call: false }.into(),
					timeout: None,
				},
			),
			(
				r#"{"tracer": "callTracer", "tracerConfig": { "onlyTopCall": true }}"#,
				TracerConfig {
					config: CallTracerConfig { with_logs: true, only_top_call: true }.into(),
					timeout: None,
				},
			),
			(
				r#"{"tracer": "callTracer", "tracerConfig": { "onlyTopCall": true }, "timeout":
		"10ms"}"#,
				TracerConfig {
					config: CallTracerConfig { with_logs: true, only_top_call: true }.into(),
					timeout: Some(core::time::Duration::from_millis(10)),
				},
			),
			(
				r#"{"tracer": "executionTracer"}"#,
				TracerConfig { config: TracerType::ExecutionTracer(None), timeout: None },
			),
			(
				r#"{"tracer": "executionTracer", "tracerConfig": { "enableMemory": true }}"#,
				TracerConfig {
					config: ExecutionTracerConfig { enable_memory: true, ..Default::default() }
						.into(),
					timeout: None,
				},
			),
			(
				r#"{ "enableMemory": true }"#,
				TracerConfig {
					config: ExecutionTracerConfig { enable_memory: true, ..Default::default() }
						.into(),
					timeout: None,
				},
			),
		];

		// Act
		let results = cases
			.iter()
			.map(|(json_data, _)| serde_json::from_str::<TracerConfig>(json_data))
			.collect::<Result<Vec<_>, _>>();

		// Assert
		let expected = cases.into_iter().map(|(_, expected)| expected).collect::<Vec<_>>();
		assert_eq!(results.expect("Deserialization should succeed"), expected);
	}

	#[test]
	fn rejects_unknown_explicit_tracer_name() {
		// Arrange
		let json_data = r#"{"tracer": "unknownTracer"}"#;

		// Act
		let result = serde_json::from_str::<TracerConfig>(json_data);

		// Assert
		assert!(result.is_err(), "unknown explicit tracer names should be rejected");
	}
}
