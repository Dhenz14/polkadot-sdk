//! Types used by Ethereum JSON-RPC subscriptions.

use codec::{Decode, Encode};
use pallet_revive::evm::{Address, BlockHeader, H160, H256, Log};
use scale_info::TypeInfo;
use serde::{Deserialize, Serialize};
use sp_core::ConstU32;
use sp_runtime::BoundedVec;
use std::{boxed::Box, collections::BTreeSet};

/// The kind of subscription the user is requesting from the eth-rpc.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum SubscriptionKind {
	/// Subscribe to newly imported block headers.
	NewBlockHeaders,
	/// Subscribe to emitted EVM logs.
	Logs,
}

/// Options passed by the user for their subscription to make it more specific.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum SubscriptionOptions {
	/// Options passed when subscribing for logs.
	LogsOptions {
		/// An optional address to use to filter the logs.
		///
		/// If specified, then only logs where this address is the emitter will be returned in the
		/// subscription. If not specified, then it means that there's no filtering based on the
		/// address of the emitter.
		///
		/// If it's specified as a vector of addresses then all of the addresses specified in the
		/// vector pass the filter.
		#[serde(default, skip_serializing_if = "Option::is_none")]
		address: Option<BoundedOneOrMany<Address, 1000>>,

		/// An optional set of topics to filter the logs by.
		///
		/// If not specified, then logs with any topic would match the filter. If specified, then
		/// only logs which match the specified topics pass the filter.
		#[serde(default, skip_serializing_if = "Option::is_none")]
		topics: Option<BoundedVec<Option<BoundedOneOrMany<H256, 1000>>, ConstU32<4>>>,
	},
}

/// A type used as a filter for logs in subscriptions.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogsSubscriptionFilter {
	/// Defines if the filter is configured to make use of addresses or not.
	addresses: Option<BTreeSet<H160>>,

	/// Defines if the filter is configured to filter based on the topics.
	topics: Option<[Option<BTreeSet<H256>>; 4]>,
}

impl LogsSubscriptionFilter {
	/// Constructs a new logs filter.
	pub fn new(
		address: Option<BoundedOneOrMany<Address, 1000>>,
		topics: Option<BoundedVec<Option<BoundedOneOrMany<H256, 1000>>, ConstU32<4>>>,
	) -> Self {
		Self {
			addresses: address.map(|addresses| addresses.into_iter().collect()),
			topics: topics.map(|topics| {
				let mut resolved_topics = [None, None, None, None];
				for (index, topic) in topics.into_iter().enumerate() {
					resolved_topics[index] =
						topic.map(|topic_filter| topic_filter.into_iter().collect());
				}
				resolved_topics
			}),
		}
	}

	/// Checks if a certain log matches this filter.
	pub fn matches(&self, log: &Log) -> bool {
		// Check the emitter address. If it doesn't match, then we return.
		if let Some(ref address_filter) = self.addresses &&
			!address_filter.contains(&log.address) &&
			!address_filter.is_empty()
		{
			return false;
		}

		// Check the topics filter to ensure that the log matches the topics filter.
		if let Some(ref topics_filters) = self.topics {
			let mut event_topics = log.topics.iter();
			for topics_filter in topics_filters {
				let event_topic = event_topics.next();

				match (topics_filter, event_topic) {
					// Wildcard filters.
					(None, _) => {},
					(Some(topic_filters), _) if topic_filters.is_empty() => {},
					// There's a filter but there's no topic at this index, return false at this
					// point.
					(Some(..), None) => return false,
					// There's a filter and there's also a topic at this index. So filter based on
					// it.
					(Some(topics_filter), Some(topic)) => {
						if !topics_filter.contains(topic) {
							return false;
						}
					},
				}
			}
		}

		true
	}
}

/// Resolved parameters for the subscription request which contains both the request type and the
/// options.
#[derive(Clone, Debug)]
pub enum SubscriptionParameters {
	/// Parameters for a new block headers subscription.
	NewBlockHeaders,
	/// Parameters for a logs subscription.
	Logs(LogsSubscriptionFilter),
}

impl SubscriptionParameters {
	/// Resolve user-provided subscription arguments into subscription parameters.
	pub fn new(
		subscription_kind: SubscriptionKind,
		subscription_options: Option<SubscriptionOptions>,
	) -> Option<Self> {
		match (subscription_kind, subscription_options) {
			(SubscriptionKind::Logs, None) => {
				Some(Self::Logs(LogsSubscriptionFilter::new(None, None)))
			},
			(
				SubscriptionKind::Logs,
				Some(SubscriptionOptions::LogsOptions { address, topics }),
			) => Some(Self::Logs(LogsSubscriptionFilter::new(address, topics))),
			(SubscriptionKind::NewBlockHeaders, None) => Some(Self::NewBlockHeaders),
			_ => None,
		}
	}
}

/// Item sent as an `eth_subscription` notification.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SubscriptionItem {
	/// A block header notification.
	BlockHeader(BlockHeader),
	/// An EVM log notification.
	Log(Log),
}

/// A helper type used when a type can be serialized and deserialized as either being one or as an
/// array.
#[derive(
	Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Encode, Decode, TypeInfo,
)]
#[serde(untagged)]
pub enum BoundedOneOrMany<T, const BOUND: u32> {
	/// One item.
	One(T),
	/// Many items, bounded by `BOUND`.
	Many(BoundedVec<T, ConstU32<BOUND>>),
}

impl<T: 'static, const BOUND: u32> IntoIterator for BoundedOneOrMany<T, BOUND> {
	type IntoIter = Box<dyn Iterator<Item = T>>;
	type Item = T;

	fn into_iter(self) -> Self::IntoIter {
		match self {
			BoundedOneOrMany::One(item) => Box::new(core::iter::once(item)) as _,
			BoundedOneOrMany::Many(bounded_vec) => Box::new(bounded_vec.into_iter()) as _,
		}
	}
}
