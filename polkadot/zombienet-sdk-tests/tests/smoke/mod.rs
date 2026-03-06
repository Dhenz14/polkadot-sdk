// Copyright (C) Parity Technologies (UK) Ltd.
// SPDX-License-Identifier: Apache-2.0

mod collator_revamp_interop;
#[cfg(feature = "zombie-metadata")]
mod coretime_revenue;

#[cfg(feature = "zombie-ci")]
mod coretime_smoke;

#[cfg(feature = "zombie-ci")]
mod deregister_register_validator;

#[cfg(feature = "zombie-ci")]
mod parachains_smoke;

#[cfg(feature = "zombie-ci")]
mod precompile_pvf_smoke;
