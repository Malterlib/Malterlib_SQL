// Copyright © Unbroken AB
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

#pragma once

namespace NMib::NSQL::NPrivate
{
	// Configures the SQLite global allocator to use Malterlib's memory manager and disables
	// the internal memory-status mutex. Must be called before any other SQLite API.
	// The init is one-shot and shared across all SQLite backends; sqlite3_shutdown() runs
	// when the owning aggregate is destructed.
	void fg_SqliteEnsureInitialized();
}
