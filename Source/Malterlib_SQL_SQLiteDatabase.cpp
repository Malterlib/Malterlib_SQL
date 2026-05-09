// Copyright © Unbroken AB
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

#include "SQLiteDatabase/Malterlib_SQL_SQLiteDatabase_Internal.h"

namespace NMib::NSQL
{
	NConcurrency::TCActor<ICSqlDatabaseBackendActor> fg_CreateDatabaseBackendSQLite(ICSqlSchemaVersions const *_pSchemaVersions, CSQLiteDatabaseBackendSettings &&_Settings)
	{
		return NConcurrency::fg_ConstructActor<NPrivate::CSQLiteDatabaseBackendActor>(_pSchemaVersions, fg_Move(_Settings));
	}
}
