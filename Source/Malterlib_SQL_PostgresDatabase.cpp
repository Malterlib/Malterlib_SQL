// Copyright © Unbroken AB
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

#include "PostgresDatabase/Malterlib_SQL_PostgresDatabase_Internal.h"

namespace NMib::NSQL
{
	NConcurrency::TCActor<ICSqlDatabaseBackendActor> fg_CreateDatabaseBackendPostgres(ICSqlSchemaVersions const *_pSchemaVersions, CPostgresDatabaseBackendSettings &&_Settings)
	{
		return NConcurrency::fg_ConstructActor<NPrivate::CPostgresDatabaseBackendActor>(_pSchemaVersions, fg_Move(_Settings));
	}
}
