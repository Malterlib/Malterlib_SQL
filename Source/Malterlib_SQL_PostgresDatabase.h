// Copyright © Unbroken AB
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

#pragma once

#include <Mib/SQL/Database>
#include <Mib/SQL/PostgresClient>

namespace NMib::NSQL
{
	struct CPostgresDatabaseBackendSettings
	{
		CPostgresConnectionSettings m_ConnectionSettings;
		umint m_ConnectionPoolSize = 16;
		umint m_nSelectRowsPerBatch = 128;
		uint32 m_nPipelineLength = 5;
	};

	NConcurrency::TCActor<ICSqlDatabaseBackendActor> fg_CreateDatabaseBackendPostgres(ICSqlSchemaVersions const *_pSchemaVersions, CPostgresDatabaseBackendSettings &&_Settings);
	NConcurrency::TCWrapped<CSqlSchemaMigrationPlan> fg_PostgresPlanSchemaMigration(ICSqlSchemaVersions const &_SchemaVersions, NStr::CStr const *_pCurrentVersionID = nullptr);
}

#ifndef DMibPNoShortCuts
using namespace NMib::NSQL;
#endif
