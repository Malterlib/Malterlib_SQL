// Copyright © Unbroken AB
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

#pragma once

#include <Mib/SQL/Database>

namespace NMib::NSQL
{
	struct CSQLiteDatabaseBackendSettings
	{
		NStr::CStr m_Path;
		umint m_nReadConnections = 4;
		umint m_nSelectRowsPerBatch = 128;
		uint32 m_nPipelineLength = 5;
	};

	NConcurrency::TCActor<ICSqlDatabaseBackendActor> fg_CreateDatabaseBackendSQLite(ICSqlSchemaVersions const *_pSchemaVersions, CSQLiteDatabaseBackendSettings &&_Settings);
	NConcurrency::TCWrapped<CSqlSchemaMigrationPlan> fg_SqlitePlanSchemaMigration(ICSqlSchemaVersions const &_SchemaVersions, NStr::CStr const *_pCurrentVersionID = nullptr);
}

#ifndef DMibPNoShortCuts
using namespace NMib::NSQL;
#endif
