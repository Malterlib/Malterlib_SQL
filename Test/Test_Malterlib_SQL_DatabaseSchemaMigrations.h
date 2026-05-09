// Copyright © Unbroken AB
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

#pragma once

#include "Test_Malterlib_SQL_DatabaseSchema_v1.h"
#include "Test_Malterlib_SQL_DatabaseSchema.h"

namespace NMib::NSQL::NTest::NMigrations
{
	constexpr auto gc_SchemaVersions = fg_SqlSchemaVersions
		(
			fg_SqlSchemaMigration(NVersion1::gc_SchemaVersion)
			, fg_SqlSchemaMigration(gc_SchemaVersion)
		)
	;

	static_assert(gc_SchemaVersions.mc_nVersions == 2);
	static_assert(gc_SchemaVersions.m_Migrations.mc_nMigrations == 2);
	static_assert(gc_SchemaVersions.f_ForEachVersion([](auto const &...p_Versions) { return sizeof...(p_Versions); }) == 2);
}
