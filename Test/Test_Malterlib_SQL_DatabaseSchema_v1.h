// Copyright © Unbroken AB
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

#pragma once

#include <Mib/SQL/DatabaseSchema>

namespace NMib::NSQL::NTest::NVersion1
{
	struct CUserRow
	{
		int64 m_ID = 0;
		NStr::CStr m_Email;
		NStorage::TCOptional<NStr::CStr> m_DisplayName;
	};

	struct CSessionRow
	{
		int64 m_ID = 0;
		int64 m_UserID = 0;
		NStr::CStr m_Token;
	};

	constexpr auto gc_UserTable = fg_SqlTable<CUserRow>
		(
			NStr::gc_Str<"users">
			, fg_SqlColumns
			(
				fg_SqlColumn(NStr::gc_Str<"id">, &CUserRow::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement)
				, fg_SqlColumn(NStr::gc_Str<"email">, &CUserRow::m_Email)
				, fg_SqlColumn(NStr::gc_Str<"display_name">, &CUserRow::m_DisplayName)
			)
			, fg_SqlIndexes
			(
				fg_SqlIndex(NStr::gc_Str<"users_email_lookup">, NStr::gc_Str<"email">)
			)
			, fg_SqlConstraints
			(
				fg_SqlUnique(NStr::gc_Str<"users_email_unique">, NStr::gc_Str<"email">)
			)
		)
	;

	constexpr auto gc_SessionTable = fg_SqlTable<CSessionRow>
		(
			NStr::gc_Str<"sessions">
			, fg_SqlColumns
			(
				fg_SqlColumn(NStr::gc_Str<"id">, &CSessionRow::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement)
				, fg_SqlColumn(NStr::gc_Str<"user_id">, &CSessionRow::m_UserID)
				, fg_SqlColumn(NStr::gc_Str<"token">, &CSessionRow::m_Token)
			)
			, fg_SqlIndexes
			(
				fg_SqlIndex(NStr::gc_Str<"sessions_user_id">, NStr::gc_Str<"user_id">)
				, fg_SqlIndex(NStr::gc_Str<"sessions_token_unique">, ESqlIndexFlag::mc_Unique, NStr::gc_Str<"token">)
			)
			, fg_SqlConstraints
			(
				fg_SqlForeignKey
				(
					NStr::gc_Str<"sessions_user_fk">
					, fg_SqlColumnNames(NStr::gc_Str<"user_id">)
					, fg_SqlReferences(NStr::gc_Str<"users">, NStr::gc_Str<"id">)
					, ESqlForeignKeyAction::mc_Cascade
				)
			)
		)
	;

	constexpr auto gc_Database = fg_SqlDatabase
		(
			NStr::gc_Str<"build_coordinator">
			, gc_UserTable
			, gc_SessionTable
		)
	;

	constexpr auto gc_SchemaVersion = fg_SqlSchemaVersion(NStr::gc_Str<"0001">, gc_Database);

	static_assert(gc_Database.mc_nTables == 2);
	static_assert(gc_SchemaVersion.f_Database().mc_nTables == 2);
}
