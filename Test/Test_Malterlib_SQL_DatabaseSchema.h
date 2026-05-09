// Copyright © Unbroken AB
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

#pragma once

#include <Mib/SQL/DatabaseSchema>

namespace NMib::NSQL::NTest
{
	struct CUserRow
	{
		uint64 m_ID = 0;
		NStr::CStr m_Email;
		NStorage::TCOptional<NStr::CStr> m_DisplayName;
		bool m_bAdmin = false;
	};

	struct CSessionRow
	{
		uint64 m_ID = 0;
		uint64 m_UserID = 0;
		NStr::CStr m_Token;
	};

	struct CUserRoleRow
	{
		uint64 m_UserID = 0;
		NStr::CStr m_Role;
	};

	constexpr auto gc_UserColumns = fg_SqlColumns
		(
			fg_SqlColumn(NStr::gc_Str<"id">, &CUserRow::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement)
			, fg_SqlColumn(NStr::gc_Str<"email">, &CUserRow::m_Email)
			, fg_SqlColumn(NStr::gc_Str<"display_name">, &CUserRow::m_DisplayName)
			, fg_SqlColumn(NStr::gc_Str<"admin">, &CUserRow::m_bAdmin, ESqlColumnFlag::mc_None, NStr::gc_Str<"0">)
		)
	;

	constexpr auto gc_UserTable = fg_SqlTable<CUserRow>
		(
			NStr::gc_Str<"users">
			, gc_UserColumns
			, fg_SqlIndexes
			(
				fg_SqlIndex<&CUserRow::m_Email>(gc_SqlIndexName<"users", "email">)
			)
			, fg_SqlConstraints
			(
				fg_SqlUnique<&CUserRow::m_Email>(gc_SqlUniqueName<"users", "email">)
				, fg_SqlCheck(NStr::gc_Str<"users_admin_bool">, NStr::gc_Str<"admin IN (FALSE, TRUE)">)
			)
		)
	;

	static_assert(gc_UserTable.mc_nColumns == 4);
	static_assert(gc_UserTable.mc_nIndexes == 1);
	static_assert(gc_UserTable.mc_nConstraints == 2);
	static_assert(gc_UserTable.f_ForEachColumn([](auto const &...p_Columns) { return sizeof...(p_Columns); }) == 4);
	static_assert(gc_UserTable.f_ForEachIndex([](auto const &...p_Indexes) { return sizeof...(p_Indexes); }) == 1);
	static_assert(gc_UserTable.f_ForEachConstraint([](auto const &...p_Constraints) { return sizeof...(p_Constraints); }) == 2);

	constexpr auto gc_SessionColumns = fg_SqlColumns
		(
			fg_SqlColumn(NStr::gc_Str<"id">, &CSessionRow::m_ID, ESqlColumnFlag::mc_PrimaryKey | ESqlColumnFlag::mc_AutoIncrement)
			, fg_SqlColumn(NStr::gc_Str<"user_id">, &CSessionRow::m_UserID)
			, fg_SqlColumn(NStr::gc_Str<"token">, &CSessionRow::m_Token)
		)
	;

	constexpr auto gc_SessionTable = fg_SqlTable<CSessionRow>
		(
			NStr::gc_Str<"sessions">
			, gc_SessionColumns
			, fg_SqlIndexes
			(
				fg_SqlIndex<&CSessionRow::m_UserID>(gc_SqlIndexName<"sessions", "user_id">)
				, fg_SqlIndex<&CSessionRow::m_Token>(gc_SqlUniqueName<"sessions", "token">, ESqlIndexFlag::mc_Unique)
			)
			, fg_SqlConstraints
			(
				fg_SqlForeignKey<&CSessionRow::m_UserID>
				(
					gc_SqlForeignKeyName<"sessions", "users", "user_id">
					, fg_SqlReferences<&CUserRow::m_ID>(gc_UserTable)
					, ESqlForeignKeyAction::mc_Cascade
				)
			)
		)
	;

	constexpr auto gc_UserRoleColumns = fg_SqlColumns
		(
			fg_SqlColumn(NStr::gc_Str<"user_id">, &CUserRoleRow::m_UserID)
			, fg_SqlColumn(NStr::gc_Str<"role">, &CUserRoleRow::m_Role)
		)
	;

	constexpr auto gc_UserRoleTable = fg_SqlTable<CUserRoleRow>
		(
			NStr::gc_Str<"user_roles">
			, gc_UserRoleColumns
			, fg_SqlIndexes
			(
				fg_SqlIndex<&CUserRoleRow::m_Role>(NStr::gc_Str<"user_roles_role">)
			)
			, fg_SqlConstraints
			(
				fg_SqlPrimaryKey<&CUserRoleRow::m_UserID, &CUserRoleRow::m_Role>(gc_SqlPrimaryKeyName<"user_roles", "user_id", "role">)
				, fg_SqlForeignKey<&CUserRoleRow::m_UserID>
				(
					NStr::gc_Str<"user_roles_user_fk">
					, fg_SqlReferences<&CUserRow::m_ID>(gc_UserTable)
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
			, gc_UserRoleTable
		)
	;

	constexpr auto gc_SchemaVersion = fg_SqlSchemaVersion(NStr::gc_Str<"0002">, gc_Database);

	static_assert(gc_Database.mc_nTables == 3);
	static_assert(gc_SchemaVersion.f_Database().mc_nTables == 3);
	static_assert(gc_Database.f_ForEachTable([](auto const &...p_Tables) { return sizeof...(p_Tables); }) == 3);
}
