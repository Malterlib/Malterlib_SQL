// Copyright © Unbroken AB
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

#include <Mib/SQL/DatabaseSchema>

namespace NMib::NSQL
{
	namespace
	{
		CSqlSchemaMigrationDescription const *fg_SqlFindMigration
			(
				NContainer::TCVector<CSqlSchemaMigrationDescription> const &_Migrations
				, NStr::CStr const &_FromVersionID
				, NStr::CStr const &_ToVersionID
			)
		{
			for (auto const &Migration : _Migrations)
			{
				if (*Migration.m_pFromVersionID == _FromVersionID && *Migration.m_pToVersionID == _ToVersionID)
					return &Migration;
			}

			return nullptr;
		}

		void fg_SqlInsertMigrationPlanStep
			(
				CSqlSchemaMigrationPlan &o_Plan
				, ESqlSchemaMigrationPlanStepType _Type
				, NStr::CStr _FromVersionID
				, NStr::CStr _ToVersionID
				, umint _nOperations = 0
			)
		{
			o_Plan.m_Steps.f_InsertLast
				(
					{
						.m_Type = _Type
						, .m_FromVersionID = fg_Move(_FromVersionID)
						, .m_ToVersionID = fg_Move(_ToVersionID)
						, .m_nOperations = _nOperations
					}
				)
			;
		}
	}

	CSqlSchemaMigrationPlan fg_SqlPlanSchemaMigration(ICSqlSchemaVersions const &_SchemaVersions, NStr::CStr const *_pCurrentVersionID)
	{
		CSqlSchemaMigrationPlan Plan;
		NContainer::TCVector<CSqlSchemaVersionDescription> Versions = _SchemaVersions.f_DescribeVersions();
		NContainer::TCVector<CSqlSchemaMigrationDescription> Migrations = _SchemaVersions.f_DescribeMigrations();
		if (Versions.f_IsEmpty())
			return Plan;

		NStr::CStr CurrentVersionID = _pCurrentVersionID ? *_pCurrentVersionID : NStr::CStr();
		if (CurrentVersionID.f_IsEmpty())
		{
			fg_SqlInsertMigrationPlanStep
				(
					Plan
					, ESqlSchemaMigrationPlanStepType::mc_CreateInitialSchema
					, NStr::CStr()
					, Versions[Versions.f_GetLen() - 1].f_ID()
					, Versions[Versions.f_GetLen() - 1].m_nTables
				)
			;
			for (auto const &Version : Versions)
				fg_SqlInsertMigrationPlanStep(Plan, ESqlSchemaMigrationPlanStepType::mc_MarkSchemaVersionApplied, NStr::CStr(), Version.f_ID());

			return Plan;
		}

		umint iCurrentVersion = umint(-1);
		for (umint iVersion = 0; iVersion < Versions.f_GetLen(); ++iVersion)
		{
			if (Versions[iVersion].f_ID() == CurrentVersionID)
			{
				iCurrentVersion = iVersion;
				break;
			}
		}

		if (iCurrentVersion == umint(-1))
		{
			using namespace NStr;

			Plan.m_Warnings.f_InsertLast("Current schema version '{}' is not present in the compiled schema versions"_f << CurrentVersionID);
			return Plan;
		}

		if (iCurrentVersion == Versions.f_GetLen() - 1)
		{
			fg_SqlInsertMigrationPlanStep(Plan, ESqlSchemaMigrationPlanStepType::mc_AlreadyCurrent, CurrentVersionID, CurrentVersionID);
			return Plan;
		}

		for (umint iVersion = iCurrentVersion + 1; iVersion < Versions.f_GetLen(); ++iVersion)
		{
			auto const &PreviousVersion = Versions[iVersion - 1];
			auto const &NextVersion = Versions[iVersion];
			if (auto const *pMigration = fg_SqlFindMigration(Migrations, PreviousVersion.f_ID(), NextVersion.f_ID()))
			{
				fg_SqlInsertMigrationPlanStep
					(
						Plan
						, ESqlSchemaMigrationPlanStepType::mc_ApplyMigrationOperations
						, PreviousVersion.f_ID()
						, NextVersion.f_ID()
						, pMigration->m_Operations.f_GetLen()
					)
				;
			}

			fg_SqlInsertMigrationPlanStep(Plan, ESqlSchemaMigrationPlanStepType::mc_SyncAdditiveSchema, PreviousVersion.f_ID(), NextVersion.f_ID(), NextVersion.m_nTables);
			fg_SqlInsertMigrationPlanStep(Plan, ESqlSchemaMigrationPlanStepType::mc_MarkSchemaVersionApplied, PreviousVersion.f_ID(), NextVersion.f_ID());
		}

		return Plan;
	}
}
