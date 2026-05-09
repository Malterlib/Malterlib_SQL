// Copyright © Unbroken AB
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

#pragma once

namespace NMib::NSQL
{
	constexpr NStr::CStr const &CSqlColumnDescription::f_Name() const
	{
		return *m_pName;
	}

	constexpr NStr::CStr const &CSqlColumnDescription::f_DefaultSql() const
	{
		return *m_pDefaultSql;
	}

	constexpr NStr::CStr const &CSqlColumnDescription::f_Comment() const
	{
		return *m_pComment;
	}

	inline CNonPortableColumnOptions const *CSqlColumnDescription::f_NonPortableOptions(NStr::CStr const &_BackendID) const
	{
		for (umint i = 0; i < m_nNonPortableOptions; ++i)
			if (*m_pNonPortableOptions[i].m_pBackendID == _BackendID)
				return &m_pNonPortableOptions[i];

		return nullptr;
	}

	constexpr bool CSqlColumnDescription::f_IsNullable() const
	{
		return fg_IsSet(m_Flags, ESqlColumnFlag::mc_Nullable);
	}

	constexpr bool CSqlColumnDescription::f_IsPrimaryKey() const
	{
		return fg_IsSet(m_Flags, ESqlColumnFlag::mc_PrimaryKey);
	}

	constexpr NStr::CStr const &CSqlTableDescription::f_Name() const
	{
		return *m_pName;
	}

	constexpr NStr::CStr const &CSqlSchemaVersionDescription::f_ID() const
	{
		return *m_pID;
	}

	constexpr NStr::CStr const &CSqlSchemaVersionDescription::f_DatabaseName() const
	{
		return *m_pDatabaseName;
	}

	inline void fg_SqlAppendChecksumString(NStr::CStr &_Checksum, NStr::CStr const &_Value)
	{
		using namespace NStr;

		_Checksum += "{}:{};"_f << _Value.f_GetLen() << _Value;
	}

	inline void fg_SqlAppendChecksumStringPointer(NStr::CStr &_Checksum, NStr::CStr const *_pValue)
	{
		if (_pValue)
		{
			_Checksum += "present;";
			fg_SqlAppendChecksumString(_Checksum, *_pValue);
		}
		else
			_Checksum += "null;";
	}

	inline void fg_SqlAppendChecksumStringPointerList(NStr::CStr &_Checksum, NContainer::TCVector<NStr::CStr const *> const &_Values)
	{
		using namespace NStr;

		_Checksum += "{};"_f << _Values.f_GetLen();
		for (auto const *pValue : _Values)
			fg_SqlAppendChecksumStringPointer(_Checksum, pValue);
	}

	inline NStr::CStr fg_SqlSchemaVersionChecksum(NStr::CStr const &_ID, CSqlDatabaseDescription const &_Database)
	{
		using namespace NStr;

		NStr::CStr Checksum = "schema-v1;";

		fg_SqlAppendChecksumString(Checksum, _ID);
		fg_SqlAppendChecksumString(Checksum, _Database.f_Name());

		for (auto const &Table : _Database.m_Tables)
		{
			fg_SqlAppendChecksumString(Checksum, Table.f_Name());
			Checksum += "cols:{};"_f << Table.m_Columns.f_GetLen();

			for (auto const &Column : Table.m_Columns)
			{
				fg_SqlAppendChecksumString(Checksum, Column.f_Name());
				Checksum += "type:{};flags:{};"_f << uint8(Column.m_Type) << uint32(Column.m_Flags);
				fg_SqlAppendChecksumString(Checksum, Column.f_DefaultSql());
				fg_SqlAppendChecksumString(Checksum, Column.f_Comment());
				Checksum += "nonportable:{};"_f << Column.m_nNonPortableOptions;

				for (umint iOption = 0; iOption < Column.m_nNonPortableOptions; ++iOption)
				{
					auto const &Option = Column.m_pNonPortableOptions[iOption];
					fg_SqlAppendChecksumString(Checksum, *Option.m_pBackendID);
					fg_SqlAppendChecksumString(Checksum, *Option.m_pDefaultSql);
					fg_SqlAppendChecksumString(Checksum, *Option.m_pCollationSql);
					fg_SqlAppendChecksumString(Checksum, *Option.m_pGeneratedSql);
					fg_SqlAppendChecksumString(Checksum, *Option.m_pCustomSql);
				}
			}

			Checksum += "indexes:{};"_f << Table.m_Indexes.f_GetLen();
			for (auto const &Index : Table.m_Indexes)
			{
				fg_SqlAppendChecksumString(Checksum, Index.f_Name());
				Checksum += "flags:{};columns:"_f << uint32(Index.m_Flags);
				fg_SqlAppendChecksumStringPointerList(Checksum, Index.m_Columns);
			}

			Checksum += "constraints:{};"_f << Table.m_Constraints.f_GetLen();
			for (auto const &Constraint : Table.m_Constraints)
			{
				fg_SqlAppendChecksumString(Checksum, Constraint.f_Name());
				Checksum += "type:{};columns:"_f << uint8(Constraint.m_Type);
				fg_SqlAppendChecksumStringPointerList(Checksum, Constraint.m_Columns);
				Checksum += "referenced-table:";
				fg_SqlAppendChecksumStringPointer(Checksum, Constraint.m_pReferencedTable);
				Checksum += "referenced-columns:";
				fg_SqlAppendChecksumStringPointerList(Checksum, Constraint.m_ReferencedColumns);
				Checksum += "check:";
				fg_SqlAppendChecksumStringPointer(Checksum, Constraint.m_pCheckSql);
				Checksum += "delete:{};update:{};"_f << uint8(Constraint.m_OnDelete) << uint8(Constraint.m_OnUpdate);
			}
		}

		return Checksum;
	}

	template <typename t_CType, ESqlColumnType t_ColumnType, ESqlValueType t_ValueType>
	auto TCSqlPrimitiveTypeTraits<t_CType, t_ColumnType, t_ValueType>::fs_ToSqlStorage(CSqlType const &_Value) -> CSqlType const &
	{
		return _Value;
	}

	template <typename t_CType, ESqlColumnType t_ColumnType, ESqlValueType t_ValueType>
	auto TCSqlPrimitiveTypeTraits<t_CType, t_ColumnType, t_ValueType>::fs_ToSqlStorage(CSqlType &&_Value) -> CSqlType &&
	{
		return fg_Move(_Value);
	}

	template <typename t_CType, ESqlColumnType t_ColumnType, ESqlValueType t_ValueType>
	auto TCSqlPrimitiveTypeTraits<t_CType, t_ColumnType, t_ValueType>::fs_FromSqlStorage(CSqlType const &_Value) -> CSqlType const &
	{
		return _Value;
	}

	template <typename t_CType, ESqlColumnType t_ColumnType, ESqlValueType t_ValueType>
	auto TCSqlPrimitiveTypeTraits<t_CType, t_ColumnType, t_ValueType>::fs_FromSqlStorage(CSqlType &&_Value) -> CSqlType &&
	{
		return fg_Move(_Value);
	}

	template <typename t_CType, typename t_CSqlType>
	auto TCSqlMappedTypeTraits<t_CType, t_CSqlType>::fs_ToSqlStorage(t_CType const &_Value) -> CSqlType
	{
		return CSqlType(_Value);
	}

	template <typename t_CType, typename t_CSqlType>
	auto TCSqlMappedTypeTraits<t_CType, t_CSqlType>::fs_ToSqlStorage(t_CType &&_Value) -> CSqlType
	{
		return CSqlType(fg_Move(_Value));
	}

	template <typename t_CType, typename t_CSqlType>
	auto TCSqlMappedTypeTraits<t_CType, t_CSqlType>::fs_FromSqlStorage(CSqlType const &_Value) -> t_CType
	{
		return t_CType(_Value);
	}

	template <typename t_CType, typename t_CSqlType>
	auto TCSqlMappedTypeTraits<t_CType, t_CSqlType>::fs_FromSqlStorage(CSqlType &&_Value) -> t_CType
	{
		return t_CType(fg_Move(_Value));
	}

	constexpr NStr::CStr const &CSqlDatabaseDescription::f_Name() const
	{
		return *m_pName;
	}

	inline constexpr CSqlSchemaRenameTable::CSqlSchemaRenameTable(NStr::CStr const *_pOldName, NStr::CStr const *_pNewName)
		: m_pOldName(_pOldName)
		, m_pNewName(_pNewName)
	{
	}

	inline CSqlSchemaMigrationOperationDescription CSqlSchemaRenameTable::f_Describe() const
	{
		return
			{
				.m_Type = ESqlSchemaMigrationOperationType::mc_RenameTable
				, .m_pOldName = m_pOldName
				, .m_pNewName = m_pNewName
			}
		;
	}

	inline constexpr TCSqlSchemaRenameColumn::TCSqlSchemaRenameColumn(NStr::CStr const *_pTableName, NStr::CStr const *_pOldName, NStr::CStr const *_pNewName)
		: m_pTableName(_pTableName)
		, m_pOldName(_pOldName)
		, m_pNewName(_pNewName)
	{
	}

	inline CSqlSchemaMigrationOperationDescription TCSqlSchemaRenameColumn::f_Describe() const
	{
		return
			{
				.m_Type = ESqlSchemaMigrationOperationType::mc_RenameColumn
				, .m_pTableName = m_pTableName
				, .m_pOldName = m_pOldName
				, .m_pNewName = m_pNewName
			}
		;
	}

	inline constexpr CSqlSchemaRebuildTable::CSqlSchemaRebuildTable(NStr::CStr const *_pTableName)
		: m_pTableName(_pTableName)
	{
	}

	inline CSqlSchemaMigrationOperationDescription CSqlSchemaRebuildTable::f_Describe() const
	{
		return
			{
				.m_Type = ESqlSchemaMigrationOperationType::mc_RebuildTable
				, .m_pTableName = m_pTableName
			}
		;
	}

	inline constexpr CSqlSchemaDropTable::CSqlSchemaDropTable(NStr::CStr const *_pTableName)
		: m_pTableName(_pTableName)
	{
	}

	inline CSqlSchemaMigrationOperationDescription CSqlSchemaDropTable::f_Describe() const
	{
		return
			{
				.m_Type = ESqlSchemaMigrationOperationType::mc_DropTable
				, .m_pTableName = m_pTableName
			}
		;
	}

	inline constexpr CSqlSchemaDropColumn::CSqlSchemaDropColumn(NStr::CStr const *_pTableName, NStr::CStr const *_pColumnName)
		: m_pTableName(_pTableName)
		, m_pColumnName(_pColumnName)
	{
	}

	inline CSqlSchemaMigrationOperationDescription CSqlSchemaDropColumn::f_Describe() const
	{
		return
			{
				.m_Type = ESqlSchemaMigrationOperationType::mc_DropColumn
				, .m_pTableName = m_pTableName
				, .m_pOldName = m_pColumnName
			}
		;
	}

	inline constexpr CSqlSchemaUpdateColumnSql::CSqlSchemaUpdateColumnSql(NStr::CStr const *_pTableName, NStr::CStr const *_pColumnName, NStr::CStr const *_pSql)
		: m_pTableName(_pTableName)
		, m_pColumnName(_pColumnName)
		, m_pSql(_pSql)
	{
	}

	inline CSqlSchemaMigrationOperationDescription CSqlSchemaUpdateColumnSql::f_Describe() const
	{
		return
			{
				.m_Type = ESqlSchemaMigrationOperationType::mc_UpdateColumnSql
				, .m_pTableName = m_pTableName
				, .m_pNewName = m_pColumnName
				, .m_pSql = m_pSql
			}
		;
	}

	constexpr NStr::CStr const &CSqlIndexDescription::f_Name() const
	{
		return *m_pName;
	}

	constexpr bool CSqlIndexDescription::f_IsUnique() const
	{
		return fg_IsSet(m_Flags, ESqlIndexFlag::mc_Unique);
	}

	constexpr NStr::CStr const &CSqlConstraintDescription::f_Name() const
	{
		return *m_pName;
	}

	constexpr NStr::CStr const &CSqlConstraintDescription::f_ReferencedTable() const
	{
		return *m_pReferencedTable;
	}

	constexpr NStr::CStr const &CSqlConstraintDescription::f_CheckSql() const
	{
		return *m_pCheckSql;
	}

	template <typename t_CRow, typename t_CMember>
	constexpr TCSqlColumn<t_CRow, t_CMember>::TCSqlColumn(NStr::CStr const *_pName, t_CMember t_CRow::*_pMember, ESqlColumnType _Type, CSqlColumnOptions _Options)
		: m_pName(_pName)
		, m_pMember(_pMember)
		, m_Type(_Type)
		, m_Flags(_Options.m_Flags)
		, m_pDefaultSql(_Options.m_pDefaultSql)
		, m_Options(_Options)
	{
		if (_Type != TCSqlColumnType<CMember>::mc_Type)
			throw "SQL column type override does not match member type";
	}

	template <typename t_CRow, typename t_CMember>
	constexpr NStr::CStr const &TCSqlColumn<t_CRow, t_CMember>::f_Name() const
	{
		return *m_pName;
	}

	template <typename t_CRow, typename t_CMember>
	constexpr NStr::CStr const &TCSqlColumn<t_CRow, t_CMember>::f_DefaultSql() const
	{
		return *m_Options.m_pDefaultSql;
	}

	template <typename t_CRow, typename t_CMember>
	CSqlColumnDescription TCSqlColumn<t_CRow, t_CMember>::f_Describe() const
	{
		return
			{
				.m_pColumn = this
				, .m_pName = m_pName
				, .m_Type = m_Type
				, .m_Flags = f_IsNullable() ? m_Options.m_Flags | ESqlColumnFlag::mc_Nullable : m_Options.m_Flags
				, .m_pDefaultSql = m_Options.m_pDefaultSql
				, .m_pComment = m_Options.m_pComment
				, .m_pNonPortableOptions = m_Options.m_NonPortableOptions
				, .m_nNonPortableOptions = m_Options.m_nNonPortableOptions
			}
		;
	}

	template <typename t_CRow, typename t_CMember>
	constexpr t_CMember &TCSqlColumn<t_CRow, t_CMember>::f_Value(t_CRow &_Row) const
	{
		return _Row.*m_pMember;
	}

	template <typename t_CRow, typename t_CMember>
	constexpr t_CMember const &TCSqlColumn<t_CRow, t_CMember>::f_Value(t_CRow const &_Row) const
	{
		return _Row.*m_pMember;
	}

	template <typename t_CRow, typename t_CMember>
	constexpr bool TCSqlColumn<t_CRow, t_CMember>::f_IsNullable() const
	{
		return fg_IsSet(m_Options.m_Flags, ESqlColumnFlag::mc_Nullable) || NStorage::cIsOptional<CMember>;
	}

	template <typename ...tp_CColumns>
	template <typename tf_FFunctor>
	constexpr decltype(auto) TCSqlColumns<tp_CColumns...>::f_ForEachColumn(tf_FFunctor &&_fFunctor) const
	{
		return NStorage::fg_TupleApply
			(
				[&](auto const &...p_Columns) -> decltype(auto)
				{
					return fg_Forward<tf_FFunctor>(_fFunctor)(p_Columns...);
				}
				, m_Columns
			)
		;
	}

	template <umint t_nColumns>
	constexpr TCSqlIndex<t_nColumns>::TCSqlIndex(NStr::CStr const *_pName, ESqlIndexFlag _Flags, NStr::CStr const *const (&_ColumnNames)[t_nColumns])
		: m_pName(_pName)
		, m_Flags(_Flags)
	{
		for (umint i = 0; i < t_nColumns; ++i)
			m_ColumnNames[i] = _ColumnNames[i];
	}

	template <umint t_nColumns>
	constexpr NStr::CStr const &TCSqlIndex<t_nColumns>::f_Name() const
	{
		return *m_pName;
	}

	template <umint t_nColumns>
	constexpr bool TCSqlIndex<t_nColumns>::f_IsUnique() const
	{
		return fg_IsSet(m_Flags, ESqlIndexFlag::mc_Unique);
	}

	template <umint t_nColumns>
	template <typename tf_FFunctor>
	constexpr decltype(auto) TCSqlIndex<t_nColumns>::f_ForEachColumnName(tf_FFunctor &&_fFunctor) const
	{
		return [&]<umint ...tfp_Indices>(NMeta::TCIndices<tfp_Indices...>) -> decltype(auto)
			{
				return fg_Forward<tf_FFunctor>(_fFunctor)(*m_ColumnNames[tfp_Indices]...);
			}
			(NMeta::TCConsecutiveIndices<t_nColumns>())
		;
	}

	template <umint t_nColumns>
	CSqlIndexDescription TCSqlIndex<t_nColumns>::f_Describe() const
	{
		CSqlIndexDescription Description;
		Description.m_pIndex = this;
		Description.m_pName = m_pName;
		Description.m_Flags = m_Flags;

		f_ForEachColumnName
			(
				[&](auto const &...p_ColumnNames)
				{
					(
						[&]
						{
							Description.m_Columns.f_InsertLast(&p_ColumnNames);
						}
						()
						, ...
					);
					}
				)
		;

		return Description;
	}

	template <auto ...tp_pMembers>
	constexpr TCSqlMemberIndex<tp_pMembers...>::TCSqlMemberIndex(NStr::CStr const *_pName, ESqlIndexFlag _Flags)
		: m_pName(_pName)
		, m_Flags(_Flags)
	{
	}

	template <auto ...tp_pMembers>
	constexpr NStr::CStr const &TCSqlMemberIndex<tp_pMembers...>::f_Name() const
	{
		return *m_pName;
	}

	template <auto ...tp_pMembers>
	constexpr bool TCSqlMemberIndex<tp_pMembers...>::f_IsUnique() const
	{
		return fg_IsSet(m_Flags, ESqlIndexFlag::mc_Unique);
	}

	template <auto ...tp_pMembers>
	template <typename tf_CColumns>
	CSqlIndexDescription TCSqlMemberIndex<tp_pMembers...>::f_Describe(tf_CColumns const &_Columns) const
	{
		CSqlIndexDescription Description;
		Description.m_pName = m_pName;
		Description.m_Flags = m_Flags;

		(
			[&]
			{
				Description.m_Columns.f_InsertLast(fg_SqlColumnNamePointerForMemberChecked(_Columns, tp_pMembers));
			}
			()
			, ...
		);

		return Description;
	}

	template <typename ...tp_CIndexes>
	template <typename tf_FFunctor>
	constexpr decltype(auto) TCSqlIndexes<tp_CIndexes...>::f_ForEachIndex(tf_FFunctor &&_fFunctor) const
	{
		return NStorage::fg_TupleApply
			(
				[&](auto const &...p_Indexes) -> decltype(auto)
				{
					return fg_Forward<tf_FFunctor>(_fFunctor)(p_Indexes...);
				}
				, m_Indexes
			)
		;
	}

	template <umint t_nColumns>
	template <typename tf_FFunctor>
	constexpr decltype(auto) TCSqlColumnNames<t_nColumns>::f_ForEachColumnName(tf_FFunctor &&_fFunctor) const
	{
		return [&]<umint ...tfp_Indices>(NMeta::TCIndices<tfp_Indices...>) -> decltype(auto)
			{
				return fg_Forward<tf_FFunctor>(_fFunctor)(*m_ColumnNames[tfp_Indices]...);
			}
			(NMeta::TCConsecutiveIndices<t_nColumns>())
		;
	}

	template <umint t_nColumns>
	constexpr NStr::CStr const &TCSqlForeignKeyReference<t_nColumns>::f_TableName() const
	{
		return *m_pTableName;
	}

	template <umint t_nColumns>
	template <typename tf_FFunctor>
	constexpr decltype(auto) TCSqlForeignKeyReference<t_nColumns>::f_ForEachColumnName(tf_FFunctor &&_fFunctor) const
	{
		return [&]<umint ...tfp_Indices>(NMeta::TCIndices<tfp_Indices...>) -> decltype(auto)
			{
				return fg_Forward<tf_FFunctor>(_fFunctor)(*m_ColumnNames[tfp_Indices]...);
			}
			(NMeta::TCConsecutiveIndices<t_nColumns>())
		;
	}

	template <ESqlConstraintType t_Type, umint t_nColumns>
	constexpr TCSqlColumnConstraint<t_Type, t_nColumns>::TCSqlColumnConstraint(NStr::CStr const *_pName, NStr::CStr const *const (&_ColumnNames)[t_nColumns])
		: m_pName(_pName)
	{
		for (umint i = 0; i < t_nColumns; ++i)
			m_ColumnNames[i] = _ColumnNames[i];
	}

	template <ESqlConstraintType t_Type, umint t_nColumns>
	constexpr NStr::CStr const &TCSqlColumnConstraint<t_Type, t_nColumns>::f_Name() const
	{
		return *m_pName;
	}

	template <ESqlConstraintType t_Type, umint t_nColumns>
	template <typename tf_FFunctor>
	constexpr decltype(auto) TCSqlColumnConstraint<t_Type, t_nColumns>::f_ForEachColumnName(tf_FFunctor &&_fFunctor) const
	{
		return [&]<umint ...tfp_Indices>(NMeta::TCIndices<tfp_Indices...>) -> decltype(auto)
			{
				return fg_Forward<tf_FFunctor>(_fFunctor)(*m_ColumnNames[tfp_Indices]...);
			}
			(NMeta::TCConsecutiveIndices<t_nColumns>())
		;
	}

	template <ESqlConstraintType t_Type, umint t_nColumns>
	CSqlConstraintDescription TCSqlColumnConstraint<t_Type, t_nColumns>::f_Describe() const
	{
		CSqlConstraintDescription Description;
		Description.m_pConstraint = this;
		Description.m_pName = m_pName;
		Description.m_Type = t_Type;

		f_ForEachColumnName
			(
				[&](auto const &...p_ColumnNames)
				{
					(
						[&]
						{
							Description.m_Columns.f_InsertLast(&p_ColumnNames);
						}
						()
						, ...
					);
				}
			)
		;

		return Description;
	}

	template <ESqlConstraintType t_Type, auto ...tp_pMembers>
	constexpr TCSqlMemberColumnConstraint<t_Type, tp_pMembers...>::TCSqlMemberColumnConstraint(NStr::CStr const *_pName)
		: m_pName(_pName)
	{
	}

	template <ESqlConstraintType t_Type, auto ...tp_pMembers>
	constexpr NStr::CStr const &TCSqlMemberColumnConstraint<t_Type, tp_pMembers...>::f_Name() const
	{
		return *m_pName;
	}

	template <ESqlConstraintType t_Type, auto ...tp_pMembers>
	template <typename tf_CColumns>
	CSqlConstraintDescription TCSqlMemberColumnConstraint<t_Type, tp_pMembers...>::f_Describe(tf_CColumns const &_Columns) const
	{
		CSqlConstraintDescription Description;
		Description.m_pName = m_pName;
		Description.m_Type = t_Type;

		(
			[&]
			{
				Description.m_Columns.f_InsertLast(fg_SqlColumnNamePointerForMemberChecked(_Columns, tp_pMembers));
			}
			()
			, ...
		);

		return Description;
	}

	template <ESqlConstraintType t_Type, auto ...tp_pMembers>
	template <typename tf_CMember>
	constexpr bool TCSqlMemberColumnConstraint<t_Type, tp_pMembers...>::f_MemberIsInConstraint(tf_CMember _pMember) const
	{
		bool bFound = false;
		(
			[&]
			{
				if constexpr (requires { _pMember == tp_pMembers; })
					bFound = bFound || (_pMember == tp_pMembers);
			}
			()
			, ...
		);

		return bFound;
	}

	constexpr CSqlCheckConstraint::CSqlCheckConstraint(NStr::CStr const *_pName, NStr::CStr const *_pCheckSql)
		: m_pName(_pName)
		, m_pCheckSql(_pCheckSql)
	{
	}

	constexpr NStr::CStr const &CSqlCheckConstraint::f_Name() const
	{
		return *m_pName;
	}

	constexpr NStr::CStr const &CSqlCheckConstraint::f_CheckSql() const
	{
		return *m_pCheckSql;
	}

	inline CSqlConstraintDescription CSqlCheckConstraint::f_Describe() const
	{
		return
			{
				.m_pConstraint = this
				, .m_pName = m_pName
				, .m_Type = ESqlConstraintType::mc_Check
				, .m_pCheckSql = m_pCheckSql
			}
		;
	}

	template <umint t_nColumns, umint t_nReferencedColumns>
	constexpr TCSqlForeignKeyConstraint<t_nColumns, t_nReferencedColumns>::TCSqlForeignKeyConstraint
		(
			NStr::CStr const *_pName
			, TCSqlColumnNames<t_nColumns> _Columns
			, TCSqlForeignKeyReference<t_nReferencedColumns> _Reference
			, ESqlForeignKeyAction _OnDelete
			, ESqlForeignKeyAction _OnUpdate
		)
		: m_pName(_pName)
		, m_Columns(_Columns)
		, m_Reference(_Reference)
		, m_OnDelete(_OnDelete)
		, m_OnUpdate(_OnUpdate)
	{
	}

	template <umint t_nColumns, umint t_nReferencedColumns>
	constexpr NStr::CStr const &TCSqlForeignKeyConstraint<t_nColumns, t_nReferencedColumns>::f_Name() const
	{
		return *m_pName;
	}

	template <umint t_nColumns, umint t_nReferencedColumns>
	constexpr NStr::CStr const &TCSqlForeignKeyConstraint<t_nColumns, t_nReferencedColumns>::f_ReferencedTable() const
	{
		return m_Reference.f_TableName();
	}

	template <umint t_nColumns, umint t_nReferencedColumns>
	template <typename tf_FFunctor>
	constexpr decltype(auto) TCSqlForeignKeyConstraint<t_nColumns, t_nReferencedColumns>::f_ForEachColumnName(tf_FFunctor &&_fFunctor) const
	{
		return m_Columns.f_ForEachColumnName(fg_Forward<tf_FFunctor>(_fFunctor));
	}

	template <umint t_nColumns, umint t_nReferencedColumns>
	template <typename tf_FFunctor>
	constexpr decltype(auto) TCSqlForeignKeyConstraint<t_nColumns, t_nReferencedColumns>::f_ForEachReferencedColumnName(tf_FFunctor &&_fFunctor) const
	{
		return m_Reference.f_ForEachColumnName(fg_Forward<tf_FFunctor>(_fFunctor));
	}

	template <umint t_nColumns, umint t_nReferencedColumns>
	CSqlConstraintDescription TCSqlForeignKeyConstraint<t_nColumns, t_nReferencedColumns>::f_Describe() const
	{
		CSqlConstraintDescription Description;
		Description.m_pConstraint = this;
		Description.m_pName = m_pName;
		Description.m_Type = ESqlConstraintType::mc_ForeignKey;
		Description.m_pReferencedTable = m_Reference.m_pTableName;
		Description.m_OnDelete = m_OnDelete;
		Description.m_OnUpdate = m_OnUpdate;

		f_ForEachColumnName
			(
				[&](auto const &...p_ColumnNames)
				{
					(
						[&]
						{
							Description.m_Columns.f_InsertLast(&p_ColumnNames);
						}
						()
						, ...
					);
				}
			)
		;

		f_ForEachReferencedColumnName
			(
				[&](auto const &...p_ColumnNames)
				{
					(
						[&]
						{
							Description.m_ReferencedColumns.f_InsertLast(&p_ColumnNames);
						}
						()
						, ...
					);
				}
			)
		;

		return Description;
	}

	template <umint t_nReferencedColumns, auto ...tp_pMembers>
	constexpr TCSqlMemberForeignKeyConstraint<t_nReferencedColumns, tp_pMembers...>::TCSqlMemberForeignKeyConstraint
		(
			NStr::CStr const *_pName
			, TCSqlForeignKeyReference<t_nReferencedColumns> _Reference
			, ESqlForeignKeyAction _OnDelete
			, ESqlForeignKeyAction _OnUpdate
		)
		: m_pName(_pName)
		, m_Reference(_Reference)
		, m_OnDelete(_OnDelete)
		, m_OnUpdate(_OnUpdate)
	{
	}

	template <umint t_nReferencedColumns, auto ...tp_pMembers>
	constexpr NStr::CStr const &TCSqlMemberForeignKeyConstraint<t_nReferencedColumns, tp_pMembers...>::f_Name() const
	{
		return *m_pName;
	}

	template <umint t_nReferencedColumns, auto ...tp_pMembers>
	constexpr NStr::CStr const &TCSqlMemberForeignKeyConstraint<t_nReferencedColumns, tp_pMembers...>::f_ReferencedTable() const
	{
		return m_Reference.f_TableName();
	}

	template <umint t_nReferencedColumns, auto ...tp_pMembers>
	template <typename tf_FFunctor>
	constexpr decltype(auto) TCSqlMemberForeignKeyConstraint<t_nReferencedColumns, tp_pMembers...>::f_ForEachReferencedColumnName(tf_FFunctor &&_fFunctor) const
	{
		return m_Reference.f_ForEachColumnName(fg_Forward<tf_FFunctor>(_fFunctor));
	}

	template <umint t_nReferencedColumns, auto ...tp_pMembers>
	template <typename tf_CColumns>
	CSqlConstraintDescription TCSqlMemberForeignKeyConstraint<t_nReferencedColumns, tp_pMembers...>::f_Describe(tf_CColumns const &_Columns) const
	{
		CSqlConstraintDescription Description;
		Description.m_pName = m_pName;
		Description.m_Type = ESqlConstraintType::mc_ForeignKey;
		Description.m_pReferencedTable = m_Reference.m_pTableName;
		Description.m_OnDelete = m_OnDelete;
		Description.m_OnUpdate = m_OnUpdate;

		(
			[&]
			{
				Description.m_Columns.f_InsertLast(fg_SqlColumnNamePointerForMemberChecked(_Columns, tp_pMembers));
			}
			()
			, ...
		);

		f_ForEachReferencedColumnName
			(
				[&](auto const &...p_ColumnNames)
				{
					(
						[&]
						{
							Description.m_ReferencedColumns.f_InsertLast(&p_ColumnNames);
						}
						()
						, ...
					);
				}
			)
		;

		return Description;
	}

	template <typename ...tp_CConstraints>
	template <typename tf_FFunctor>
	constexpr decltype(auto) TCSqlConstraints<tp_CConstraints...>::f_ForEachConstraint(tf_FFunctor &&_fFunctor) const
	{
		return NStorage::fg_TupleApply
			(
				[&](auto const &...p_Constraints) -> decltype(auto)
				{
					return fg_Forward<tf_FFunctor>(_fFunctor)(p_Constraints...);
				}
				, m_Constraints
			)
		;
	}

	template <typename t_CRow, typename t_CColumns, typename t_CIndexes, typename t_CConstraints>
	constexpr TCSqlTable<t_CRow, t_CColumns, t_CIndexes, t_CConstraints>::TCSqlTable(NStr::CStr const *_pName, CColumns _Columns, CIndexes _Indexes, CConstraints _Constraints)
		: m_pName(_pName)
		, m_Columns(_Columns)
		, m_Indexes(_Indexes)
		, m_Constraints(_Constraints)
	{
	}

	template <typename t_CRow, typename t_CColumns, typename t_CIndexes, typename t_CConstraints>
	constexpr NStr::CStr const &TCSqlTable<t_CRow, t_CColumns, t_CIndexes, t_CConstraints>::f_Name() const
	{
		return *m_pName;
	}

	template <typename t_CRow, typename t_CColumns, typename t_CIndexes, typename t_CConstraints>
	CSqlTableDescription TCSqlTable<t_CRow, t_CColumns, t_CIndexes, t_CConstraints>::f_Describe() const
	{
		CSqlTableDescription Description;
		Description.m_pTable = this;
		Description.m_pName = m_pName;
		Description.m_nColumns = mc_nColumns;
		Description.m_nIndexes = mc_nIndexes;
		Description.m_nConstraints = mc_nConstraints;
		Description.m_Columns = f_DescribeColumns();
		Description.m_Indexes = f_DescribeIndexes();
		Description.m_Constraints = f_DescribeConstraints();

		return Description;
	}

	template <typename t_CRow, typename t_CColumns, typename t_CIndexes, typename t_CConstraints>
	template <typename tf_FFunctor>
	constexpr decltype(auto) TCSqlTable<t_CRow, t_CColumns, t_CIndexes, t_CConstraints>::f_ForEachColumn(tf_FFunctor &&_fFunctor) const
	{
		return m_Columns.f_ForEachColumn(fg_Forward<tf_FFunctor>(_fFunctor));
	}

	template <typename t_CRow, typename t_CColumns, typename t_CIndexes, typename t_CConstraints>
	template <typename tf_FFunctor>
	constexpr decltype(auto) TCSqlTable<t_CRow, t_CColumns, t_CIndexes, t_CConstraints>::f_ForEachIndex(tf_FFunctor &&_fFunctor) const
	{
		return m_Indexes.f_ForEachIndex(fg_Forward<tf_FFunctor>(_fFunctor));
	}

	template <typename t_CRow, typename t_CColumns, typename t_CIndexes, typename t_CConstraints>
	template <typename tf_FFunctor>
	constexpr decltype(auto) TCSqlTable<t_CRow, t_CColumns, t_CIndexes, t_CConstraints>::f_ForEachConstraint(tf_FFunctor &&_fFunctor) const
	{
		return m_Constraints.f_ForEachConstraint(fg_Forward<tf_FFunctor>(_fFunctor));
	}

	template <typename t_CRow, typename t_CColumns, typename t_CIndexes, typename t_CConstraints>
	NContainer::TCVector<CSqlColumnDescription> TCSqlTable<t_CRow, t_CColumns, t_CIndexes, t_CConstraints>::f_DescribeColumns() const
	{
		NContainer::TCVector<CSqlColumnDescription> Descriptions;
		f_ForEachColumn
			(
				[&](auto const &...p_Columns)
				{
					(
						[&]
						{
							Descriptions.f_InsertLast(p_Columns.f_Describe());
						}
						()
						, ...
					);
				}
			)
		;

		return Descriptions;
	}

	template <typename t_CTable, auto t_pIDMember, auto t_pVersionMember, auto ...tp_pSaveMembers>
	constexpr TCSqlRepository<t_CTable, t_pIDMember, t_pVersionMember, tp_pSaveMembers...>::TCSqlRepository(CTable const &_Table)
		: m_Table(_Table)
	{
	}

	template <typename t_CRow, typename t_CColumns, typename t_CIndexes, typename t_CConstraints>
	NContainer::TCVector<CSqlIndexDescription> TCSqlTable<t_CRow, t_CColumns, t_CIndexes, t_CConstraints>::f_DescribeIndexes() const
	{
		NContainer::TCVector<CSqlIndexDescription> Descriptions;
		f_ForEachIndex
			(
				[&](auto const &...p_Indexes)
				{
					(
						[&]
						{
							if constexpr (requires { p_Indexes.f_Describe(m_Columns); })
								Descriptions.f_InsertLast(p_Indexes.f_Describe(m_Columns));
							else
								Descriptions.f_InsertLast(p_Indexes.f_Describe());
						}
						()
						, ...
					);
				}
			)
		;

		return Descriptions;
	}

	template <typename t_CRow, typename t_CColumns, typename t_CIndexes, typename t_CConstraints>
	NContainer::TCVector<CSqlConstraintDescription> TCSqlTable<t_CRow, t_CColumns, t_CIndexes, t_CConstraints>::f_DescribeConstraints() const
	{
		NContainer::TCVector<CSqlConstraintDescription> Descriptions;
		f_ForEachConstraint
			(
				[&](auto const &...p_Constraints)
				{
					(
						[&]
						{
							if constexpr (requires { p_Constraints.f_Describe(m_Columns); })
								Descriptions.f_InsertLast(p_Constraints.f_Describe(m_Columns));
							else
								Descriptions.f_InsertLast(p_Constraints.f_Describe());
						}
						()
						, ...
					);
				}
			)
		;

		return Descriptions;
	}

	constexpr ESqlDialect fg_SqlColumnTypeSupportedDialects(ESqlColumnType _Type)
	{
		constexpr ESqlDialect c_Standard1999AndLater = ESqlDialect::mc_SQL1999 | ESqlDialect::mc_SQL2011 | ESqlDialect::mc_SQL2016 | ESqlDialect::mc_SQL2023;
		constexpr ESqlDialect c_Standard2011AndLater = ESqlDialect::mc_SQL2011 | ESqlDialect::mc_SQL2016 | ESqlDialect::mc_SQL2023;
		constexpr ESqlDialect c_Standard2016AndLater = ESqlDialect::mc_SQL2016 | ESqlDialect::mc_SQL2023;

		switch (_Type)
		{
		case ESqlColumnType::mc_Invalid:
			return ESqlDialect::mc_None;
		case ESqlColumnType::mc_Integer8:
		case ESqlColumnType::mc_Integer16:
		case ESqlColumnType::mc_Integer32:
		case ESqlColumnType::mc_Integer64:
		case ESqlColumnType::mc_UnsignedInteger8:
		case ESqlColumnType::mc_UnsignedInteger16:
		case ESqlColumnType::mc_UnsignedInteger32:
		case ESqlColumnType::mc_UnsignedInteger64:
		case ESqlColumnType::mc_Float32:
		case ESqlColumnType::mc_Float64:
		case ESqlColumnType::mc_Text:
		case ESqlColumnType::mc_Blob:
		case ESqlColumnType::mc_Boolean:
		case ESqlColumnType::mc_Time:
			return c_Standard1999AndLater | ESqlDialect::mc_SQLite | ESqlDialect::mc_Postgres | ESqlDialect::mc_Dynamic;
		case ESqlColumnType::mc_UUID:
			return ESqlDialect::mc_Postgres | ESqlDialect::mc_Dynamic;
		case ESqlColumnType::mc_Date:
		case ESqlColumnType::mc_TimeOfDay:
		case ESqlColumnType::mc_Timestamp:
		case ESqlColumnType::mc_TimestampTz:
		case ESqlColumnType::mc_Interval:
			return c_Standard2011AndLater | ESqlDialect::mc_Postgres | ESqlDialect::mc_Dynamic;
		case ESqlColumnType::mc_Json:
			return c_Standard2016AndLater | ESqlDialect::mc_Postgres | ESqlDialect::mc_Dynamic;
		case ESqlColumnType::mc_Jsonb:
			return ESqlDialect::mc_Postgres | ESqlDialect::mc_Dynamic;
		case ESqlColumnType::mc_Array_Integer16:
		case ESqlColumnType::mc_Array_Integer32:
		case ESqlColumnType::mc_Array_Integer64:
		case ESqlColumnType::mc_Array_Float32:
		case ESqlColumnType::mc_Array_Float64:
		case ESqlColumnType::mc_Array_Text:
		case ESqlColumnType::mc_Array_Boolean:
		case ESqlColumnType::mc_Array_Bytes:
			return c_Standard1999AndLater | ESqlDialect::mc_Postgres | ESqlDialect::mc_Dynamic;
		case ESqlColumnType::mc_Array_Date:
		case ESqlColumnType::mc_Array_TimeOfDay:
		case ESqlColumnType::mc_Array_Timestamp:
		case ESqlColumnType::mc_Array_TimestampTz:
		case ESqlColumnType::mc_Array_Interval:
			return c_Standard2011AndLater | ESqlDialect::mc_Postgres | ESqlDialect::mc_Dynamic;
		case ESqlColumnType::mc_Array_Json:
			return c_Standard2016AndLater | ESqlDialect::mc_Postgres | ESqlDialect::mc_Dynamic;
		case ESqlColumnType::mc_Array_UUID:
		case ESqlColumnType::mc_Array_Jsonb:
			return ESqlDialect::mc_Postgres | ESqlDialect::mc_Dynamic;
		}

		DMibNeverGetHere;
		return ESqlDialect::mc_None;
	}

	constexpr bool fg_SqlDialectsSupportColumnType(ESqlDialect _Dialects, ESqlColumnType _Type)
	{
		return fg_IsSet(fg_SqlColumnTypeSupportedDialects(_Type), _Dialects);
	}

	constexpr ESqlDialect fg_SqlValueTypeSupportedDialects(ESqlValueType _Type)
	{
		constexpr ESqlDialect c_Standard1999AndLater = ESqlDialect::mc_SQL1999 | ESqlDialect::mc_SQL2011 | ESqlDialect::mc_SQL2016 | ESqlDialect::mc_SQL2023;
		constexpr ESqlDialect c_Standard2011AndLater = ESqlDialect::mc_SQL2011 | ESqlDialect::mc_SQL2016 | ESqlDialect::mc_SQL2023;
		constexpr ESqlDialect c_Standard2016AndLater = ESqlDialect::mc_SQL2016 | ESqlDialect::mc_SQL2023;
		constexpr ESqlDialect c_AllStandardsAndBackends = c_Standard1999AndLater | ESqlDialect::mc_SQLite | ESqlDialect::mc_Postgres | ESqlDialect::mc_Dynamic;

		switch (_Type)
		{
		case ESqlValueType::mc_Null:
			return c_AllStandardsAndBackends;
		case ESqlValueType::mc_Integer8:
		case ESqlValueType::mc_Integer16:
		case ESqlValueType::mc_Integer32:
		case ESqlValueType::mc_Integer64:
		case ESqlValueType::mc_UnsignedInteger8:
		case ESqlValueType::mc_UnsignedInteger16:
		case ESqlValueType::mc_UnsignedInteger32:
		case ESqlValueType::mc_UnsignedInteger64:
		case ESqlValueType::mc_Float32:
		case ESqlValueType::mc_Float64:
		case ESqlValueType::mc_Text:
		case ESqlValueType::mc_Blob:
		case ESqlValueType::mc_Boolean:
		case ESqlValueType::mc_Time:
			return c_AllStandardsAndBackends;
		case ESqlValueType::mc_UUID:
			return ESqlDialect::mc_Postgres | ESqlDialect::mc_Dynamic;
		case ESqlValueType::mc_Date:
		case ESqlValueType::mc_TimeOfDay:
		case ESqlValueType::mc_Timestamp:
		case ESqlValueType::mc_TimestampTz:
		case ESqlValueType::mc_Interval:
			return c_Standard2011AndLater | ESqlDialect::mc_Postgres | ESqlDialect::mc_Dynamic;
		case ESqlValueType::mc_Json:
			return c_Standard2016AndLater | ESqlDialect::mc_Postgres | ESqlDialect::mc_Dynamic;
		case ESqlValueType::mc_Jsonb:
			return ESqlDialect::mc_Postgres | ESqlDialect::mc_Dynamic;
		case ESqlValueType::mc_UnrecognizedBackend:
			return ESqlDialect::mc_Postgres | ESqlDialect::mc_Dynamic;
		case ESqlValueType::mc_Array_Integer16:
		case ESqlValueType::mc_Array_Integer32:
		case ESqlValueType::mc_Array_Integer64:
		case ESqlValueType::mc_Array_Float32:
		case ESqlValueType::mc_Array_Float64:
		case ESqlValueType::mc_Array_Text:
		case ESqlValueType::mc_Array_Boolean:
		case ESqlValueType::mc_Array_Bytes:
			return c_Standard1999AndLater | ESqlDialect::mc_Postgres | ESqlDialect::mc_Dynamic;
		case ESqlValueType::mc_Array_Date:
		case ESqlValueType::mc_Array_TimeOfDay:
		case ESqlValueType::mc_Array_Timestamp:
		case ESqlValueType::mc_Array_TimestampTz:
		case ESqlValueType::mc_Array_Interval:
			return c_Standard2011AndLater | ESqlDialect::mc_Postgres | ESqlDialect::mc_Dynamic;
		case ESqlValueType::mc_Array_Json:
			return c_Standard2016AndLater | ESqlDialect::mc_Postgres | ESqlDialect::mc_Dynamic;
		case ESqlValueType::mc_Array_UUID:
		case ESqlValueType::mc_Array_Jsonb:
			return ESqlDialect::mc_Postgres | ESqlDialect::mc_Dynamic;
		}

		DMibNeverGetHere;
		return ESqlDialect::mc_None;
	}

	constexpr bool fg_SqlDialectsSupportValueType(ESqlDialect _Dialects, ESqlValueType _Type)
	{
		return fg_IsSet(fg_SqlValueTypeSupportedDialects(_Type), _Dialects);
	}

	constexpr bool fg_SqlColumnTypeIsInteger(ESqlColumnType _Type)
	{
		switch (_Type)
		{
		case ESqlColumnType::mc_Integer8:
		case ESqlColumnType::mc_Integer16:
		case ESqlColumnType::mc_Integer32:
		case ESqlColumnType::mc_Integer64:
		case ESqlColumnType::mc_UnsignedInteger8:
		case ESqlColumnType::mc_UnsignedInteger16:
		case ESqlColumnType::mc_UnsignedInteger32:
		case ESqlColumnType::mc_UnsignedInteger64:
			return true;
		default:
			return false;
		}
	}

	template <NStr::TCStrConstData t_Prefix, NStr::TCStrConstData ...tp_Parts>
	consteval auto fg_SqlGeneratedNameData()
	{
		using CChar = typename decltype(t_Prefix)::CChar;
		constexpr umint c_nChars = decltype(t_Prefix)::mc_nChars + ((decltype(tp_Parts)::mc_nChars) + ... + 0);
		CChar Name[c_nChars] = {};
		umint iWrite = 0;

		for (umint i = 0; i < decltype(t_Prefix)::mc_nChars - 1; ++i)
			Name[iWrite++] = t_Prefix.m_Data[i];

		(
			[&]
			{
				Name[iWrite++] = '_';
				for (umint i = 0; i < decltype(tp_Parts)::mc_nChars - 1; ++i)
					Name[iWrite++] = tp_Parts.m_Data[i];
			}
			()
			, ...
		);

		return NStr::TCStrConstData<c_nChars, CChar>(Name);
	}

	template <ESqlDialect t_Dialects, typename t_CColumn>
	constexpr bool fg_SqlColumnMatchesDialect()
	{
		return fg_SqlDialectsSupportColumnType(t_Dialects, t_CColumn::mc_Type);
	}

	template <ESqlDialect t_Dialects, typename ...tp_CColumns>
	struct TCSqlColumnsMatchDialect<t_Dialects, TCSqlColumns<tp_CColumns...>>
	{
		static constexpr bool mc_bValue = (fg_SqlColumnMatchesDialect<t_Dialects, tp_CColumns>() && ...);
	};

	template <ESqlDialect t_Dialects, typename t_CTable>
	constexpr bool fg_SqlTableMatchesDialect()
	{
		return TCSqlColumnsMatchDialect<t_Dialects, typename t_CTable::CColumns>::mc_bValue;
	}

	template <ESqlDialect t_Dialect, typename ...tp_CTables>
	constexpr TCSqlDatabase<t_Dialect, tp_CTables...>::TCSqlDatabase(NStr::CStr const *_pName, CTables _Tables)
		: m_pName(_pName)
		, m_Tables(_Tables)
	{
	}

	template <ESqlDialect t_Dialect, typename ...tp_CTables>
	constexpr NStr::CStr const &TCSqlDatabase<t_Dialect, tp_CTables...>::f_Name() const
	{
		return *m_pName;
	}

	template <ESqlDialect t_Dialect, typename ...tp_CTables>
	CSqlDatabaseDescription TCSqlDatabase<t_Dialect, tp_CTables...>::f_Describe() const
	{
		CSqlDatabaseDescription Description;
		Description.m_pDatabase = this;
		Description.m_pName = m_pName;
		Description.m_nTables = mc_nTables;
		Description.m_Tables = f_DescribeTables();

		return Description;
	}

	template <ESqlDialect t_Dialect, typename ...tp_CTables>
	template <typename tf_FFunctor>
	constexpr decltype(auto) TCSqlDatabase<t_Dialect, tp_CTables...>::f_ForEachTable(tf_FFunctor &&_fFunctor) const
	{
		return NStorage::fg_TupleApply
			(
				[&](auto const &...p_Tables) -> decltype(auto)
				{
					return fg_Forward<tf_FFunctor>(_fFunctor)(p_Tables...);
				}
				, m_Tables
			)
		;
	}

	template <ESqlDialect t_Dialect, typename ...tp_CTables>
	NContainer::TCVector<CSqlTableDescription> TCSqlDatabase<t_Dialect, tp_CTables...>::f_DescribeTables() const
	{
		NContainer::TCVector<CSqlTableDescription> Descriptions;
		f_ForEachTable
			(
				[&](tp_CTables const &...p_Tables)
				{
					(
						[&]
						{
							Descriptions.f_InsertLast(p_Tables.f_Describe());
						}
						()
						, ...
					);
				}
			)
		;

		return Descriptions;
	}

	template <typename t_CDatabase>
	constexpr TCSqlSchemaVersion<t_CDatabase>::TCSqlSchemaVersion(NStr::CStr const *_pID, CDatabase _Database)
		: m_pID(_pID)
		, m_Database(_Database)
	{
	}

	template <typename t_CDatabase>
	constexpr NStr::CStr const &TCSqlSchemaVersion<t_CDatabase>::f_ID() const
	{
		return *m_pID;
	}

	template <typename t_CDatabase>
	constexpr t_CDatabase const &TCSqlSchemaVersion<t_CDatabase>::f_Database() const
	{
		return m_Database;
	}

	template <typename t_CDatabase>
	CSqlSchemaVersionDescription TCSqlSchemaVersion<t_CDatabase>::f_Describe() const
	{
		CSqlDatabaseDescription DatabaseDescription = m_Database.f_Describe();
		NStr::CStr Checksum = fg_SqlSchemaVersionChecksum(*m_pID, DatabaseDescription);

		return
			{
				.m_pSchema = this
				, .m_pID = m_pID
				, .m_pDatabaseName = &m_Database.f_Name()
				, .m_Checksum = fg_Move(Checksum)
				, .m_nTables = t_CDatabase::mc_nTables
				, .m_Database = fg_Move(DatabaseDescription)
			}
		;
	}

	template <typename t_CSchemaVersion, typename ...tp_COperations>
	constexpr TCSqlSchemaMigration<t_CSchemaVersion, tp_COperations...>::TCSqlSchemaMigration(CSchemaVersion _SchemaVersion, COperations _Operations)
		: m_SchemaVersion(_SchemaVersion)
		, m_Operations(_Operations)
	{
	}

	template <typename t_CSchemaVersion, typename ...tp_COperations>
	template <typename tf_FFunctor>
	constexpr decltype(auto) TCSqlSchemaMigration<t_CSchemaVersion, tp_COperations...>::f_ForEachOperation(tf_FFunctor &&_fFunctor) const
	{
		return NStorage::fg_TupleApply
			(
				[&](auto const &...p_Operations) -> decltype(auto)
				{
					return fg_Forward<tf_FFunctor>(_fFunctor)(p_Operations...);
				}
				, m_Operations
			)
		;
	}

	template <typename t_CSchemaVersion, typename ...tp_COperations>
	CSqlSchemaVersionDescription TCSqlSchemaMigration<t_CSchemaVersion, tp_COperations...>::f_DescribeVersion() const
	{
		return m_SchemaVersion.f_Describe();
	}

	template <typename t_CSchemaVersion, typename ...tp_COperations>
	CSqlSchemaMigrationDescription TCSqlSchemaMigration<t_CSchemaVersion, tp_COperations...>::f_DescribeMigration(NStr::CStr const *_pFromVersionID) const
	{
		CSqlSchemaMigrationDescription Description;
		Description.m_pFromVersionID = _pFromVersionID;
		Description.m_pToVersionID = &m_SchemaVersion.f_ID();

		f_ForEachOperation
			(
				[&](tp_COperations const &...p_Operations)
				{
					(
						[&]
						{
							Description.m_Operations.f_InsertLast(p_Operations.f_Describe());
						}
						()
						, ...
					);
				}
			)
		;

		return Description;
	}

	template <typename ...tp_CMigrations>
	constexpr TCSqlSchemaMigrations<tp_CMigrations...>::TCSqlSchemaMigrations(CMigrations _Migrations)
		: m_Migrations(_Migrations)
	{
	}

	template <typename ...tp_CMigrations>
	template <typename tf_FFunctor>
	constexpr decltype(auto) TCSqlSchemaMigrations<tp_CMigrations...>::f_ForEachMigration(tf_FFunctor &&_fFunctor) const
	{
		return NStorage::fg_TupleApply
			(
				[&](auto const &...p_Migrations) -> decltype(auto)
				{
					return fg_Forward<tf_FFunctor>(_fFunctor)(p_Migrations...);
				}
				, m_Migrations
			)
		;
	}

	template <typename ...tp_CMigrations>
	template <typename tf_FFunctor>
	constexpr decltype(auto) TCSqlSchemaMigrations<tp_CMigrations...>::f_ForEachVersion(tf_FFunctor &&_fFunctor) const
	{
		return f_ForEachMigration
			(
				[&](tp_CMigrations const &...p_Migrations) -> decltype(auto)
				{
					return fg_Forward<tf_FFunctor>(_fFunctor)(p_Migrations.m_SchemaVersion...);
				}
			)
		;
	}

	template <typename ...tp_CMigrations>
	NContainer::TCVector<CSqlSchemaVersionDescription> TCSqlSchemaMigrations<tp_CMigrations...>::f_DescribeVersions() const
	{
		NContainer::TCVector<CSqlSchemaVersionDescription> Descriptions;
		f_ForEachMigration
			(
				[&](tp_CMigrations const &...p_Migrations)
				{
					(
						[&]
						{
							Descriptions.f_InsertLast(p_Migrations.f_DescribeVersion());
						}
						()
						, ...
					);
				}
			)
		;

		return Descriptions;
	}

	template <typename ...tp_CMigrations>
	NContainer::TCVector<CSqlSchemaMigrationDescription> TCSqlSchemaMigrations<tp_CMigrations...>::f_DescribeMigrations() const
	{
		NContainer::TCVector<CSqlSchemaMigrationDescription> Descriptions;
		NStr::CStr const *pPreviousVersionID = nullptr;
		f_ForEachMigration
			(
				[&](tp_CMigrations const &...p_Migrations)
				{
					(
						[&]
						{
							if (pPreviousVersionID)
								Descriptions.f_InsertLast(p_Migrations.f_DescribeMigration(pPreviousVersionID));
							pPreviousVersionID = &p_Migrations.m_SchemaVersion.f_ID();
						}
						()
						, ...
					);
				}
			)
		;

		return Descriptions;
	}

	template <typename t_CSchemaMigrations>
	constexpr TCSqlSchemaVersions<t_CSchemaMigrations>::TCSqlSchemaVersions(CSchemaMigrations _Migrations)
		: m_Migrations(_Migrations)
	{
	}

	template <typename t_CSchemaMigrations>
	template <typename tf_FFunctor>
	constexpr decltype(auto) TCSqlSchemaVersions<t_CSchemaMigrations>::f_ForEachVersion(tf_FFunctor &&_fFunctor) const
	{
		return m_Migrations.f_ForEachVersion(fg_Forward<tf_FFunctor>(_fFunctor));
	}

	template <typename t_CSchemaMigrations>
	CSqlSchemaVersionDescription TCSqlSchemaVersions<t_CSchemaMigrations>::f_Describe() const
	{
		CSqlSchemaVersionDescription Description;
		NContainer::TCVector<CSqlSchemaVersionDescription> Descriptions = m_Migrations.f_DescribeVersions();
		if (Descriptions.f_GetLen() != 0)
			Description = Descriptions[Descriptions.f_GetLen() - 1];

		return Description;
	}

	template <typename t_CSchemaMigrations>
	NContainer::TCVector<CSqlSchemaVersionDescription> TCSqlSchemaVersions<t_CSchemaMigrations>::f_DescribeVersions() const
	{
		return m_Migrations.f_DescribeVersions();
	}

	template <typename t_CSchemaMigrations>
	NContainer::TCVector<CSqlSchemaMigrationDescription> TCSqlSchemaVersions<t_CSchemaMigrations>::f_DescribeMigrations() const
	{
		return m_Migrations.f_DescribeMigrations();
	}

	consteval CSqlColumnOptions fg_SqlColumnOptions(ESqlColumnFlag _Flags, NStr::TCStrConst<NStr::CStr> _Comment)
	{
		CSqlColumnOptions Options;
		Options.m_Flags = _Flags;
		Options.m_pComment = &_Comment.m_Str;

		return Options;
	}

	template <umint tf_nNonPortableOptions>
	consteval CSqlColumnOptions fg_SqlColumnOptions
		(
			ESqlColumnFlag _Flags
			, NStr::TCStrConst<NStr::CStr> _Comment
			, CNonPortableColumnOptions const (&_NonPortableOptions)[tf_nNonPortableOptions]
		)
	{
		static_assert(tf_nNonPortableOptions <= CSqlColumnOptions::mc_MaxNonPortableOptions, "Too many non-portable SQL column option entries");

		CSqlColumnOptions Options;
		Options.m_Flags = _Flags;
		Options.m_pComment = &_Comment.m_Str;
		Options.m_nNonPortableOptions = tf_nNonPortableOptions;

		for (umint iOption = 0; iOption < tf_nNonPortableOptions; ++iOption)
			Options.m_NonPortableOptions[iOption] = _NonPortableOptions[iOption];

		return Options;
	}

	template <typename tf_CRow, typename tf_CMember>
	consteval auto fg_SqlColumn
		(
			NStr::TCStrConst<NStr::CStr> _Name
			, tf_CMember tf_CRow::*_pMember
			, ESqlColumnFlag _Flags
			, NStr::TCStrConst<NStr::CStr> _DefaultSql
		)
		requires (TCSqlTypeTraits<NStorage::TCOptionalType<tf_CMember>>::mc_bSupported)
	{
		CSqlColumnOptions Options = fg_SqlColumnOptions(_Flags);
		Options.m_pDefaultSql = &_DefaultSql.m_Str;

		return fg_SqlColumn(_Name, _pMember, Options);
	}

	template <typename tf_CRow, typename tf_CMember>
	consteval auto fg_SqlColumn
		(
			NStr::TCStrConst<NStr::CStr> _Name
			, tf_CMember tf_CRow::*_pMember
			, CSqlColumnOptions _Options
		)
		requires (TCSqlTypeTraits<NStorage::TCOptionalType<tf_CMember>>::mc_bSupported)
	{
		auto Column = TCSqlColumn<tf_CRow, tf_CMember>(&_Name.m_Str, _pMember, TCSqlColumnType<tf_CMember>::mc_Type, _Options);
		Column.m_NameHash = fg_SqlHashMixString(gc_SqlHashSeed, _Name);

		return Column;
	}

	template <typename tf_CRow, typename tf_CMember>
	consteval auto fg_SqlColumn
		(
			NStr::TCStrConst<NStr::CStr> _Name
			, tf_CMember tf_CRow::*_pMember
			, ESqlColumnType _Type
			, ESqlColumnFlag _Flags
			, NStr::TCStrConst<NStr::CStr> _DefaultSql
		)
		requires (TCSqlTypeTraits<NStorage::TCOptionalType<tf_CMember>>::mc_bSupported)
	{
		CSqlColumnOptions Options = fg_SqlColumnOptions(_Flags);
		Options.m_pDefaultSql = &_DefaultSql.m_Str;

		return fg_SqlColumn(_Name, _pMember, _Type, Options);
	}

	template <typename tf_CRow, typename tf_CMember>
	consteval auto fg_SqlColumn
		(
			NStr::TCStrConst<NStr::CStr> _Name
			, tf_CMember tf_CRow::*_pMember
			, ESqlColumnType _Type
			, CSqlColumnOptions _Options
		)
		requires (TCSqlTypeTraits<NStorage::TCOptionalType<tf_CMember>>::mc_bSupported)
	{
		auto Column = TCSqlColumn<tf_CRow, tf_CMember>(&_Name.m_Str, _pMember, _Type, _Options);
		Column.m_NameHash = fg_SqlHashMixString(gc_SqlHashSeed, _Name);

		return Column;
	}

	template <typename ...tfp_CColumns>
	consteval auto fg_SqlColumns(tfp_CColumns &&...p_Columns)
	{
		return TCSqlColumns<NTraits::TCRemoveReferenceAndQualifiers<tfp_CColumns>...>
			{
				.m_Columns = NStorage::fg_Tuple(fg_Forward<tfp_CColumns>(p_Columns)...)
			}
		;
	}

	template <typename ...tfp_CColumnNames>
	consteval auto fg_SqlIndex
		(
			NStr::TCStrConst<NStr::CStr> _Name
			, ESqlIndexFlag _Flags
			, tfp_CColumnNames &&...p_ColumnNames
		)
	{
		NStr::CStr const *ColumnNames[] = {&p_ColumnNames.m_Str...};

		return TCSqlIndex<sizeof...(tfp_CColumnNames)>(&_Name.m_Str, _Flags, ColumnNames);
	}

	template <typename ...tfp_CColumnNames>
	consteval auto fg_SqlIndex(NStr::TCStrConst<NStr::CStr> _Name, tfp_CColumnNames &&...p_ColumnNames)
	{
		return fg_SqlIndex(_Name, ESqlIndexFlag::mc_None, fg_Forward<tfp_CColumnNames>(p_ColumnNames)...);
	}

	template <auto ...tfp_pMembers>
	consteval auto fg_SqlIndex(NStr::TCStrConst<NStr::CStr> _Name, ESqlIndexFlag _Flags)
		requires (sizeof...(tfp_pMembers) > 0)
	{
		return TCSqlMemberIndex<tfp_pMembers...>(&_Name.m_Str, _Flags);
	}

	template <auto ...tfp_pMembers>
	consteval auto fg_SqlIndex(NStr::TCStrConst<NStr::CStr> _Name)
		requires (sizeof...(tfp_pMembers) > 0)
	{
		return fg_SqlIndex<tfp_pMembers...>(_Name, ESqlIndexFlag::mc_None);
	}

	template <typename ...tfp_CIndexes>
	consteval auto fg_SqlIndexes(tfp_CIndexes &&...p_Indexes)
	{
		return TCSqlIndexes<NTraits::TCRemoveReferenceAndQualifiers<tfp_CIndexes>...>
			{
				.m_Indexes = NStorage::fg_Tuple(fg_Forward<tfp_CIndexes>(p_Indexes)...)
			}
		;
	}

	template <typename ...tfp_CColumnNames>
	consteval auto fg_SqlColumnNames(tfp_CColumnNames &&...p_ColumnNames)
	{
		return TCSqlColumnNames<sizeof...(tfp_CColumnNames)>
			{
				.m_ColumnNames = {&p_ColumnNames.m_Str...}
			}
		;
	}

	template <typename tf_CColumns, typename tf_CRow, typename tf_CMember>
	constexpr NStr::CStr const *fg_SqlColumnNamePointerForMember(tf_CColumns const &_Columns, tf_CMember tf_CRow::*_pMember)
	{
		NStr::CStr const *pColumnName = nullptr;
		_Columns.f_ForEachColumn
			(
				[&](auto const &...p_Columns)
				{
					(
						[&]
						{
							if constexpr (requires { p_Columns.m_pMember == _pMember; })
							{
								if (p_Columns.m_pMember == _pMember)
									pColumnName = p_Columns.m_pName;
							}
						}
						()
						, ...
					);
				}
			)
		;

		return pColumnName;
	}

	template <typename tf_CColumns, typename tf_CRow, typename tf_CMember>
	constexpr NStr::CStr const *fg_SqlColumnNamePointerForMemberChecked(tf_CColumns const &_Columns, tf_CMember tf_CRow::*_pMember)
	{
		NStr::CStr const *pColumnName = fg_SqlColumnNamePointerForMember(_Columns, _pMember);
		if (!pColumnName)
			throw "SQL member pointer does not match any column in this column set";

		return pColumnName;
	}

	template <auto ...tp_pMembers>
	template <typename tf_CColumns, typename tf_FFunctor>
	constexpr decltype(auto) TCSqlMemberIndex<tp_pMembers...>::f_ForEachColumnName(tf_CColumns const &_Columns, tf_FFunctor &&_fFunctor) const
	{
		return fg_Forward<tf_FFunctor>(_fFunctor)(*fg_SqlColumnNamePointerForMemberChecked(_Columns, tp_pMembers)...);
	}

	template <ESqlConstraintType t_Type, auto ...tp_pMembers>
	template <typename tf_CColumns, typename tf_FFunctor>
	constexpr decltype(auto) TCSqlMemberColumnConstraint<t_Type, tp_pMembers...>::f_ForEachColumnName(tf_CColumns const &_Columns, tf_FFunctor &&_fFunctor) const
	{
		return fg_Forward<tf_FFunctor>(_fFunctor)(*fg_SqlColumnNamePointerForMemberChecked(_Columns, tp_pMembers)...);
	}

	template <umint t_nReferencedColumns, auto ...tp_pMembers>
	template <typename tf_CColumns, typename tf_FFunctor>
	constexpr decltype(auto) TCSqlMemberForeignKeyConstraint<t_nReferencedColumns, tp_pMembers...>::f_ForEachColumnName(tf_CColumns const &_Columns, tf_FFunctor &&_fFunctor) const
	{
		return fg_Forward<tf_FFunctor>(_fFunctor)(*fg_SqlColumnNamePointerForMemberChecked(_Columns, tp_pMembers)...);
	}

	template <typename tf_CColumn>
	constexpr ch8 const *fg_SqlValidateColumn(tf_CColumn const &_Column)
	{
		constexpr NStr::CStr const *c_pEmptyStr = &NStr::gc_Str<"">.m_Str;

		if (_Column.m_Type == ESqlColumnType::mc_Invalid)
			return "SQL column has invalid type";

		if (fg_IsSet(_Column.m_Options.m_Flags, ESqlColumnFlag::mc_PrimaryKey) && _Column.f_IsNullable())
			return "SQL primary-key column cannot be nullable";

		for (umint iOption = 0; iOption < _Column.m_Options.m_nNonPortableOptions; ++iOption)
		{
			CNonPortableColumnOptions const &Option = _Column.m_Options.m_NonPortableOptions[iOption];
			if (Option.m_pGeneratedSql != c_pEmptyStr && (_Column.m_Options.m_pDefaultSql != c_pEmptyStr || Option.m_pDefaultSql != c_pEmptyStr))
				return "SQL generated column cannot have a default expression";

			if (Option.m_pGeneratedSql != c_pEmptyStr && fg_IsSet(_Column.m_Options.m_Flags, ESqlColumnFlag::mc_PrimaryKey))
				return "SQL generated column cannot be a primary key";
		}

		if (fg_IsSet(_Column.m_Options.m_Flags, ESqlColumnFlag::mc_AutoIncrement))
		{
			if (!fg_IsSet(_Column.m_Options.m_Flags, ESqlColumnFlag::mc_PrimaryKey))
				return "SQL autoincrement column must be a primary key";

			if (!fg_SqlColumnTypeIsInteger(_Column.m_Type))
				return "SQL autoincrement column must use an integer type";

			if (_Column.m_Options.m_pDefaultSql != c_pEmptyStr)
				return "SQL autoincrement column cannot have a default expression";
		}

		return nullptr;
	}

	template <typename tf_CColumns>
	constexpr bool fg_SqlColumnsContainName(tf_CColumns const &_Columns, NStr::CStr const *_pName)
	{
		bool bContains = false;
		_Columns.f_ForEachColumn
			(
				[&](auto const &...p_Columns)
				{
					(
						[&]
						{
							bContains = bContains || p_Columns.m_pName == _pName;
						}
						()
						, ...
					);
				}
			)
		;

		return bContains;
	}

	template <typename tf_CColumns>
	constexpr ch8 const *fg_SqlValidateColumns(tf_CColumns const &_Columns)
	{
		ch8 const *pReturn = nullptr;
		_Columns.f_ForEachColumn
			(
				[&](auto const &...p_Columns)
				{
					(
						[&]
						{
							if (!pReturn)
								pReturn = fg_SqlValidateColumn(p_Columns);
						}
						()
						, ...
					);

					if (pReturn)
						return;

					(
						[&]
						{
							NStr::CStr const *pColumnName = p_Columns.m_pName;
							umint nMatchingColumns = 0;
							(
								[&]
								{
									if (p_Columns.m_pName == pColumnName)
										++nMatchingColumns;
								}
								()
								, ...
							);

							if (nMatchingColumns > 1)
								pReturn = "SQL table has duplicate column names";
						}
						()
						, ...
					);
				}
			)
		;

		return pReturn;
	}

	template <typename tf_CColumns, typename tf_CIndexes>
	constexpr ch8 const *fg_SqlValidateIndexes(tf_CColumns const &_Columns, tf_CIndexes const &_Indexes)
	{
		ch8 const *pError = nullptr;
		_Indexes.f_ForEachIndex
			(
				[&](auto const &...p_Indexes)
				{
					(
						[&]
						{
							if (pError)
								return;

							using CIndex = NTraits::TCRemoveReferenceAndQualifiers<decltype(p_Indexes)>;
							if constexpr (CIndex::mc_nColumns == 0)
							{
								pError = "SQL index must reference at least one column";
								return;
							}

							if constexpr (requires { p_Indexes.f_ForEachColumnName(_Columns, [](auto const &...) {}); })
							{
								p_Indexes.f_ForEachColumnName
									(
										_Columns
										, [&](auto const &...p_ColumnNames)
										{
											(
												[&]
												{
													if (!fg_SqlColumnsContainName(_Columns, &p_ColumnNames))
														pError = "SQL index references a missing column";
												}
												()
												, ...
											);
										}
									)
								;
							}
							else
							{
								p_Indexes.f_ForEachColumnName
									(
										[&](auto const &...p_ColumnNames)
										{
											(
												[&]
												{
													if (!fg_SqlColumnsContainName(_Columns, &p_ColumnNames))
														pError = "SQL index references a missing column";
												}
												()
												, ...
											);
										}
									)
								;
							}
						}
						()
						, ...
					);
				}
			)
		;

		return pError;
	}

	template <typename tf_CColumns, typename tf_CConstraints>
	constexpr ch8 const *fg_SqlValidateLocalConstraints(tf_CColumns const &_Columns, tf_CConstraints const &_Constraints)
	{
		ch8 const *pError = nullptr;
		_Constraints.f_ForEachConstraint
			(
				[&](auto const &...p_Constraints)
				{
					(
						[&]
						{
							if (pError)
								return;

							using CConstraint = NTraits::TCRemoveReferenceAndQualifiers<decltype(p_Constraints)>;
							if constexpr (CConstraint::mc_Type != ESqlConstraintType::mc_Check && CConstraint::mc_nColumns == 0)
							{
								pError = "SQL constraint must reference at least one column";
								return;
							}

							if constexpr (CConstraint::mc_Type != ESqlConstraintType::mc_Check)
							{
								if constexpr (requires { p_Constraints.f_ForEachColumnName(_Columns, [](auto const &...) {}); })
								{
									p_Constraints.f_ForEachColumnName
										(
											_Columns
											, [&](auto const &...p_ColumnNames)
											{
												(
													[&]
													{
														if (!fg_SqlColumnsContainName(_Columns, &p_ColumnNames))
															pError = "SQL constraint references a missing local column";
													}
													()
													, ...
												);
											}
										)
									;
								}
								else
								{
									p_Constraints.f_ForEachColumnName
										(
											[&](auto const &...p_ColumnNames)
											{
												(
													[&]
													{
														if (!fg_SqlColumnsContainName(_Columns, &p_ColumnNames))
															pError = "SQL constraint references a missing local column";
													}
													()
													, ...
												);
											}
										)
									;
								}
							}
						}
						()
						, ...
					);
				}
			)
		;

		return pError;
	}

	template <typename tf_CTable>
	constexpr ch8 const *fg_SqlValidateTable(tf_CTable const &_Table)
	{
		if (auto *pError = fg_SqlValidateColumns(_Table.m_Columns))
			return pError;

		if (auto *pError = fg_SqlValidateIndexes(_Table.m_Columns, _Table.m_Indexes))
			return pError;

		if (auto *pError = fg_SqlValidateLocalConstraints(_Table.m_Columns, _Table.m_Constraints))
			return pError;

		return nullptr;
	}

	template <typename tf_CDatabase>
	constexpr ch8 const *fg_SqlValidateReferencedColumn(tf_CDatabase const &_Database, NStr::CStr const *_pTableName, NStr::CStr const *_pColumnName)
	{
		bool bTableFound = false;
		bool bColumnFound = false;
		_Database.f_ForEachTable
			(
				[&](auto const &...p_Tables)
				{
					(
						[&]
						{
							if (p_Tables.m_pName == _pTableName)
							{
								bTableFound = true;
								bColumnFound = bColumnFound || fg_SqlColumnsContainName(p_Tables.m_Columns, _pColumnName);
							}
						}
						()
						, ...
					);
				}
			)
		;

		if (!bTableFound)
			return "SQL foreign key references a missing table";

		if (!bColumnFound)
			return "SQL foreign key references a missing column";

		return nullptr;
	}

	template <typename tf_CDatabase, typename tf_CConstraint>
	constexpr ch8 const *fg_SqlValidateForeignKeyConstraint(tf_CDatabase const &_Database, tf_CConstraint const &_Constraint)
	{
		ch8 const *pError = nullptr;
		using CConstraint = NTraits::TCRemoveReferenceAndQualifiers<tf_CConstraint>;
		if constexpr (CConstraint::mc_Type == ESqlConstraintType::mc_ForeignKey)
		{
			if constexpr (CConstraint::mc_nReferencedColumns != CConstraint::mc_nColumns)
				return "SQL foreign key column count does not match referenced column count";

			_Constraint.f_ForEachReferencedColumnName
				(
					[&](auto const &...p_ColumnNames)
					{
						(
							[&]
							{
								if (!pError)
									pError = fg_SqlValidateReferencedColumn(_Database, _Constraint.m_Reference.m_pTableName, &p_ColumnNames);
							}
							()
							, ...
						);
				}
			)
			;
		}

		return pError;
	}

	template <typename tf_CDatabase>
	constexpr umint fg_SqlCountDatabaseIndexName(tf_CDatabase const &_Database, NStr::CStr const *_pIndexName)
	{
		umint nMatching = 0;
		_Database.f_ForEachTable
			(
				[&](auto const &...p_Tables)
				{
					(
						[&]
						{
							p_Tables.f_ForEachIndex
								(
									[&](auto const &...p_Indexes)
									{
										(
											[&]
											{
												if (p_Indexes.m_pName == _pIndexName)
													++nMatching;
											}
											()
											, ...
										);
									}
								)
							;
						}
						()
						, ...
					);
				}
			)
		;
		return nMatching;
	}

	template <typename tf_CDatabase>
	constexpr ch8 const *fg_SqlValidateDatabase(tf_CDatabase const &_Database)
	{
		ch8 const *pError = nullptr;
		_Database.f_ForEachTable
			(
				[&](auto const &...p_Tables)
				{
					(
						[&]
						{
							auto const &Table = p_Tables;
							umint nMatchingTables = 0;
							(
								[&]
								{
									if (Table.m_pName == p_Tables.m_pName)
										++nMatchingTables;
								}
								()
								, ...
							);

							if (nMatchingTables > 1)
							{
								if (!pError)
								{
									pError = "SQL database has duplicate table names";
									return;
								}
							}

							if (!pError && (pError = fg_SqlValidateTable(Table)))
								return;

							Table.f_ForEachConstraint
								(
									[&](auto const &...p_Constraints)
									{
										(
											[&]
											{
												if (!pError)
													pError = fg_SqlValidateForeignKeyConstraint(_Database, p_Constraints);
											}
											()
											, ...
										);
									}
								)
							;

							// Index names are schema-wide on both backends, and both emit CREATE INDEX IF NOT EXISTS, so
							// a name reused across tables would silently skip the later index (leaving a declared, possibly
							// unique, index unenforced while the schema is still recorded as applied). Reject it here.
							Table.f_ForEachIndex
								(
									[&](auto const &...p_Indexes)
									{
										(
											[&]
											{
												if (!pError && fg_SqlCountDatabaseIndexName(_Database, p_Indexes.m_pName) > 1)
													pError = "SQL database has duplicate index names";
											}
											()
											, ...
										);
									}
								)
							;
						}
						()
						, ...
					);
				}
			)
		;
		return pError;
	}

	template <auto ...tfp_pMembers, typename tf_CTable>
	consteval auto fg_SqlColumnNames(tf_CTable const &_Table)
		requires (sizeof...(tfp_pMembers) > 0)
	{
		return TCSqlColumnNames<sizeof...(tfp_pMembers)>
			{
				.m_ColumnNames = {fg_SqlColumnNamePointerForMemberChecked(_Table, tfp_pMembers)...}
			}
		;
	}

	template <typename ...tfp_CColumnNames>
	consteval auto fg_SqlPrimaryKey(NStr::TCStrConst<NStr::CStr> _Name, tfp_CColumnNames &&...p_ColumnNames)
	{
		NStr::CStr const *ColumnNames[] = {&p_ColumnNames.m_Str...};

		return TCSqlColumnConstraint<ESqlConstraintType::mc_PrimaryKey, sizeof...(tfp_CColumnNames)>(&_Name.m_Str, ColumnNames);
	}

	template <auto ...tfp_pMembers>
	consteval auto fg_SqlPrimaryKey(NStr::TCStrConst<NStr::CStr> _Name)
		requires (sizeof...(tfp_pMembers) > 0)
	{
		return TCSqlMemberColumnConstraint<ESqlConstraintType::mc_PrimaryKey, tfp_pMembers...>(&_Name.m_Str);
	}

	template <typename ...tfp_CColumnNames>
	consteval auto fg_SqlUnique(NStr::TCStrConst<NStr::CStr> _Name, tfp_CColumnNames &&...p_ColumnNames)
	{
		NStr::CStr const *ColumnNames[] = {&p_ColumnNames.m_Str...};

		return TCSqlColumnConstraint<ESqlConstraintType::mc_Unique, sizeof...(tfp_CColumnNames)>(&_Name.m_Str, ColumnNames);
	}

	template <auto ...tfp_pMembers>
	consteval auto fg_SqlUnique(NStr::TCStrConst<NStr::CStr> _Name)
		requires (sizeof...(tfp_pMembers) > 0)
	{
		return TCSqlMemberColumnConstraint<ESqlConstraintType::mc_Unique, tfp_pMembers...>(&_Name.m_Str);
	}

	consteval auto fg_SqlCheck(NStr::TCStrConst<NStr::CStr> _Name, NStr::TCStrConst<NStr::CStr> _CheckSql)
	{
		return CSqlCheckConstraint(&_Name.m_Str, &_CheckSql.m_Str);
	}

	template <typename ...tfp_CColumnNames>
	consteval auto fg_SqlReferences(NStr::TCStrConst<NStr::CStr> _TableName, tfp_CColumnNames &&...p_ColumnNames)
	{
		return TCSqlForeignKeyReference<sizeof...(tfp_CColumnNames)>
			{
				.m_pTableName = &_TableName.m_Str
				, .m_ColumnNames = {&p_ColumnNames.m_Str...}
			}
		;
	}

	template <auto ...tfp_pMembers, typename tf_CTable>
	consteval auto fg_SqlReferences(tf_CTable const &_Table)
		requires (sizeof...(tfp_pMembers) > 0)
	{
		return TCSqlForeignKeyReference<sizeof...(tfp_pMembers)>
			{
				.m_pTableName = _Table.m_pName
				, .m_ColumnNames = {fg_SqlColumnNamePointerForMember(_Table, tfp_pMembers)...}
			}
		;
	}

	template <umint tf_nColumns, umint tf_nReferencedColumns>
	consteval auto fg_SqlForeignKey
		(
			NStr::TCStrConst<NStr::CStr> _Name
			, TCSqlColumnNames<tf_nColumns> _Columns
			, TCSqlForeignKeyReference<tf_nReferencedColumns> _Reference
			, ESqlForeignKeyAction _OnDelete
			, ESqlForeignKeyAction _OnUpdate
		)
	{
		return TCSqlForeignKeyConstraint<tf_nColumns, tf_nReferencedColumns>(&_Name.m_Str, _Columns, _Reference, _OnDelete, _OnUpdate);
	}

	template <auto ...tfp_pMembers, umint tf_nReferencedColumns>
	consteval auto fg_SqlForeignKey
		(
			NStr::TCStrConst<NStr::CStr> _Name
			, TCSqlForeignKeyReference<tf_nReferencedColumns> _Reference
			, ESqlForeignKeyAction _OnDelete
			, ESqlForeignKeyAction _OnUpdate
		)
		requires (sizeof...(tfp_pMembers) > 0)
	{
		return TCSqlMemberForeignKeyConstraint<tf_nReferencedColumns, tfp_pMembers...>(&_Name.m_Str, _Reference, _OnDelete, _OnUpdate);
	}

	template <typename ...tfp_CConstraints>
	consteval auto fg_SqlConstraints(tfp_CConstraints &&...p_Constraints)
	{
		return TCSqlConstraints<NTraits::TCRemoveReferenceAndQualifiers<tfp_CConstraints>...>
			{
				.m_Constraints = NStorage::fg_Tuple(fg_Forward<tfp_CConstraints>(p_Constraints)...)
			}
		;
	}

	template <typename tf_CRow, typename ...tfp_CColumns>
	consteval auto fg_SqlTable(NStr::TCStrConst<NStr::CStr> _Name, TCSqlColumns<tfp_CColumns...> _Columns)
	{
		return fg_SqlTable<tf_CRow>(_Name, _Columns, fg_SqlIndexes(), fg_SqlConstraints());
	}

	template <typename tf_CRow, typename ...tfp_CColumns, typename ...tfp_CIndexes>
	consteval auto fg_SqlTable(NStr::TCStrConst<NStr::CStr> _Name, TCSqlColumns<tfp_CColumns...> _Columns, TCSqlIndexes<tfp_CIndexes...> _Indexes)
	{
		return fg_SqlTable<tf_CRow>(_Name, _Columns, _Indexes, fg_SqlConstraints());
	}

	template <typename tf_CRow, typename ...tfp_CColumns, typename ...tfp_CIndexes, typename ...tfp_CConstraints>
	consteval auto fg_SqlTable
		(
			NStr::TCStrConst<NStr::CStr> _Name
			, TCSqlColumns<tfp_CColumns...> _Columns
			, TCSqlIndexes<tfp_CIndexes...> _Indexes
			, TCSqlConstraints<tfp_CConstraints...> _Constraints
		)
	{
		auto Table = TCSqlTable<tf_CRow, TCSqlColumns<tfp_CColumns...>, TCSqlIndexes<tfp_CIndexes...>, TCSqlConstraints<tfp_CConstraints...>>(&_Name.m_Str, _Columns, _Indexes, _Constraints);
		Table.m_NameHash = fg_SqlHashMixString(gc_SqlHashSeed, _Name);

		return Table;
	}

	template <ESqlDialect t_Dialect, typename ...tfp_CTables>
	consteval auto fg_SqlDatabase(NStr::TCStrConst<NStr::CStr> _Name, tfp_CTables &&...p_Tables)
	{
		return TCSqlDatabase<t_Dialect, NTraits::TCRemoveReferenceAndQualifiers<tfp_CTables>...>(&_Name.m_Str, NStorage::fg_Tuple(fg_Forward<tfp_CTables>(p_Tables)...));
	}

	template <typename ...tfp_CTables>
	consteval auto fg_SqlDatabase(NStr::TCStrConst<NStr::CStr> _Name, tfp_CTables &&...p_Tables)
	{
		return fg_SqlDatabase<ESqlDialect::mc_SQL1999>(_Name, fg_Forward<tfp_CTables>(p_Tables)...);
	}

	template <auto tf_pIDMember, auto tf_pVersionMember, auto ...tfp_pSaveMembers, typename tf_CTable>
	consteval auto fg_SqlRepository(tf_CTable const &_Table)
	{
		return TCSqlRepository<tf_CTable, tf_pIDMember, tf_pVersionMember, tfp_pSaveMembers...>(_Table);
	}

	template <typename tf_CDatabase>
	consteval auto fg_SqlSchemaVersion(NStr::TCStrConst<NStr::CStr> _ID, tf_CDatabase &&_Database)
	{
		return TCSqlSchemaVersion<NTraits::TCRemoveReferenceAndQualifiers<tf_CDatabase>>(&_ID.m_Str, fg_Forward<tf_CDatabase>(_Database));
	}

	consteval auto fg_SqlRenameTable(NStr::TCStrConst<NStr::CStr> _OldName, NStr::TCStrConst<NStr::CStr> _NewName)
	{
		return CSqlSchemaRenameTable(&_OldName.m_Str, &_NewName.m_Str);
	}

	consteval auto fg_SqlRenameColumn(NStr::TCStrConst<NStr::CStr> _TableName, NStr::TCStrConst<NStr::CStr> _OldName, NStr::TCStrConst<NStr::CStr> _NewName)
	{
		return TCSqlSchemaRenameColumn(&_TableName.m_Str, &_OldName.m_Str, &_NewName.m_Str);
	}

	consteval auto fg_SqlRebuildTable(NStr::TCStrConst<NStr::CStr> _TableName)
	{
		return CSqlSchemaRebuildTable(&_TableName.m_Str);
	}

	consteval auto fg_SqlDropTable(NStr::TCStrConst<NStr::CStr> _TableName)
	{
		return CSqlSchemaDropTable(&_TableName.m_Str);
	}

	consteval auto fg_SqlDropColumn(NStr::TCStrConst<NStr::CStr> _TableName, NStr::TCStrConst<NStr::CStr> _ColumnName)
	{
		return CSqlSchemaDropColumn(&_TableName.m_Str, &_ColumnName.m_Str);
	}

	consteval auto fg_SqlUpdateColumnSql(NStr::TCStrConst<NStr::CStr> _TableName, NStr::TCStrConst<NStr::CStr> _ColumnName, NStr::TCStrConst<NStr::CStr> _Sql)
	{
		return CSqlSchemaUpdateColumnSql(&_TableName.m_Str, &_ColumnName.m_Str, &_Sql.m_Str);
	}

	template <typename tf_CSchemaVersion, typename ...tfp_COperations>
	consteval auto fg_SqlSchemaMigration(tf_CSchemaVersion &&_SchemaVersion, tfp_COperations &&...p_Operations)
	{
		return TCSqlSchemaMigration<NTraits::TCRemoveReferenceAndQualifiers<tf_CSchemaVersion>, NTraits::TCRemoveReferenceAndQualifiers<tfp_COperations>...>
			(
				fg_Forward<tf_CSchemaVersion>(_SchemaVersion)
				, NStorage::fg_Tuple(fg_Forward<tfp_COperations>(p_Operations)...)
			)
		;
	}

	template <typename ...tfp_CMigrations>
	consteval auto fg_SqlSchemaMigrations(tfp_CMigrations &&...p_Migrations)
	{
		return TCSqlSchemaMigrations<NTraits::TCRemoveReferenceAndQualifiers<tfp_CMigrations>...>(NStorage::fg_Tuple(fg_Forward<tfp_CMigrations>(p_Migrations)...));
	}

	template <typename ...tfp_CMigrations>
	consteval auto fg_SqlSchemaVersions(tfp_CMigrations &&...p_Migrations)
	{
		return TCSqlSchemaVersions<TCSqlSchemaMigrations<NTraits::TCRemoveReferenceAndQualifiers<tfp_CMigrations>...>>(fg_SqlSchemaMigrations(fg_Forward<tfp_CMigrations>(p_Migrations)...));
	}
}
