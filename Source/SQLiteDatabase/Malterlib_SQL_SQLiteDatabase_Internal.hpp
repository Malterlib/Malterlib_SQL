// Copyright © Unbroken AB
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

#pragma once

namespace NMib::NSQL::NPrivate
{
	// The statement is owned by this object and must be set
	inline sqlite3_stmt &CSqliteStatement::f_Statement() const
	{
		return *m_pStatement;
	}
}
