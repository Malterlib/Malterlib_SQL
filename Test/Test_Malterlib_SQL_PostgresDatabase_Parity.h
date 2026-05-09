// Copyright © Unbroken AB
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

#pragma once

#include <Mib/SQL/Database>

namespace NMib::NSQL::NTest::NPostgresDatabase
{
	NConcurrency::TCFuture<void> fg_RunPostgresParityTests(NSQL::CSqlDatabaseClient *_pDatabase);
}
