// Copyright © Unbroken AB
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

#include "Malterlib_SQL_SQLiteDatabase_Internal.h"

namespace NMib::NSQL::NPrivate
{
	NConcurrency::TCFuture<CSqlTransactionInterface> CSQLiteDatabaseBackendActor::f_BeginTransaction(CSqlTransactionSettings _Settings)
	{
		CSQLiteDatabaseCheckout Checkout = co_await fp_CheckoutWriteDatabase();
		NStorage::TCSharedPointer<CSQLiteDatabaseHandle> pOwnedDatabase;

		if (!mp_bSingleConnection)
			pOwnedDatabase = fg_Construct();

		auto Transaction = NConcurrency::fg_ConstructActor<CSQLiteTransactionActor>(mp_Schema, mp_Settings, _Settings, false, fg_Move(Checkout), fg_Move(pOwnedDatabase));
		co_await Transaction(&CSQLiteTransactionActor::f_OpenBegin);

		auto Subscription = NConcurrency::g_ActorSubscription / [Transaction]() mutable -> NConcurrency::TCFuture<void>
			{
				co_await Transaction(&ICSqlTransactionActor::f_RollbackTransaction);

				co_return {};
			}
		;

		co_return CSqlTransactionInterface(fg_Move(Transaction), fg_Move(Subscription));
	}

	NConcurrency::TCFuture<CSqlTransactionInterface> CSQLiteDatabaseBackendActor::f_BeginReadTransaction(CSqlTransactionSettings _Settings)
	{
		CSQLiteDatabaseCheckout Checkout;
		NStorage::TCSharedPointer<CSQLiteDatabaseHandle> pOwnedDatabase;

		if (mp_bSingleConnection)
			Checkout = co_await fp_CheckoutWriteDatabase();
		else
		{
			co_await fp_WaitForOpen();
			pOwnedDatabase = fg_Construct();
		}

		auto Transaction = NConcurrency::fg_ConstructActor<CSQLiteTransactionActor>(mp_Schema, mp_Settings, _Settings, true, fg_Move(Checkout), fg_Move(pOwnedDatabase));
		co_await Transaction(&CSQLiteTransactionActor::f_OpenBegin);

		auto Subscription = NConcurrency::g_ActorSubscription / [Transaction]() mutable -> NConcurrency::TCFuture<void>
			{
				co_await Transaction(&ICSqlTransactionActor::f_RollbackTransaction);

				co_return {};
			}
		;

		co_return CSqlTransactionInterface(fg_Move(Transaction), fg_Move(Subscription));
	}
}
