use std::mem;
use std::collections::HashMap;
use std::future::Future;
use std::hash::Hash;
use std::sync::{Arc, Mutex};

use futures::{Stream, stream, StreamExt};
use itertools::Itertools;
use rust_decimal::Decimal;

type ClientId = u16;
type TxId = u32;
type Amount = Decimal;

#[derive(Hash, Debug, Eq, PartialEq, Clone)]
pub enum TransactionType {
    Deposit,
    Withdrawal,
    Dispute,
    Resolve,
    ChargeBack,
}

/// Represents a transaction.
#[derive(Hash, Debug, Eq, PartialEq, Clone)]
pub struct Transaction {
    pub tx_type: TransactionType,
    pub client_id: ClientId,
    pub tx_id: TxId,
    pub amount: Decimal,
}

const PAGE_SIZE_IN_BYTES: usize = 4096;
const TRANSACTION_WITH_SEQ_ID_PER_MEMORY_PAGE: usize = PAGE_SIZE_IN_BYTES / (mem::size_of::<Transaction>() + mem::size_of::<usize>());

/// Represents a client account information.
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct Account {
    pub id: ClientId,
    pub available: Amount,
    pub held: Amount,
    pub locked: bool,
}

impl Account {
    fn empty(id: ClientId) -> Self {
        Account {
            id,
            available: Decimal::ZERO,
            held: Decimal::ZERO,
            locked: false,
        }
    }
}

///
/// Base API that represents a transaction log of continuous transaction payment event.
///
/// NOTE: It also preserves the insertion order of transactions.
///
pub trait TransactionLog {
    ///
    /// Stores an iter of Transaction into a backend store.
    ///
    fn insert_batch<I>(&mut self, client_id: ClientId, tx_batch: I) -> ()
        where I: IntoIterator<Item=Transaction>;

    ///
    /// List all existing account who has at least one transaction in their history.
    ///
    fn list_existing_client_ids(&self) -> Vec<ClientId>;

    ///
    /// Replay all transaction log for a specific client (if exists) and produce an Account summary.
    ///
    fn materialize_account(&self, client_id: ClientId) -> Option<Account>;
}

///
/// A [[TransactionLog]] that use memory as its backend store.
///
pub struct InMemoryTransactionLog {
    tx_log: HashMap<ClientId, Vec<Transaction>>,
    tx_index: HashMap<ClientId, HashMap<TxId, usize>>,
}

impl TransactionLog for InMemoryTransactionLog {
    fn insert_batch<I>(&mut self, client_id: ClientId, tx_batch: I)
        where I: IntoIterator<Item=Transaction> {
        let curr_client_tx_log = self.tx_log
            .entry(client_id)
            .or_insert(Vec::new());

        let curr_client_tx_index = self.tx_index
            .entry(client_id)
            .or_insert(HashMap::new());

        let mut offset = curr_client_tx_log.len();
        curr_client_tx_log
            .extend(
                tx_batch
                    .into_iter()
                    .inspect(|tx| {
                        match tx.tx_type {
                            TransactionType::Withdrawal | TransactionType::Deposit => {
                                curr_client_tx_index
                                    .entry(tx.tx_id)
                                    .or_insert(offset);
                                offset += 1;
                                ()
                            }
                            _ => ()
                        }
                    })
            )
    }

    fn list_existing_client_ids(&self) -> Vec<ClientId> {
        self.tx_index
            .keys()
            .map(|client_id| *client_id)
            .collect_vec()
    }

    fn materialize_account(&self, client_id: ClientId) -> Option<Account> {
        let curr_client_tx_log = self.tx_log.get(&client_id)?;
        let curr_client_tx_index = self.tx_index.get(&client_id)?;
        let mut account = Account::empty(client_id);
        let mut disputed_tx: HashMap<TxId, Amount> = HashMap::new();
        'replay_loop: for (i, tx) in curr_client_tx_log.iter().enumerate() {
            match &tx.tx_type {
                TransactionType::Deposit => account.available += tx.amount,
                TransactionType::Withdrawal => {
                    if account.available >= tx.amount {
                        account.available -= tx.amount
                    }
                }
                TransactionType::Dispute => {
                    let disputed_amount = curr_client_tx_index
                        .get(&tx.tx_id)
                        .and_then(|j| curr_client_tx_log[..i].get(*j))
                        .map(|tx| tx.amount);
                    match disputed_amount {
                        Some(to_held) => {
                            account.held += to_held;
                            account.available -= to_held;
                            disputed_tx.insert(tx.tx_id, to_held);
                        }
                        _ => ()
                    }
                }
                TransactionType::Resolve | TransactionType::ChargeBack => {
                    match disputed_tx.get(&tx.tx_id) {
                        Some(held_amount) => {
                            account.held -= *held_amount;
                            if tx.tx_type == TransactionType::Resolve {
                                account.available += *held_amount
                            } else {
                                account.locked = true;
                                break 'replay_loop;
                            }
                        }
                        _ => ()
                    }

                    disputed_tx.remove(&tx.tx_id);
                }
            }
        }

        Some(account)
    }
}

///
/// An implementation of [[payment_engine::TransactionLog]] that use in-memory container
/// to hold transactions.
///
impl InMemoryTransactionLog {
    pub fn new() -> Self {
        InMemoryTransactionLog {
            tx_log: HashMap::new(),
            tx_index: HashMap::new(),
        }
    }
}


///
/// Process incoming transaction to maintain client's account balance and status.
///
/// Acts as a "facade" to process transaction and query account information.
///
pub struct PaymentEngine<TxLog: TransactionLog> {
    transaction_log: Arc<Mutex<TxLog>>,
}

impl<TxLog: TransactionLog> PaymentEngine<TxLog> {

    ///
    /// Creates a new instance of [[PaymentEngine]]
    ///
    pub fn new(transaction_log: TxLog) -> PaymentEngine<TxLog> {
        PaymentEngine::from_shared(Arc::new(Mutex::new(transaction_log)))
    }

    ///
    /// Creates a new instance of [[PaymentEngine]] using a shared transaction log.
    ///
    pub fn from_shared(shared_transaction_log: Arc<Mutex<TxLog>>) -> PaymentEngine<TxLog> {
        PaymentEngine {
            transaction_log: shared_transaction_log
        }
    }

    ///
    /// Accepts a source of transaction to log into the backend system.
    ///
    /// Calls [[accept_tx_stream_with_chunk_size]] with a chunk size of [[TRANSACTION_WITH_SEQ_ID_PER_MEMORY_PAGE]]
    /// which correspond to the maximum amount of transaction indexed with their sequence order (usize) that fits
    /// into a memory page of 4Kib.
    ///
    pub fn accept_tx_stream<Source>(&mut self, source: Source) -> impl Future<Output = ()>
        where Source: Stream<Item=Transaction> {
        self.accept_tx_stream_with_chunk_size(source, TRANSACTION_WITH_SEQ_ID_PER_MEMORY_PAGE)
    }

    ///
    /// Accepts a source of transaction to log into the backend system with specific chunk size to use
    /// when batching transactions.
    ///
    /// It is recommended to use a `batch_size` that given the following equation:
    ///
    /// `batch_size` * (<memory size of [[Transaction]]>  + <memory size of [[usize]]>) gives
    /// a multiple of the underlying OS batch size which is typically 4Kib.
    ///
    pub fn accept_tx_stream_with_chunk_size<Source>(&mut self, source: Source, batch_size: usize) -> impl Future<Output = ()>
        where Source: Stream<Item=Transaction> {
        let local_tx_log = self.transaction_log.clone();
        source
            .enumerate()
            .chunks(batch_size)
            // Sorting allow us to partition transaction by client id and reduce cache miss since we are
            // processing one client at a time instead of one transaction at a time.
            .map(|mut page: Vec<(usize, Transaction)>| {
                page.sort_unstable_by_key(|(i, transaction)| (transaction.client_id, i.clone()));
                page
            })
            .flat_map(|page| {
                let group_iter = &page
                    .into_iter()
                    .map(|(_seq_id, tx)| tx)
                    .group_by(|tx| tx.client_id);

                stream::iter(group_iter
                    .into_iter()
                    .map(|(x, y)| (x, y.collect_vec()))
                    .collect_vec()
                )
            })
            .for_each(move |(client_id, transactions)| {
                let local_tx_log = local_tx_log.clone();
                async move {
                    local_tx_log.lock().unwrap().insert_batch(client_id, transactions);
                }
            })
    }

    ///
    /// Materializes existings accounts asynchronously.
    ///
    pub fn materialize_all_accounts(&self) -> impl Stream<Item = Account> + '_ {
        let client_ids = {
            self.transaction_log.lock().unwrap().list_existing_client_ids()
        };

        stream::unfold(client_ids.into_iter(), |mut client_id_iter| {
            let local_transaction_log = self.transaction_log.clone();
                async move {
                    if let Some(client_id) = client_id_iter.next() {
                        let account = local_transaction_log
                            .lock()
                            .unwrap()
                            .materialize_account(client_id)
                            .unwrap_or(Account::empty(client_id));
                        Some((account, client_id_iter))
                    } else {
                        None
                    }
                }
        })
    }
}


#[cfg(test)]
mod test {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use futures::executor::block_on;
    use futures::stream;
    use itertools::Itertools;
    use rust_decimal::Decimal;
    use futures::prelude::*;

    use crate::{InMemoryTransactionLog, PaymentEngine, Transaction, TransactionType};
    use crate::payment_engine::{Account, ClientId, TransactionLog};

    #[test]
    fn it_should_materialize_into_account_with_positive_amount() {
        let client_id = 1;
        let mut tx_log = InMemoryTransactionLog::new();
        let transactions = vec![
            Transaction {
                tx_id: 1,
                client_id: client_id,
                tx_type: TransactionType::Deposit,
                amount: Decimal::ONE_HUNDRED,
            }
        ];

        let mut expected = Account::empty(client_id);
        expected.available = Decimal::ONE_HUNDRED;
        tx_log.insert_batch(client_id, transactions);
        let actual = tx_log.materialize_account(client_id);
        assert_eq!(actual, Some(expected));
    }

    #[test]
    fn it_should_materialize_into_empty_account() {
        let client_id = 1;
        let mut tx_log = InMemoryTransactionLog::new();
        let transactions = vec![];
        let expected = Account::empty(client_id);
        tx_log.insert_batch(client_id, transactions);
        let actual = tx_log.materialize_account(client_id);
        assert_eq!(actual, Some(expected));
    }

    #[test]
    fn it_should_prevent_materializing_with_a_negative_balance() {
        let client_id = 1;
        let mut tx_log = InMemoryTransactionLog::new();
        let transactions = vec![
            Transaction {
                tx_id: 1,
                client_id: client_id,
                tx_type: TransactionType::Withdrawal,
                amount: Decimal::ONE_HUNDRED,
            }
        ];
        let expected = Account::empty(client_id);
        tx_log.insert_batch(client_id, transactions);
        let actual = tx_log.materialize_account(client_id);
        assert_eq!(actual, Some(expected));
    }

    #[test]
    fn it_should_prevent_withdrawing_more_than_available() {
        let client_id = 1;
        let mut tx_log = InMemoryTransactionLog::new();
        let transactions = vec![
            Transaction {
                tx_id: 1,
                client_id: client_id,
                tx_type: TransactionType::Deposit,
                amount: Decimal::ONE_HUNDRED,
            },
            Transaction {
                tx_id: 1,
                client_id: client_id,
                tx_type: TransactionType::Withdrawal,
                amount: Decimal::new(200, 0),
            },
        ];
        let mut expected = Account::empty(client_id);
        expected.available = Decimal::ONE_HUNDRED;
        tx_log.insert_batch(client_id, transactions);
        let actual = tx_log.materialize_account(client_id);
        assert_eq!(actual, Some(expected));
    }

    #[test]
    fn it_should_allow_to_withdraw_if_available_amount_is_enough() {
        let client_id = 1;
        let mut tx_log = InMemoryTransactionLog::new();
        let transactions = vec![
            Transaction {
                tx_id: 1,
                client_id: client_id,
                tx_type: TransactionType::Deposit,
                amount: Decimal::ONE_HUNDRED,
            },
            Transaction {
                tx_id: 1,
                client_id: client_id,
                tx_type: TransactionType::Withdrawal,
                amount: Decimal::ONE,
            },
        ];
        let mut expected = Account::empty(client_id);
        expected.available = Decimal::ONE_HUNDRED - Decimal::ONE;
        tx_log.insert_batch(client_id, transactions);
        let actual = tx_log.materialize_account(client_id);
        assert_eq!(actual, Some(expected));
    }

    #[test]
    fn it_should_locked_an_account_if_charge_back_is_applied_ignorer_all_subsequent_transaction() {
        let client_id = 1;
        let mut tx_log = InMemoryTransactionLog::new();
        let transactions = vec![
            Transaction {
                tx_id: 1,
                client_id: client_id,
                tx_type: TransactionType::Deposit,
                amount: Decimal::ONE_HUNDRED,
            },
            Transaction {
                tx_id: 1,
                client_id: client_id,
                tx_type: TransactionType::Dispute,
                amount: Decimal::ZERO,
            },
            Transaction {
                tx_id: 1,
                client_id: client_id,
                tx_type: TransactionType::ChargeBack,
                amount: Decimal::ZERO,
            },
            Transaction {
                tx_id: 2,
                client_id: client_id,
                tx_type: TransactionType::Deposit,
                amount: Decimal::ONE,
            },
        ];
        let mut expected = Account::empty(client_id);
        expected.locked = true;
        tx_log.insert_batch(client_id, transactions);
        let actual = tx_log.materialize_account(client_id);
        assert_eq!(actual, Some(expected));
    }

    #[test]
    fn it_should_resolve_a_legit_dispute() {
        let client_id = 1;
        let mut tx_log = InMemoryTransactionLog::new();
        let transactions = vec![
            Transaction {
                tx_id: 1,
                client_id: client_id,
                tx_type: TransactionType::Deposit,
                amount: Decimal::ONE_HUNDRED,
            },
            Transaction {
                tx_id: 1,
                client_id: client_id,
                tx_type: TransactionType::Dispute,
                amount: Decimal::ZERO,
            },
            Transaction {
                tx_id: 1,
                client_id: client_id,
                tx_type: TransactionType::Resolve,
                amount: Decimal::ZERO,
            },
        ];
        let mut expected = Account::empty(client_id);
        expected.available = Decimal::ONE_HUNDRED;
        tx_log.insert_batch(client_id, transactions);
        let actual = tx_log.materialize_account(client_id);
        assert_eq!(actual, Some(expected));
    }

    #[test]
    fn it_should_held_money_when_a_dispute_is_pending() {
        let client_id = 1;
        let mut tx_log = InMemoryTransactionLog::new();
        let transactions = vec![
            Transaction {
                tx_id: 1,
                client_id: client_id,
                tx_type: TransactionType::Deposit,
                amount: Decimal::ONE,
            },
            Transaction {
                tx_id: 2,
                client_id: client_id,
                tx_type: TransactionType::Deposit,
                amount: Decimal::ONE_HUNDRED,
            },
            Transaction {
                tx_id: 1,
                client_id: client_id,
                tx_type: TransactionType::Dispute,
                amount: Decimal::ZERO,
            },
        ];
        let mut expected = Account::empty(client_id);
        expected.available = Decimal::ONE_HUNDRED;
        expected.held = Decimal::ONE;
        tx_log.insert_batch(client_id, transactions);
        let actual = tx_log.materialize_account(client_id);
        assert_eq!(actual, Some(expected));
    }

    #[test]
    fn it_should_still_materialize_into_an_empty_account_even_if_client_only_has_invalid_disputes() {
        let client_id = 1;
        let mut tx_log = InMemoryTransactionLog::new();
        let transactions = vec![
            Transaction {
                tx_id: 1,
                client_id: client_id,
                tx_type: TransactionType::Dispute,
                amount: Decimal::ONE,
            }
        ];
        let expected = Account::empty(client_id);
        tx_log.insert_batch(client_id, transactions);
        let actual = tx_log.materialize_account(client_id);
        assert_eq!(actual, Some(expected));
    }

    #[test]
    fn it_should_not_chargeback_a_deposit_if_no_dispute_has_been_log_before() {
        let client_id = 1;
        let mut tx_log = InMemoryTransactionLog::new();
        let transactions = vec![
            Transaction {
                tx_id: 1,
                client_id: client_id,
                tx_type: TransactionType::Deposit,
                amount: Decimal::ONE,
            },
            Transaction {
                tx_id: 1,
                client_id,
                tx_type: TransactionType::ChargeBack,
                amount: Decimal::ZERO,
            },
        ];
        let mut expected = Account::empty(client_id);
        expected.available = Decimal::ONE;
        tx_log.insert_batch(client_id, transactions);
        let actual = tx_log.materialize_account(client_id);
        assert_eq!(actual, Some(expected));
    }

    #[test]
    fn it_should_not_resolve_a_dispute_that_never_happened() {
        let client_id = 1;
        let mut tx_log = InMemoryTransactionLog::new();
        let transactions = vec![
            Transaction {
                tx_id: 1,
                client_id: client_id,
                tx_type: TransactionType::Deposit,
                amount: Decimal::ONE,
            },
            Transaction {
                tx_id: 1,
                client_id,
                tx_type: TransactionType::Resolve,
                amount: Decimal::ZERO,
            },
        ];
        let mut expected = Account::empty(client_id);
        expected.available = Decimal::ONE;
        tx_log.insert_batch(client_id, transactions);
        let actual = tx_log.materialize_account(client_id);
        assert_eq!(actual, Some(expected));
    }

    #[test]
    fn it_should_list_no_client_ids_when_tx_log_is_empty() {
        let tx_log = InMemoryTransactionLog::new();
        assert!(tx_log.list_existing_client_ids().is_empty());
    }

    #[test]
    fn it_should_list_multiple_clients_even_those_with_invalid_transaction() {
        let mut tx_log = InMemoryTransactionLog::new();
        let transactions1 = vec![
            Transaction {
                tx_id: 1,
                client_id: 1,
                tx_type: TransactionType::Deposit,
                amount: Decimal::ONE,
            }
        ];
        let transactions2 = vec![
            Transaction {
                tx_id: 1,
                client_id: 2,
                tx_type: TransactionType::Dispute,
                amount: Decimal::ZERO,
            }
        ];
        tx_log.insert_batch(1, transactions1);
        tx_log.insert_batch(2, transactions2);
        let mut actual = tx_log.list_existing_client_ids();
        actual.sort();
        assert_eq!(actual, vec![1, 2]);
    }


    ///
    /// Stores incoming transaction, but don't do any processing logic.
    ///
    /// This struct is used for testing purposes since it allows you to "spy" incoming transaction
    /// request and validate ingestion logic.
    struct SpyTransactionLog {
        inner: HashMap<ClientId, Vec<Transaction>>
    }

    impl SpyTransactionLog {
        fn new() -> SpyTransactionLog {
            SpyTransactionLog {
                inner: HashMap::new()
            }
        }
    }

    impl TransactionLog for SpyTransactionLog {
        fn insert_batch<I>(&mut self, client_id: ClientId, tx_batch: I) -> ()
            where I: IntoIterator<Item=Transaction> {
            self.inner
                .entry(client_id)
                .or_insert(Vec::new())
                .extend(tx_batch)
        }

        fn list_existing_client_ids(&self) -> Vec<ClientId> {
            self.inner.keys().map(|x| x.clone()).collect_vec()
        }

        fn materialize_account(&self, client_id: ClientId) -> Option<Account> {
            Some(Account::empty(client_id))
        }
    }

    #[test]
    fn it_should_accept_incoming_transaction_and_log_it() {
        let spy_tx_log = Arc::new(Mutex::new(SpyTransactionLog::new()));
        let mut pe = PaymentEngine::from_shared(spy_tx_log.clone());

        let transactions = vec![
            Transaction {
                tx_id: 1,
                client_id: 1,
                tx_type: TransactionType::Deposit,
                amount: Decimal::ONE,
            },
            Transaction {
                tx_id: 2,
                client_id: 2,
                tx_type: TransactionType::Deposit,
                amount: Decimal::ONE,
            },
            Transaction {
                tx_id: 3,
                client_id: 1,
                tx_type: TransactionType::Deposit,
                amount: Decimal::ONE_HUNDRED,
            }
        ];

        let mut expected_map = HashMap::new();
        expected_map.entry(1).or_insert(vec![
            Transaction {
                tx_id: 1,
                client_id: 1,
                tx_type: TransactionType::Deposit,
                amount: Decimal::ONE,
            },
            Transaction {
                tx_id: 3,
                client_id: 1,
                tx_type: TransactionType::Deposit,
                amount: Decimal::ONE_HUNDRED,
            }
        ]);
        expected_map.entry(2).or_insert(vec![
            Transaction {
                tx_id: 2,
                client_id: 2,
                tx_type: TransactionType::Deposit,
                amount: Decimal::ONE,
            }
        ]);

        let my_stream = stream::iter(transactions.into_iter());

        block_on(pe.accept_tx_stream(my_stream));
        {
            let actual = &spy_tx_log.lock().unwrap().inner;
            assert_eq!(actual, &expected_map);
        }
    }

    #[test]
    fn it_should_accept_incoming_transaction_and_log_it_even_if_log_size_does_fit_total_incoming_transaction_count() {
        let spy_tx_log = Arc::new(Mutex::new(SpyTransactionLog::new()));
        let mut pe = PaymentEngine::from_shared(spy_tx_log.clone());

        let transactions = vec![
            Transaction {
                tx_id: 1,
                client_id: 1,
                tx_type: TransactionType::Deposit,
                amount: Decimal::ONE,
            },
            Transaction {
                tx_id: 2,
                client_id: 2,
                tx_type: TransactionType::Deposit,
                amount: Decimal::ONE,
            },
            Transaction {
                tx_id: 3,
                client_id: 1,
                tx_type: TransactionType::Deposit,
                amount: Decimal::ONE_HUNDRED,
            }
        ];

        let mut expected_map = HashMap::new();
        expected_map.entry(1).or_insert(vec![
            Transaction {
                tx_id: 1,
                client_id: 1,
                tx_type: TransactionType::Deposit,
                amount: Decimal::ONE,
            },
            Transaction {
                tx_id: 3,
                client_id: 1,
                tx_type: TransactionType::Deposit,
                amount: Decimal::ONE_HUNDRED,
            }
        ]);
        expected_map.entry(2).or_insert(vec![
            Transaction {
                tx_id: 2,
                client_id: 2,
                tx_type: TransactionType::Deposit,
                amount: Decimal::ONE,
            }
        ]);

        let my_stream = stream::iter(transactions.into_iter());
        block_on(pe.accept_tx_stream_with_chunk_size(my_stream, 2));
        let accounts: Vec<Account> = block_on(pe.materialize_all_accounts().collect());

        {
            let actual = &spy_tx_log.lock().unwrap().inner;
            assert_eq!(actual, &expected_map);
        }

        assert!(accounts.len() == 2);
    }


    #[test]
    fn payment_engine_should_accept_empty_transaction_stream() {
        let spy_tx_log = Arc::new(Mutex::new(SpyTransactionLog::new()));
        let mut pe = PaymentEngine::from_shared(spy_tx_log.clone());
        let transactions = vec![];
        let expected_map = HashMap::new();
        let my_stream = stream::iter(transactions.into_iter());

        block_on(pe.accept_tx_stream(my_stream));
        let accounts: Vec<Account> = block_on(pe.materialize_all_accounts().collect());

        {
            let actual = &spy_tx_log.lock().unwrap().inner;
            assert_eq!(actual, &expected_map);
        }
        assert!(accounts.is_empty());
    }

    #[test]
    fn it_should_test_payment_engine_integration_with_in_memory_transaction() {
        let tx_log = InMemoryTransactionLog::new();
        let transactions = vec![
            Transaction {
                tx_id: 1,
                client_id: 1,
                tx_type: TransactionType::Deposit,
                amount: Decimal::ONE,
            },
            Transaction {
                tx_id: 2,
                client_id: 2,
                tx_type: TransactionType::Deposit,
                amount: Decimal::ONE_HUNDRED,
            }
        ];

        let expected_accounts = vec![
            Account {
                id: 1,
                available: Decimal::ONE,
                held: Decimal::ZERO,
                locked: false
            },
            Account {
                id: 2,
                available: Decimal::ONE_HUNDRED,
                held: Decimal::ZERO,
                locked: false
            }
        ];

        let tx_stream = stream::iter(transactions.into_iter());
        let mut pe = PaymentEngine::new(tx_log);

        block_on(pe.accept_tx_stream(tx_stream));
        let mut actual_accounts: Vec<Account> = block_on(pe.materialize_all_accounts().collect());
        actual_accounts.sort_unstable_by_key(|account| account.id);

        assert_eq!(actual_accounts, expected_accounts);

    }
}
