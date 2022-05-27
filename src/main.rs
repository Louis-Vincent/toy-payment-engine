extern crate core;

mod payment_engine;

use std::{env, io};
use std::sync::{Arc, Mutex};
use csv::Trim;
use futures::executor::block_on;
use futures::stream;
use rust_decimal::Decimal;
use serde::Deserialize;
use serde::Serialize;
use crate::payment_engine::{Account, InMemoryTransactionLog, PaymentEngine, Transaction, TransactionType};
use futures::StreamExt;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
enum CsvTransactionType {
    Deposit,
    Withdrawal,
    Dispute,
    Resolve,
    ChargeBack,
}

/// Represents a transaction
#[derive(Debug, Deserialize)]
struct CsvTransaction {
    #[serde(rename = "type")]
    tx_type: CsvTransactionType,
    client: u16,
    tx: u32,
    amount: Option<Decimal>,
}

#[derive(Debug, Serialize)]
struct CsvAccount {
    client: u16,
    available: Decimal,
    held: Decimal,
    total: Decimal,
    locked: bool
}

impl CsvAccount {
    fn from_account(account: Account) -> CsvAccount {
        CsvAccount {
            client: account.id,
            available: account.available,
            held: account.held,
            total: account.available,
            locked: account.locked
        }
    }
}

impl CsvTransaction {
    pub fn into_transaction(self) -> Transaction {
        Transaction {
            tx_type: match self.tx_type {
                CsvTransactionType::Deposit => TransactionType::Deposit,
                CsvTransactionType::Withdrawal => TransactionType::Withdrawal,
                CsvTransactionType::Dispute=> TransactionType::Dispute,
                CsvTransactionType::Resolve=> TransactionType::Resolve,
                CsvTransactionType::ChargeBack => TransactionType::ChargeBack
            },
            tx_id: self.tx,
            client_id: self.client,
            amount: self.amount
                .map(|dec| dec.round_dp(4))
                .unwrap_or(Decimal::ZERO)
        }
    }
}


fn main() {

    let args: Vec<String> = env::args().collect();

    if args.len() != 2 {
        panic!("Missing transaction file path arguments");
    }

    match args.get(1) {
        Some(tsx_file_path) => {

            let mut pe = PaymentEngine::new(InMemoryTransactionLog::new());

            let mut reader = csv::ReaderBuilder::new()
                .delimiter(b',')
                .trim(Trim::All)
                .has_headers(true)
                .from_path(tsx_file_path)
                // fail fast and let it panic, we can not recover if user does not provide a real file.
                .unwrap();

            let writer = csv::WriterBuilder::new()
                .delimiter(b',')
                .has_headers(true)
                .from_writer(io::stdout());

            let shared_writer = Arc::new(Mutex::new(writer));

            let csv_records = reader
                .deserialize()
                .map(|record| {
                    let csv_tx: CsvTransaction = record.unwrap();
                    csv_tx.into_transaction()
                });

            block_on(pe.accept_tx_stream(stream::iter(csv_records)));
            let fut = pe
                .materialize_all_accounts()
                .for_each(|account| {
                    let local_shared_writer = shared_writer.clone();
                    async move {
                        let csv_account = CsvAccount::from_account(account);
                        // We unwrap and let it panic if any error since we can not recover from it.
                        local_shared_writer.lock().unwrap().serialize(csv_account).unwrap()
                    }
                });

            block_on(fut);
        }
        None => panic!("Missing transaction file path arguments")
    }

}
