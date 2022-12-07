use pallas::ledger::traverse::{MultiEraBlock, MultiEraOutput, Asset, MultiEraTx};
use serde::Deserialize;

use crate::{crosscut, model, prelude::*};

#[derive(Deserialize)]
pub struct Config {
    pub key_prefix: Option<String>,
    // filter for outputs with factory or pool NFT
    pub filter_policy_ids_hex: Option<Vec<String>>,
    pub pool_contract_address: String,
}

pub struct Reducer {
    config: Config,
}

impl Reducer {

    fn process_produced_txo(
        &mut self,
        _tx: &MultiEraTx,
        utxo: &MultiEraOutput,
        _output_idx: usize,
        output: &mut super::OutputPort,
    ) -> Result<(), gasket::error::Error> {
        let address = utxo.address().map(|addr| addr.to_string()).or_panic()?;
        if address != self.config.pool_contract_address {
            return Ok(())
        }
        println!("{}", &address);
        match &self.config.filter_policy_ids_hex {
            Some(expected_policy_ids) => {
                for policy_id in expected_policy_ids {
                    let mut found = false;
        
                    for asset in utxo.non_ada_assets() {
                        if found { break; }
                        if let Some(pid) = asset.policy_hex() {
                            found = pid.eq(policy_id)
                        }
                    }

                    if !found {
                        return Ok(())
                    }
                }
            }
            None => return Ok(()),
        };

        let mut non_ada_fung_assets: Vec<Asset> = Vec::new();
        for asset in utxo.non_ada_assets() {
            match asset {
                Asset::NativeAsset(_, _, quantity) => {
                    if quantity > 1 {
                        non_ada_fung_assets.push(asset);
                    }
                },
                _ => (),
            }
        }

        let key: String;
        let member: String;

        match (non_ada_fung_assets.len(), utxo.non_ada_assets().len()) {
            (1, 3) => { // ADA / token pool
                let asset = non_ada_fung_assets.get(0).unwrap();
                // key is just a single policy id
                key = format!("{}.{}", asset.policy_hex().unwrap(), asset.ascii_name().unwrap());
                match *asset {
                    Asset::NativeAsset(_, _, quantity) => {
                        member = format!("{}:{}", utxo.lovelace_amount().to_string(), quantity.to_string())
                    },
                    _ => return Ok(())
                }
            },
            (2, 4) => {
                // sort by policy id to always create same redis keys
                non_ada_fung_assets.sort_by(|a1, a2| a1.policy_hex().unwrap().cmp(&a2.policy_hex().unwrap()));
                let token_a = non_ada_fung_assets.get(0).unwrap();
                let token_b = non_ada_fung_assets.get(1).unwrap();
                let token_a_key = format!("{}.{}", token_a.policy_hex().unwrap(), token_a.ascii_name().unwrap());
                let token_b_key = format!("{}.{}", token_b.policy_hex().unwrap(), token_b.ascii_name().unwrap());
                
                key = format!(
                    "{}:{}", 
                    token_a_key,
                    token_b_key,
                );
                
                match (token_a, token_b) {
                    (Asset::NativeAsset(_, _, q1), Asset::NativeAsset(_, _, q2)) => member = format!("{}:{}", q1.to_string(), q2.to_string()),
                    _ => return Ok(())
                }
            }, // token / token pool
            _ => return Ok(()) // invalid
        };

        let crdt = model::CRDTCommand::set_add(
            self.config.key_prefix.as_deref(),
            &key,
            member
        );

        output.send(crdt.into())
    }

    pub fn reduce_block<'b>(
        &mut self,
        block: &'b MultiEraBlock<'b>,
        output: &mut super::OutputPort,
    ) -> Result<(), gasket::error::Error> {
        for tx in block.txs().into_iter() {
            for (idx, produced) in tx.produces() {
                self.process_produced_txo(&tx, &produced, idx, output)?;
            }
        }

        Ok(())
    }
}

impl Config {
    
    pub fn plugin(self) -> super::Reducer {
        let reducer = Reducer { config: self };
        super::Reducer::LiquidityByTokenPair(reducer)
    }
}
