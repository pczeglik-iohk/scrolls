use pallas::ledger::traverse::{MultiEraBlock, MultiEraOutput, Asset, MultiEraTx, OutputRef};
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
    policy: crosscut::policies::RuntimePolicy,
}

impl Reducer {

    fn contains_expected_policy_ids(&self, assets: Vec<Asset>) -> bool {
        match &self.config.filter_policy_ids_hex {
            Some(expected_policy_ids) => {
                for policy_id in expected_policy_ids {
                    let mut found = false;
        
                    for asset in &assets {
                        if found { break; }
                        if let Some(pid) = asset.policy_hex() {
                            found = pid.eq(policy_id)
                        }
                    }

                    if !found {
                        return false
                    }
                }
                return true
            },
            None => true
        }
    }

    fn get_fungible_assets(&self, assets: Vec<Asset>) -> Vec<Asset> {
        let mut non_ada_fungible_assets: Vec<Asset> = Vec::new();
        for asset in assets {
            match asset {
                Asset::NativeAsset(_, _, quantity) => {
                    if quantity > 1 {
                        non_ada_fungible_assets.push(asset);
                    }
                },
                _ => (),
            }
        }
        return non_ada_fungible_assets
    }

    fn key_for_asset(&self, asset: &Asset) -> Option<String> {
        if let (Some(policy_id), Some(name)) = (asset.policy_hex(), asset.ascii_name()) {
            return Some(format!("{}.{}", policy_id, name));
        }
        return None
    }

    fn get_key_value_pair(&self, mut non_fungible_assets: Vec<Asset>, utxo: &MultiEraOutput) -> Option<(String, String)> {
        let key: String;
        let member: String;

        match (non_fungible_assets.len(), utxo.non_ada_assets().len()) {
            // ADA / native asset pool
            (1, 3) => {
                let asset = non_fungible_assets.get(0).unwrap();
                // key is just a single policy id
                key = self.key_for_asset(asset).unwrap();
                match *asset {
                    Asset::NativeAsset(_, _, q) => {
                        member = format!("{}:{}", utxo.lovelace_amount().to_string(), q.to_string());
                    },
                    _ => return None
                }
            },
            // native asset / native asset pool
            (2, 4) => {
                // sort by policy id to always create same redis keys
                non_fungible_assets.sort_by(|a1, a2| a1.policy_hex().unwrap().cmp(&a2.policy_hex().unwrap()));
                let asset_a = non_fungible_assets.get(0).unwrap();
                let asset_b = non_fungible_assets.get(1).unwrap();
                key = format!("{}:{}", self.key_for_asset(asset_a).unwrap(), self.key_for_asset(asset_b).unwrap());
                
                match (asset_a, asset_b) {
                    (Asset::NativeAsset(_, _, q1), Asset::NativeAsset(_, _, q2)) => {
                        member = format!("{}:{}", q1.to_string(), q2.to_string())
                    },
                    _ => return None
                }
            },
            _ => return None // invalid asset number
        }

        return Some((key, member))
    }

    fn process_consumed_txo(
        &mut self,
        ctx: &model::BlockContext,
        input: &OutputRef,
        output: &mut super::OutputPort,
    ) -> Result<(), gasket::error::Error> {
        let utxo = ctx.find_utxo(input).apply_policy(&self.policy).or_panic()?;
        let utxo = match utxo {
            Some(x) => x,
            None => return Ok(())
        };
        let address = utxo.address().map(|addr| addr.to_string()).or_panic()?;
        if address != self.config.pool_contract_address {
            return Ok(())
        }

        if !self.contains_expected_policy_ids(utxo.non_ada_assets()) {
            return Ok(())
        }

        let fungible_non_ada_assets: Vec<Asset> = self.get_fungible_assets(utxo.non_ada_assets());
        if let Some((key, member)) = self.get_key_value_pair(fungible_non_ada_assets, &utxo) {
            // removes member from liquidity pool (old liquidity)
            let crdt = model::CRDTCommand::set_remove(
                self.config.key_prefix.as_deref(),
                &key,
                member
            );

            output.send(crdt.into())
        } else {
            return Ok(())
        }
    }

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

        if !self.contains_expected_policy_ids(utxo.non_ada_assets()) {
            return Ok(())
        }

        let fungible_non_ada_assets: Vec<Asset> = self.get_fungible_assets(utxo.non_ada_assets());
        if let Some((key, member)) = self.get_key_value_pair(fungible_non_ada_assets, utxo) {
            // adds member to liquidity pool (new liquidity)
            let crdt = model::CRDTCommand::set_add(
                self.config.key_prefix.as_deref(),
                &key,
                member
            );
    
            output.send(crdt.into())
        } else {
            return Ok(())
        }
    }

    pub fn reduce_block<'b>(
        &mut self,
        block: &'b MultiEraBlock<'b>,
        ctx: &model::BlockContext,
        output: &mut super::OutputPort,
    ) -> Result<(), gasket::error::Error> {
        for tx in block.txs().into_iter() {
            for consumed in tx.consumes().iter().map(|i| i.output_ref()) {
                self.process_consumed_txo(&ctx, &consumed, output)?;
            }

            for (idx, produced) in tx.produces() {
                self.process_produced_txo(&tx, &produced, idx, output)?;
            }
        }

        Ok(())
    }
}

impl Config {
    
    pub fn plugin(self, policy: &crosscut::policies::RuntimePolicy) -> super::Reducer {
        let reducer = Reducer {
            config: self,
            policy: policy.clone(),
        };
        super::Reducer::LiquidityByTokenPair(reducer)
    }
}
