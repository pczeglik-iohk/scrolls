use pallas::ledger::traverse::{Asset, MultiEraBlock, MultiEraOutput, MultiEraTx, OutputRef};
use serde::Deserialize;

use crate::{crosscut, model, prelude::*};

#[derive(Deserialize)]
pub struct Config {
    pub key_prefix: Option<String>,
    pub member_prefix: String,
    // filter for outputs with factory or pool NFT
    // comment for build
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
                        if found {
                            break;
                        }
                        if let Some(pid) = asset.policy_hex() {
                            found = pid.eq(policy_id)
                        }
                    }

                    if !found {
                        return false;
                    }
                }
                return true;
            }
            None => true,
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
                }
                _ => (),
            }
        }
        return non_ada_fungible_assets;
    }

    fn key_for_asset(&self, asset: &Asset) -> Option<String> {
        if let (Some(policy_id), Some(asset_name)) = (asset.policy_hex(), asset.ascii_name()) {
            return Some(format!("{}.{}", policy_id, asset_name));
        }
        return None;
    }

    fn get_key_value_pair(
        &self,
        mut fungible_non_ada_assets: Vec<Asset>,
        utxo: &MultiEraOutput,
    ) -> Option<(String, String)> {
        let key: String;
        let member: String;

        let mut min_non_ada_assets = 0;
        match &self.config.filter_policy_ids_hex {
            Some(policies) => {
                min_non_ada_assets = policies.len();
            }
            _ => (),
        }

        if utxo.non_ada_assets().len() == min_non_ada_assets + 1
            && fungible_non_ada_assets.len() == 1
        {
            // ada pool
            if let Some(asset) = fungible_non_ada_assets.get(0) {
                // key is just a single policy id
                match self.key_for_asset(asset) {
                    Some(k) => key = k,
                    None => return None,
                }
                match *asset {
                    Asset::NativeAsset(_, _, q) => {
                        member = format!(
                            "{}:{}:{}",
                            self.config.member_prefix,
                            utxo.lovelace_amount().to_string(),
                            q.to_string()
                        );
                    }
                    _ => return None,
                }
            } else {
                return None;
            }
        } else if utxo.non_ada_assets().len() == min_non_ada_assets + 2
            && fungible_non_ada_assets.len() == 2
        {
            // sort by policy id to always create same redis keys
            fungible_non_ada_assets
                .sort_by(|a1, a2| a1.policy_hex().unwrap().cmp(&a2.policy_hex().unwrap()));

            if let (Some(asset_a), Some(asset_b)) = (
                fungible_non_ada_assets.get(0),
                fungible_non_ada_assets.get(1),
            ) {
                match (self.key_for_asset(asset_a), self.key_for_asset(asset_b)) {
                    (Some(k1), Some(k2)) => key = format!("{}:{}", k1, k2),
                    _ => return None,
                }
                match (asset_a, asset_b) {
                    (Asset::NativeAsset(_, _, q1), Asset::NativeAsset(_, _, q2)) => {
                        member = format!(
                            "{}:{}:{}",
                            self.config.member_prefix,
                            q1.to_string(),
                            q2.to_string()
                        )
                    }
                    _ => return None,
                }
            } else {
                return None;
            }
        } else {
            return None;
        }

        return Some((key, member));
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
            None => return Ok(()),
        };
        let address = utxo.address().map(|addr| addr.to_string()).or_panic()?;
        if address != self.config.pool_contract_address {
            return Ok(());
        }

        if !self.contains_expected_policy_ids(utxo.non_ada_assets()) {
            return Ok(());
        }

        let fungible_non_ada_assets: Vec<Asset> = self.get_fungible_assets(utxo.non_ada_assets());
        if let Some((key, member)) = self.get_key_value_pair(fungible_non_ada_assets, &utxo) {
            // removes member from liquidity pool (old liquidity)
            let crdt =
                model::CRDTCommand::set_remove(self.config.key_prefix.as_deref(), &key, member);

            output.send(crdt.into())
        } else {
            return Ok(());
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
            return Ok(());
        }

        if !self.contains_expected_policy_ids(utxo.non_ada_assets()) {
            return Ok(());
        }

        let fungible_non_ada_assets: Vec<Asset> = self.get_fungible_assets(utxo.non_ada_assets());
        if let Some((key, member)) = self.get_key_value_pair(fungible_non_ada_assets, utxo) {
            // adds member to liquidity pool (new liquidity)
            let crdt = model::CRDTCommand::set_add(self.config.key_prefix.as_deref(), &key, member);

            output.send(crdt.into())
        } else {
            return Ok(());
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
