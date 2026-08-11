// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Shared OCM stream handler state.

use std::collections::VecDeque;

use ahash::{AHashMap, AHashSet};
use nautilus_core::UnixNanos;
use nautilus_model::{
    identifiers::{ClientOrderId, InstrumentId, StrategyId},
    types::Quantity,
};
use rust_decimal::Decimal;

use crate::{
    common::{
        parse::{make_customer_order_ref, make_customer_order_ref_legacy},
        types::OrderSyncEntry,
    },
    stream::parse::FillTracker,
};

/// Shared mutable state for the OCM stream handler.
///
/// Accessed by both the TCP reader closure and the execution client methods
/// (submit, modify, connect/disconnect). All access goes through `Arc<Mutex<>>`.
#[derive(Debug, Default)]
pub struct OcmState {
    pub fill_tracker: FillTracker,
    /// Maps customer_order_ref (rfo) to ClientOrderId for stream resolution.
    pub customer_order_refs: AHashMap<String, ClientOrderId>,
    /// Maps client_order_id to submitting strategy. Captured at submit so the stream task
    /// builds direct events for tracked orders without cache access.
    pub order_strategies: AHashMap<ClientOrderId, StrategyId>,
    /// Client order IDs that already had an `OrderAccepted` emitted (via the HTTP
    /// place response or stream synthesis), so acceptance is applied exactly once.
    pub accepted_orders: AHashSet<ClientOrderId>,
    /// Client order IDs that already received an OCM order status update.
    pub stream_reported_client_orders: AHashSet<ClientOrderId>,
    /// Bet IDs that have received a terminal event (cancel, lapse, fill-complete).
    pub terminal_orders: AHashSet<String>,
    terminal_order_queue: VecDeque<String>,
    /// Old bet IDs from replace operations, to suppress late stream updates.
    pub replaced_venue_order_ids: AHashSet<String>,
    /// (client_order_id, old_bet_id) pairs for in-flight replace operations.
    pub pending_update_keys: AHashSet<(ClientOrderId, String)>,
    pending_replaces: AHashMap<(ClientOrderId, String), PendingReplace>,
    pending_quantity_updates: AHashMap<(ClientOrderId, String), PendingQuantityUpdate>,
    confirmed_quantity_updates: AHashMap<(ClientOrderId, String), Quantity>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct PendingReplace {
    pub instrument_id: InstrumentId,
    pub quantity: Quantity,
    pub filled_qty: Quantity,
    reconcile_after: Option<UnixNanos>,
}

#[derive(Debug, Clone, Copy)]
struct PendingQuantityUpdate {
    original: Quantity,
    requested: Quantity,
}

impl OcmState {
    /// Bounds dedup memory while retaining recent delayed stream and REST overlap.
    const TERMINAL_ORDER_RETENTION: usize = 10_000;

    /// Registers a customer_order_ref mapping for a new order.
    pub fn register_customer_order_ref(&mut self, client_order_id: ClientOrderId) {
        let rfo = make_customer_order_ref(client_order_id.as_str());
        self.customer_order_refs.insert(rfo, client_order_id);
    }

    /// Registers both current and legacy customer_order_ref truncations.
    ///
    /// Used during reconnect sync for pre-existing orders that may
    /// have been placed with either truncation format.
    pub fn register_customer_order_ref_with_legacy(&mut self, client_order_id: ClientOrderId) {
        let rfo = make_customer_order_ref(client_order_id.as_str());
        let rfo_legacy = make_customer_order_ref_legacy(client_order_id.as_str());
        self.customer_order_refs.insert(rfo, client_order_id);

        if rfo_legacy != client_order_id.as_str() {
            self.customer_order_refs.insert(rfo_legacy, client_order_id);
        }
    }

    /// Records the submitting strategy for a tracked order.
    pub fn register_order_identity(
        &mut self,
        client_order_id: ClientOrderId,
        strategy_id: StrategyId,
    ) {
        self.order_strategies.insert(client_order_id, strategy_id);
    }

    /// Returns the submitting strategy for a tracked order, if known.
    pub fn order_strategy_id(&self, client_order_id: &ClientOrderId) -> Option<StrategyId> {
        self.order_strategies.get(client_order_id).copied()
    }

    /// Records that acceptance has been emitted for a tracked order.
    ///
    /// Returns `true` when this call newly marks the order accepted (the caller
    /// should emit `OrderAccepted`), or `false` when acceptance was already emitted.
    pub fn mark_accepted(&mut self, client_order_id: ClientOrderId) -> bool {
        self.accepted_orders.insert(client_order_id)
    }

    /// Removes customer_order_ref mappings for a client_order_id.
    pub fn remove_customer_order_refs(&mut self, client_order_id: &ClientOrderId) {
        let rfo = make_customer_order_ref(client_order_id.as_str());
        let rfo_legacy = make_customer_order_ref_legacy(client_order_id.as_str());
        self.customer_order_refs.remove(&rfo);
        self.customer_order_refs.remove(&rfo_legacy);
        self.order_strategies.remove(client_order_id);
        self.accepted_orders.remove(client_order_id);
    }

    /// Resolves a client_order_id from the unmatched order's rfo field.
    pub fn resolve_client_order_id(&self, rfo: Option<&str>) -> Option<ClientOrderId> {
        rfo.and_then(|r| self.customer_order_refs.get(r).copied())
    }

    /// Returns `true` if a cancel/lapse for this bet should be suppressed
    /// because a replace operation is pending or the bet was already replaced.
    pub fn should_suppress_cancel(&self, client_order_id: &ClientOrderId, bet_id: &str) -> bool {
        if self.replaced_venue_order_ids.contains(bet_id) {
            return true;
        }

        self.pending_update_keys
            .contains(&(*client_order_id, bet_id.to_string()))
    }

    pub(crate) fn register_pending_replace(
        &mut self,
        client_order_id: ClientOrderId,
        bet_id: String,
        instrument_id: InstrumentId,
        quantity: Quantity,
        filled_qty: Quantity,
    ) {
        self.pending_update_keys
            .insert((client_order_id, bet_id.clone()));
        self.pending_replaces.insert(
            (client_order_id, bet_id),
            PendingReplace {
                instrument_id,
                quantity,
                filled_qty,
                reconcile_after: None,
            },
        );
    }

    pub(crate) fn mark_pending_replace_ambiguous(
        &mut self,
        client_order_id: &ClientOrderId,
        old_bet_id: &str,
        reconcile_after: UnixNanos,
    ) -> bool {
        let key = (*client_order_id, old_bet_id.to_string());
        let Some(pending) = self.pending_replaces.get_mut(&key) else {
            return false;
        };

        pending.reconcile_after = Some(reconcile_after);
        true
    }

    pub(crate) fn pending_replace_reconcilable(
        &self,
        client_order_id: &ClientOrderId,
        old_bet_id: &str,
        ts_now: UnixNanos,
    ) -> bool {
        let key = (*client_order_id, old_bet_id.to_string());
        self.pending_replaces
            .get(&key)
            .and_then(|pending| pending.reconcile_after)
            .is_some_and(|reconcile_after| ts_now >= reconcile_after)
    }

    pub(crate) fn resolve_pending_replace(
        &mut self,
        client_order_id: &ClientOrderId,
        replacement_bet_id: &str,
        instrument_id: InstrumentId,
    ) -> Option<PendingReplace> {
        if self.replaced_venue_order_ids.contains(replacement_bet_id) {
            return None;
        }

        let pending: Vec<_> = self
            .pending_update_keys
            .iter()
            .filter(|(cid, bet_id)| {
                cid == client_order_id
                    && bet_id != replacement_bet_id
                    && self
                        .pending_replaces
                        .get(&(*cid, bet_id.clone()))
                        .is_some_and(|pending| pending.instrument_id == instrument_id)
            })
            .cloned()
            .collect();
        let resolution = pending
            .iter()
            .find_map(|update| self.pending_replaces.remove(update));

        for update in &pending {
            self.pending_update_keys.remove(update);
            self.pending_replaces.remove(update);
            self.replaced_venue_order_ids.insert(update.1.clone());
        }

        resolution
    }

    pub(crate) fn complete_pending_replace(
        &mut self,
        client_order_id: &ClientOrderId,
        old_bet_id: &str,
    ) -> Option<PendingReplace> {
        let key = (*client_order_id, old_bet_id.to_string());
        self.pending_update_keys.remove(&key);
        let pending = self.pending_replaces.remove(&key);
        if pending.is_some() {
            self.replaced_venue_order_ids.insert(old_bet_id.to_string());
        }
        pending
    }

    pub(crate) fn clear_pending_replace(
        &mut self,
        client_order_id: &ClientOrderId,
        old_bet_id: &str,
    ) -> Option<PendingReplace> {
        let key = (*client_order_id, old_bet_id.to_string());
        self.pending_update_keys.remove(&key);
        self.pending_replaces.remove(&key)
    }

    pub(crate) fn pending_replace(
        &self,
        client_order_id: &ClientOrderId,
    ) -> Option<(String, PendingReplace)> {
        self.pending_replaces
            .iter()
            .find(|((cid, _), _)| cid == client_order_id)
            .map(|((_, bet_id), pending)| (bet_id.clone(), *pending))
    }

    pub(crate) fn pending_replace_for_bet(
        &self,
        client_order_id: &ClientOrderId,
        old_bet_id: &str,
    ) -> Option<PendingReplace> {
        self.pending_replaces
            .get(&(*client_order_id, old_bet_id.to_string()))
            .copied()
    }

    pub(crate) fn register_pending_quantity_update(
        &mut self,
        client_order_id: ClientOrderId,
        bet_id: String,
        original: Quantity,
        requested: Quantity,
    ) {
        self.pending_quantity_updates.insert(
            (client_order_id, bet_id),
            PendingQuantityUpdate {
                original,
                requested,
            },
        );
    }

    pub(crate) fn resolve_pending_quantity_update(
        &mut self,
        client_order_id: &ClientOrderId,
        bet_id: &str,
        active_quantity: Quantity,
    ) -> bool {
        let key = (*client_order_id, bet_id.to_string());
        let Some(update) = self.pending_quantity_updates.get(&key) else {
            return false;
        };

        if active_quantity < update.requested || active_quantity >= update.original {
            return false;
        }

        self.pending_quantity_updates.remove(&key);
        self.confirmed_quantity_updates.insert(key, active_quantity);
        true
    }

    pub(crate) fn complete_pending_quantity_update(
        &mut self,
        client_order_id: &ClientOrderId,
        bet_id: &str,
        quantity: Quantity,
    ) -> bool {
        let key = (*client_order_id, bet_id.to_string());
        if self.pending_quantity_updates.remove(&key).is_none() {
            return false;
        }

        self.confirmed_quantity_updates.insert(key, quantity);
        true
    }

    pub(crate) fn confirmed_quantity_update(
        &self,
        client_order_id: &ClientOrderId,
        bet_id: &str,
    ) -> Option<Quantity> {
        self.confirmed_quantity_updates
            .get(&(*client_order_id, bet_id.to_string()))
            .copied()
    }

    pub(crate) fn clear_pending_quantity_update(
        &mut self,
        client_order_id: &ClientOrderId,
        bet_id: &str,
    ) -> bool {
        let key = (*client_order_id, bet_id.to_string());
        let pending = self.pending_quantity_updates.remove(&key).is_some();
        let confirmed = self.confirmed_quantity_updates.remove(&key).is_some();
        pending || confirmed
    }

    /// Cleans up customer_order_ref mappings for a terminal order,
    /// unless a pending replace exists for this client_order_id.
    pub fn cleanup_terminal_order(&mut self, client_order_id: &ClientOrderId) {
        let has_pending = self
            .pending_update_keys
            .iter()
            .any(|(cid, _)| cid == client_order_id);

        if !has_pending {
            self.remove_customer_order_refs(client_order_id);
        }
    }

    /// Records a terminal bet and bounds the stream and REST dedup state.
    pub fn mark_terminal_order(&mut self, bet_id: String) {
        if !self.terminal_orders.insert(bet_id.clone()) {
            return;
        }

        self.terminal_order_queue.push_back(bet_id);
        if self.terminal_order_queue.len() > Self::TERMINAL_ORDER_RETENTION
            && let Some(expired_bet_id) = self.terminal_order_queue.pop_front()
        {
            self.terminal_orders.remove(&expired_bet_id);
            self.fill_tracker.prune(&expired_bet_id);
        }
    }

    /// Anchors the fill tracker against cached orders so the post-reconnect
    /// image neither treats cumulative size as a new fill nor re-emits a
    /// fill that was published via another channel.
    pub fn sync_from_orders(&mut self, orders: &[OrderSyncEntry]) {
        for entry in orders {
            if entry.is_closed {
                self.mark_terminal_order(entry.bet_id.clone());
            } else {
                self.register_customer_order_ref_with_legacy(entry.client_order_id);
            }

            if entry.filled_qty > Decimal::ZERO {
                self.fill_tracker
                    .sync_order(&entry.bet_id, entry.filled_qty, entry.avg_px);
            }

            if !entry.trade_ids.is_empty() {
                self.fill_tracker
                    .seed_published_trade_ids(entry.trade_ids.iter().cloned());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn terminal_order_retention_evicts_fill_tracker_state() {
        let mut state = OcmState::default();
        let first_bet_id = "bet-0";
        let size_matched = Decimal::new(10, 0);
        let average_price = Decimal::new(20, 1);

        assert!(
            state
                .fill_tracker
                .advance_cumulative_fill(
                    first_bet_id,
                    size_matched,
                    Some(average_price),
                    average_price,
                )
                .is_some(),
        );
        state.mark_terminal_order(first_bet_id.to_string());
        for index in 1..=OcmState::TERMINAL_ORDER_RETENTION {
            state.mark_terminal_order(format!("bet-{index}"));
        }

        let replay_after_eviction = state.fill_tracker.advance_cumulative_fill(
            first_bet_id,
            size_matched,
            Some(average_price),
            average_price,
        );

        assert!(!state.terminal_orders.contains(first_bet_id));
        assert!(replay_after_eviction.is_some());
    }

    #[rstest]
    fn resolve_pending_replace_moves_old_bet_to_replaced_state() {
        let client_order_id = ClientOrderId::from("O-REPLACE");
        let old_bet_id = "old-bet".to_string();
        let instrument_id = InstrumentId::from("1.100-1-0.BETFAIR");
        let mut state = OcmState::default();
        state.register_pending_replace(
            client_order_id,
            old_bet_id.clone(),
            instrument_id,
            Quantity::from("10"),
            Quantity::from("3"),
        );

        let pending = state
            .resolve_pending_replace(&client_order_id, "new-bet", instrument_id)
            .unwrap();
        assert_eq!(pending.instrument_id, instrument_id);
        assert_eq!(pending.quantity, Quantity::from("10"));
        assert_eq!(pending.filled_qty, Quantity::from("3"));
        assert!(state.pending_update_keys.is_empty());
        assert_eq!(state.replaced_venue_order_ids.len(), 1);
        assert!(state.replaced_venue_order_ids.contains(&old_bet_id));
        assert!(
            state
                .resolve_pending_replace(&client_order_id, "newer-bet", instrument_id)
                .is_none()
        );
    }

    #[rstest]
    fn pending_replace_requires_matching_instrument_and_completed_ambiguity_window() {
        let client_order_id = ClientOrderId::from("O-REPLACE-COLLISION");
        let instrument_id = InstrumentId::from("1.100-1-0.BETFAIR");
        let foreign_instrument_id = InstrumentId::from("1.200-2-0.BETFAIR");
        let old_bet_id = "old-bet";
        let mut state = OcmState::default();
        state.register_pending_replace(
            client_order_id,
            old_bet_id.to_string(),
            instrument_id,
            Quantity::from("10"),
            Quantity::from("0"),
        );

        assert!(!state.pending_replace_reconcilable(
            &client_order_id,
            old_bet_id,
            UnixNanos::from(20),
        ));
        assert!(state.mark_pending_replace_ambiguous(
            &client_order_id,
            old_bet_id,
            UnixNanos::from(20),
        ));
        assert!(!state.pending_replace_reconcilable(
            &client_order_id,
            old_bet_id,
            UnixNanos::from(19),
        ));
        assert!(state.pending_replace_reconcilable(
            &client_order_id,
            old_bet_id,
            UnixNanos::from(20),
        ));
        assert!(
            state
                .resolve_pending_replace(&client_order_id, "foreign-bet", foreign_instrument_id,)
                .is_none(),
        );
        assert!(
            state
                .resolve_pending_replace(&client_order_id, "new-bet", instrument_id)
                .is_some(),
        );
    }

    #[rstest]
    fn pending_quantity_update_requires_a_definitive_reduction() {
        let client_order_id = ClientOrderId::from("O-REDUCE");
        let bet_id = "bet-1";
        let mut state = OcmState::default();
        state.register_pending_quantity_update(
            client_order_id,
            bet_id.to_string(),
            Quantity::from("10"),
            Quantity::from("4"),
        );

        assert!(!state.resolve_pending_quantity_update(
            &client_order_id,
            bet_id,
            Quantity::from("10"),
        ));
        assert!(state.resolve_pending_quantity_update(
            &client_order_id,
            bet_id,
            Quantity::from("6"),
        ));
        assert_eq!(
            state.confirmed_quantity_update(&client_order_id, bet_id),
            Some(Quantity::from("6")),
        );
        assert!(!state.resolve_pending_quantity_update(
            &client_order_id,
            bet_id,
            Quantity::from("4"),
        ));
        assert!(state.clear_pending_quantity_update(&client_order_id, bet_id));
        assert_eq!(
            state.confirmed_quantity_update(&client_order_id, bet_id),
            None,
        );
    }

    #[rstest]
    fn historical_replaced_bet_cannot_resolve_later_replace() {
        let client_order_id = ClientOrderId::from("O-REPLACE-TWICE");
        let instrument_id = InstrumentId::from("1.100-1-0.BETFAIR");
        let mut state = OcmState::default();
        state.register_pending_replace(
            client_order_id,
            "bet-1".to_string(),
            instrument_id,
            Quantity::from("10"),
            Quantity::from("0"),
        );
        assert!(
            state
                .resolve_pending_replace(&client_order_id, "bet-2", instrument_id)
                .is_some()
        );
        state.register_pending_replace(
            client_order_id,
            "bet-2".to_string(),
            instrument_id,
            Quantity::from("10"),
            Quantity::from("0"),
        );

        assert!(
            state
                .resolve_pending_replace(&client_order_id, "bet-1", instrument_id)
                .is_none()
        );
        assert_eq!(
            state
                .pending_replace(&client_order_id)
                .map(|(bet_id, _)| bet_id),
            Some("bet-2".to_string()),
        );
        assert!(
            state
                .resolve_pending_replace(&client_order_id, "bet-3", instrument_id)
                .is_some()
        );
    }
}
