use super::LocalAgentControl;
use super::LocalAgentRuntime;
use super::runtime::AgentTreeMembership;
use crate::agent::AgentStatus;
use crate::codex_thread::CodexThread;
use crate::config::Config;
use crate::session::session::Session;
use crate::thread_manager::ThreadManagerState;
use codex_history::InitialHistory;
use codex_history::ResumedHistory;
use codex_protocol::ThreadId;
use codex_protocol::error::AgentErrorContext;
use codex_protocol::error::CodexErr;
use codex_protocol::error::CodexErrKind;
use codex_protocol::error::CodexErrorDetails;
use codex_protocol::error::Result as CodexResult;
use codex_protocol::protocol::MultiAgentVersion;
use codex_protocol::protocol::SessionSource;
use codex_thread_store::ReadThreadParams;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::warn;

#[derive(Default)]
pub(super) struct V2Residency {
    state: Mutex<V2ResidencyState>,
}

#[derive(Default)]
struct V2ResidencyState {
    residents: VecDeque<ThreadId>,
    pending_slots: usize,
    unload_timers: HashMap<ThreadId, Arc<CancellationToken>>,
}

impl Drop for V2Residency {
    fn drop(&mut self) {
        let state = self
            .state
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for timer in state.unload_timers.values() {
            timer.cancel();
        }
    }
}

pub(super) struct V2ResidencySlot {
    residency: Arc<V2Residency>,
    active: bool,
}

impl V2ResidencySlot {
    pub(super) fn commit(mut self, thread_id: ThreadId) {
        self.residency.commit_slot(thread_id);
        self.active = false;
    }
}

impl Drop for V2ResidencySlot {
    fn drop(&mut self) {
        if self.active {
            self.residency.release_pending_slot();
        }
    }
}

impl LocalAgentControl {
    pub(super) async fn reserve_v2_residency_slot(
        &self,
        state: &Arc<ThreadManagerState>,
        config: &Config,
        membership: &AgentTreeMembership,
        protected_thread_id: Option<ThreadId>,
    ) -> CodexResult<V2ResidencySlot> {
        let capacity = config
            .effective_agent_max_threads(MultiAgentVersion::V2)
            .unwrap_or(usize::MAX);
        Arc::clone(&self.runtime.residency)
            .reserve_slot(state, capacity, membership, protected_thread_id)
            .await
    }

    pub(super) async fn touch_loaded_v2_residency(
        &self,
        state: &Arc<ThreadManagerState>,
        thread_id: ThreadId,
    ) -> CodexResult<()> {
        let thread = state.get_thread(thread_id).await?;
        self.runtime.pin_v2_residency(&thread).await?;
        Ok(())
    }

    pub(super) fn forget_v2_residency(&self, thread_id: ThreadId) {
        self.runtime.residency.remove(thread_id);
    }
}

impl LocalAgentRuntime {
    #[cfg(test)]
    pub(crate) fn has_completed_agent_unload_timer(&self, thread_id: ThreadId) -> bool {
        self.residency
            .state
            .lock()
            .unwrap()
            .unload_timers
            .contains_key(&thread_id)
    }

    pub(crate) fn cancel_completed_agent_unload(&self, thread_id: ThreadId) {
        self.residency.cancel_unload_timer(thread_id);
    }

    pub(crate) fn schedule_completed_agent_unload(&self, session: &Arc<Session>, delay_secs: u64) {
        let thread_id = session.thread_id;
        let Some(deadline) =
            tokio::time::Instant::now().checked_add(Duration::from_secs(delay_secs))
        else {
            warn!(%thread_id, "completed agent unload delay exceeds the timer range; skip unload");
            return;
        };
        let Ok(membership) = self.admit_start() else {
            return;
        };
        let timer = Arc::new(CancellationToken::new());
        {
            let mut state = self
                .residency
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(previous) = state.unload_timers.insert(thread_id, Arc::clone(&timer)) {
                previous.cancel();
            }
        }
        let residency = Arc::downgrade(&self.residency);
        let manager = self.manager.clone();
        let session = Arc::downgrade(session);
        let shutdown = self.shutdown.clone();
        tokio::spawn(async move {
            let unload = async {
                tokio::time::sleep_until(deadline).await;
                for attempt in 0..2 {
                    let done = match (residency.upgrade(), manager.upgrade(), session.upgrade()) {
                        (Some(residency), Some(manager), Some(session)) => {
                            residency
                                .try_unload_completed(&manager, &membership, &session, &timer)
                                .await
                        }
                        _ => true,
                    };
                    if done {
                        return;
                    }
                    if attempt == 0 {
                        tokio::time::sleep(Duration::from_secs(5)).await;
                    }
                }
                warn!(%thread_id, "completed agent unload is blocked after one retry; keep runtime loaded");
            };
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => {},
                _ = timer.cancelled() => {},
                _ = unload => {},
            }
            if let Some(residency) = residency.upgrade() {
                let mut state = residency
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if state
                    .unload_timers
                    .get(&thread_id)
                    .is_some_and(|current| Arc::ptr_eq(current, &timer))
                {
                    state.unload_timers.remove(&thread_id);
                }
            }
        });
    }

    /// Pins and touches the registered runtime without waiting for unrelated eviction.
    pub(crate) async fn pin_v2_residency(
        &self,
        thread: &CodexThread,
    ) -> CodexResult<Option<tokio::sync::OwnedRwLockReadGuard<()>>> {
        if !is_resident_candidate(thread) {
            return Ok(None);
        }
        let guard = Arc::clone(&thread.residency_gate).read_owned().await;
        let thread_id = thread.session.thread_id;
        let registered = self.upgrade()?.get_thread(thread_id).await?;
        if !std::ptr::eq(thread, registered.as_ref()) {
            return Err(CodexErr::ThreadNotFound(thread_id));
        }
        self.residency.touch(thread_id);
        Ok(Some(guard))
    }
}

impl V2Residency {
    async fn try_unload_completed(
        self: &Arc<Self>,
        manager: &Arc<ThreadManagerState>,
        membership: &AgentTreeMembership,
        session: &Arc<Session>,
        timer: &CancellationToken,
    ) -> bool {
        let thread_id = session.thread_id;
        let Ok(thread) = manager.get_thread(thread_id).await else {
            return true;
        };
        if !Arc::ptr_eq(session, &thread.session) || !is_resident_candidate(&thread) {
            return true;
        }
        let Ok(guard) = Arc::clone(&thread.residency_gate).try_write_owned() else {
            return false;
        };
        if !is_unloadable(&thread).await || has_background_work(&thread).await {
            return false;
        }
        let runtime = &session.services.local_agent_runtime;
        // Startup publishes metadata after the initial turn is admitted.
        if runtime
            .registry
            .agent_metadata_for_thread(thread_id)
            .is_none()
        {
            return false;
        }
        let reloadable = async {
            let stored = manager
                .read_stored_thread(ReadThreadParams {
                    thread_id,
                    include_archived: false,
                    include_history: false,
                })
                .await?;
            let Some(history) =
                super::spawn::load_agent_model_context(manager, thread_id, stored.history_mode)
                    .await?
            else {
                return Ok::<_, CodexErr>(false);
            };
            let history = InitialHistory::Resumed(ResumedHistory {
                history_revision: history.revision,
                conversation_id: thread_id,
                history: Arc::new(history.items),
                rollout_path: stored.rollout_path,
            });
            Ok(history.get_multi_agent_version() == Some(MultiAgentVersion::V2))
        }
        .await;
        if !matches!(reloadable, Ok(true)) {
            warn!(%thread_id, "completed agent has no reloadable saved history; keep runtime loaded");
            return true;
        }
        if timer.is_cancelled()
            || !manager
                .get_thread(thread_id)
                .await
                .is_ok_and(|current| Arc::ptr_eq(&current, &thread))
            || !is_unloadable(&thread).await
            || has_background_work(&thread).await
            || timer.is_cancelled()
        {
            return false;
        }
        let Ok(descendants) = runtime.live_thread_spawn_descendants(thread_id).await else {
            return false;
        };
        let mut checked_threads = vec![Arc::clone(&thread)];
        for descendant in descendants {
            if let Ok(descendant) = manager.get_thread(descendant).await {
                if !is_unloadable(&descendant).await || has_background_work(&descendant).await {
                    return false;
                }
                checked_threads.push(descendant);
            }
        }
        let eviction = {
            // No await between the final admission checks and teardown spawn. A turn
            // admitted after this point can deliver its result to the unloaded parent.
            let mut admission_guards = Vec::with_capacity(checked_threads.len());
            for checked in &checked_threads {
                let Ok(active) = checked.session.active_turn.try_lock() else {
                    return false;
                };
                if active.is_some() {
                    return false;
                }
                admission_guards.push(active);
            }
            if timer.is_cancelled() {
                return false;
            }
            self.start_eviction(manager, membership, thread, guard)
        };
        finish_eviction(eviction).await
    }

    fn cancel_unload_timer(&self, thread_id: ThreadId) {
        if let Some(timer) = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .unload_timers
            .remove(&thread_id)
        {
            timer.cancel();
        }
    }

    async fn reserve_slot(
        self: Arc<Self>,
        manager: &Arc<ThreadManagerState>,
        capacity: usize,
        membership: &AgentTreeMembership,
        protected_thread_id: Option<ThreadId>,
    ) -> CodexResult<V2ResidencySlot> {
        loop {
            if self.try_reserve_pending_slot(capacity) {
                return Ok(V2ResidencySlot {
                    residency: self,
                    active: true,
                });
            }
            if !self
                .try_unload_one_resident(manager, membership, protected_thread_id)
                .await
            {
                return Err(CodexErr::new(CodexErrorDetails::AgentLimitReached {
                    max_threads: capacity,
                })
                .with_agent_context(AgentErrorContext::ResidencyCapacity));
            }
        }
    }

    fn try_reserve_pending_slot(&self, capacity: usize) -> bool {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.residents.len().saturating_add(state.pending_slots) >= capacity {
            return false;
        }
        state.pending_slots += 1;
        true
    }

    async fn try_unload_one_resident(
        self: &Arc<Self>,
        manager: &Arc<ThreadManagerState>,
        membership: &AgentTreeMembership,
        protected_thread_id: Option<ThreadId>,
    ) -> bool {
        // Keep shutting-down workers counted until removal. Each runtime's write guard
        // excludes delivery and competing evictions without blocking unrelated workers.
        let candidates = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .residents
            .clone();
        for candidate_thread_id in candidates {
            if Some(candidate_thread_id) == protected_thread_id {
                continue;
            }
            let candidate_thread = {
                let threads = manager.threads.read().await;
                match threads.get(&candidate_thread_id) {
                    Some(thread) if is_resident_candidate(thread) => Arc::clone(thread),
                    Some(_) | None => {
                        // A reload cannot publish between the lookup and stale-entry removal.
                        self.remove(candidate_thread_id);
                        return true;
                    }
                }
            };
            let Ok(residency_guard) =
                Arc::clone(&candidate_thread.residency_gate).try_write_owned()
            else {
                continue;
            };
            if !manager
                .get_thread(candidate_thread_id)
                .await
                .is_ok_and(|registered| Arc::ptr_eq(&registered, &candidate_thread))
                || !is_unloadable(candidate_thread.as_ref()).await
            {
                continue;
            }
            if finish_eviction(self.start_eviction(
                manager,
                membership,
                candidate_thread,
                residency_guard,
            ))
            .await
            {
                return true;
            }
        }
        false
    }

    fn start_eviction(
        self: &Arc<Self>,
        manager: &Arc<ThreadManagerState>,
        membership: &AgentTreeMembership,
        candidate_thread: Arc<CodexThread>,
        residency_guard: tokio::sync::OwnedRwLockWriteGuard<()>,
    ) -> tokio::task::JoinHandle<bool> {
        let candidate_thread_id = candidate_thread.session.thread_id;
        // Once shutdown is submitted, cancellation cannot revoke it. The eviction task
        // must keep delivery excluded and capacity reserved through registry removal.
        let manager = Arc::clone(manager);
        let residency = Arc::clone(self);
        let teardown = membership
            .clone()
            .into_teardown_guard("resident_eviction", Some(candidate_thread_id));
        tokio::spawn(async move {
            let _residency_guard = residency_guard;
            candidate_thread.ensure_rollout_materialized().await;
            if let Err(err) = candidate_thread.shutdown_and_wait().await {
                teardown.record_shutdown_failure("stop_resident", CodexErrKind::from(&err).into());
                warn!(
                    "failed to shut down v2 resident thread before unloading {candidate_thread_id}: {err}"
                );
                teardown.complete();
                return false;
            }
            // The submission loop has stopped and the residency guard excludes senders.
            // Preserve unread queue-only mail before dropping the session that held it.
            let mail = candidate_thread
                .session
                .input_queue
                .drain_mailbox()
                .await
                .into_iter()
                .map(|mail| mail.communication)
                .collect();
            // A concurrent tree shutdown deliberately discards its unread mail.
            let _ = candidate_thread
                .session
                .services
                .local_agent_runtime
                .mailboxes
                .enqueue(candidate_thread_id, /*id*/ None, mail);
            let environments = candidate_thread.environment_selections().await;
            let mut threads = manager.threads.write().await;
            if threads
                .get(&candidate_thread_id)
                .is_some_and(|registered| !Arc::ptr_eq(registered, &candidate_thread))
            {
                teardown.complete();
                return false;
            }
            candidate_thread
                .session
                .services
                .local_agent_runtime
                .registry
                .save_evicted_environments(candidate_thread_id, environments);
            // Keep publication excluded until both entries have been removed.
            threads.remove(&candidate_thread_id);
            residency.remove(candidate_thread_id);
            teardown.complete();
            true
        })
    }

    fn touch(&self, thread_id: ThreadId) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        touch_resident(&mut state.residents, thread_id);
        if let Some(timer) = state.unload_timers.remove(&thread_id) {
            timer.cancel();
        }
    }

    fn remove(&self, thread_id: ThreadId) {
        self.cancel_unload_timer(thread_id);
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .residents
            .retain(|resident_thread_id| *resident_thread_id != thread_id);
    }

    fn commit_slot(&self, thread_id: ThreadId) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.pending_slots = state.pending_slots.saturating_sub(1);
        touch_resident(&mut state.residents, thread_id);
    }

    fn release_pending_slot(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.pending_slots = state.pending_slots.saturating_sub(1);
    }
}

fn touch_resident(residents: &mut VecDeque<ThreadId>, thread_id: ThreadId) {
    residents.retain(|resident_thread_id| *resident_thread_id != thread_id);
    residents.push_back(thread_id);
}

async fn finish_eviction(eviction: tokio::task::JoinHandle<bool>) -> bool {
    match eviction.await {
        Ok(unloaded) => unloaded,
        Err(err) => {
            warn!("v2 resident eviction task failed: {err}");
            false
        }
    }
}

fn is_resident_candidate(thread: &CodexThread) -> bool {
    thread.multi_agent_version() == Some(MultiAgentVersion::V2)
        && is_v2_resident_session_source(&thread.session_source)
}

pub(super) fn is_v2_resident_session_source(session_source: &SessionSource) -> bool {
    matches!(session_source, SessionSource::SubAgent(_))
}

async fn is_unloadable(thread: &CodexThread) -> bool {
    matches!(
        thread.agent_status().await,
        AgentStatus::Completed(_) | AgentStatus::Errored(_) | AgentStatus::Interrupted
    ) && thread.session.active_turn.lock().await.is_none()
        && !thread.session.has_outstanding_durable_sleep()
        && !thread
            .session
            .input_queue
            .has_trigger_turn_mailbox_items()
            .await
}

async fn has_background_work(thread: &CodexThread) -> bool {
    let awaiting_elicitation = *thread.session.subscribe_elicitation_pause_state().borrow();
    awaiting_elicitation
        || thread.session.services.code_mode_service.has_active_cells()
        || !thread.session.list_background_terminals().await.is_empty()
}

#[cfg(test)]
#[path = "residency_tests.rs"]
mod tests;
