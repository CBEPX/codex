use crate::StartThreadOptions;
use crate::ThreadManager;
use crate::agent::LocalAgentControl;
use crate::codex_thread::CodexThread;
use crate::config::Config;
use crate::config::test_config;
use crate::thread_manager::ThreadManagerState;
use codex_features::Feature;
use codex_login::CodexAuth;
use codex_protocol::ThreadId;
use codex_protocol::error::CodexErrorDetails;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::SubAgentSource;
use codex_protocol::protocol::ThreadSource;
use codex_protocol::protocol::TurnAbortReason;
use codex_protocol::protocol::TurnAbortedEvent;
use codex_protocol::protocol::TurnCompleteEvent;
use pretty_assertions::assert_eq;
use std::sync::Arc;
use std::time::Duration;

#[test]
fn completed_agent_unload_pin_during_startup_does_not_count_reservation_twice() {
    let residency = Arc::new(super::V2Residency::default());
    let first_id = ThreadId::new();
    assert!(residency.try_reserve_pending_slot(2));
    let first = super::V2ResidencySlot {
        residency: Arc::clone(&residency),
        active: true,
    };
    // Initial input pins the published thread before spawn commits its reserved slot.
    residency.touch(first_id);
    assert!(
        residency.try_reserve_pending_slot(2),
        "a pending startup must use exactly one slot"
    );
    let second = super::V2ResidencySlot {
        residency: Arc::clone(&residency),
        active: true,
    };
    first.commit(first_id);
    assert!(!residency.try_reserve_pending_slot(2));
    drop(second);
    assert!(residency.try_reserve_pending_slot(2));
}

#[tokio::test]
async fn completed_agent_unload_timer_does_not_retain_session_or_delay_tree_shutdown() {
    let (session, turn) = crate::session::tests::make_session_and_context().await;
    let session = Arc::new(session);
    let weak_session = Arc::downgrade(&session);
    let runtime = session.services.local_agent_runtime.clone();
    runtime.schedule_completed_agent_unload(&session, 60);
    drop(turn);
    drop(session);
    assert!(
        weak_session.upgrade().is_none(),
        "pending timer must not own the session"
    );
    tokio::time::timeout(Duration::from_secs(1), runtime.request_shutdown().wait())
        .await
        .expect("pending timer must leave the tree promptly")
        .expect("tree shutdown");
}

#[tokio::test]
async fn completed_agent_unload_replaces_deadline_and_turn_admission_cancels_it() {
    let (session, _) = crate::session::tests::make_session_and_context().await;
    let session = Arc::new(session);
    let runtime = &session.services.local_agent_runtime;
    runtime.schedule_completed_agent_unload(&session, 60);
    let first =
        Arc::clone(&runtime.residency.state.lock().unwrap().unload_timers[&session.thread_id]);
    runtime.schedule_completed_agent_unload(&session, 120);
    let second =
        Arc::clone(&runtime.residency.state.lock().unwrap().unload_timers[&session.thread_id]);
    assert!(first.is_cancelled());
    assert!(!second.is_cancelled());
    assert!(!Arc::ptr_eq(&first, &second));
    session.record_started_turn("new-turn", None).await;
    assert!(second.is_cancelled());
    assert!(
        runtime
            .residency
            .state
            .lock()
            .unwrap()
            .unload_timers
            .is_empty()
    );
    tokio::time::timeout(Duration::from_secs(1), runtime.request_shutdown().wait())
        .await
        .expect("cancelled deadlines must leave the tree")
        .expect("tree shutdown");
}

#[tokio::test]
async fn completed_agent_unload_skips_unrepresentable_deadline() {
    let (session, _) = crate::session::tests::make_session_and_context().await;
    let session = Arc::new(session);
    let runtime = &session.services.local_agent_runtime;
    runtime.schedule_completed_agent_unload(&session, u64::MAX);
    assert!(
        runtime
            .residency
            .state
            .lock()
            .unwrap()
            .unload_timers
            .is_empty()
    );
}

#[tokio::test]
async fn residency_slot_reservation_unloads_oldest_idle_v2_agent() {
    let mut config = test_config().await;
    let _ = config.features.enable(Feature::MultiAgentV2);
    config.multi_agent_v2.max_concurrent_threads_per_session = 2;
    let temp_home = tempfile::tempdir().expect("create temp home");
    config.codex_home = temp_home.path().to_path_buf().try_into().unwrap();
    config.cwd = temp_home.path().to_path_buf().try_into().unwrap();
    let manager = ThreadManager::with_models_provider_and_home_for_tests(
        CodexAuth::from_api_key("dummy"),
        config.model_provider.clone(),
        config.codex_home.to_path_buf(),
        Arc::new(codex_exec_server::EnvironmentManager::default_for_tests()),
    );
    let root = manager
        .start_thread(StartThreadOptions::new(config.clone()))
        .await
        .expect("start root thread");
    let control = manager.agent_control();
    let state = control
        .runtime
        .upgrade()
        .expect("thread manager should be live");
    let membership = control.runtime.admit_start().expect("admit residency work");

    let first_slot = control
        .reserve_v2_residency_slot(
            &state,
            &config,
            &membership,
            /*protected_thread_id*/ None,
        )
        .await
        .expect("first resident slot");
    let first =
        spawn_v2_subagent(&control, &state, config.clone(), root.thread_id, "worker-1").await;
    first_slot.commit(first.thread_id);
    mark_thread_completed(first.thread.as_ref()).await;

    let second_slot = control
        .reserve_v2_residency_slot(
            &state,
            &config,
            &membership,
            /*protected_thread_id*/ None,
        )
        .await
        .expect("second resident slot should evict the first idle agent");
    match manager.get_thread(first.thread_id).await {
        Err(err) => match err.details() {
            CodexErrorDetails::ThreadNotFound(thread_id) => assert_eq!(*thread_id, first.thread_id),
            _ => panic!("expected evicted thread to be missing, got {err:?}"),
        },
        Ok(_) => panic!("expected evicted thread to be missing"),
    }
    let second = spawn_v2_subagent(&control, &state, config, root.thread_id, "worker-2").await;
    second_slot.commit(second.thread_id);

    assert!(manager.get_thread(root.thread_id).await.is_ok());
    assert!(manager.get_thread(second.thread_id).await.is_ok());
}

#[tokio::test]
async fn interrupted_v2_agent_is_lost_after_residency_eviction() {
    let mut config = test_config().await;
    let _ = config.features.enable(Feature::MultiAgentV2);
    config.multi_agent_v2.max_concurrent_threads_per_session = 2;
    let temp_home = tempfile::tempdir().expect("create temp home");
    config.codex_home = temp_home.path().to_path_buf().try_into().unwrap();
    config.cwd = temp_home.path().to_path_buf().try_into().unwrap();
    let manager = ThreadManager::with_models_provider_and_home_for_tests(
        CodexAuth::from_api_key("dummy"),
        config.model_provider.clone(),
        config.codex_home.to_path_buf(),
        Arc::new(codex_exec_server::EnvironmentManager::default_for_tests()),
    );
    let root = manager
        .start_thread(StartThreadOptions::new(config.clone()))
        .await
        .expect("start root thread");
    let control = manager.agent_control();
    let state = control
        .runtime
        .upgrade()
        .expect("thread manager should be live");
    let membership = control.runtime.admit_start().expect("admit residency work");

    let first_slot = control
        .reserve_v2_residency_slot(
            &state,
            &config,
            &membership,
            /*protected_thread_id*/ None,
        )
        .await
        .expect("first resident slot");
    let first =
        spawn_v2_subagent(&control, &state, config.clone(), root.thread_id, "worker-1").await;
    first_slot.commit(first.thread_id);
    mark_thread_interrupted(first.thread.as_ref()).await;

    let second_slot = control
        .reserve_v2_residency_slot(
            &state,
            &config,
            &membership,
            /*protected_thread_id*/ None,
        )
        .await
        .expect("second resident slot should evict the first interrupted idle agent");
    match manager.get_thread(first.thread_id).await {
        Err(err) => match err.details() {
            CodexErrorDetails::ThreadNotFound(thread_id) => assert_eq!(*thread_id, first.thread_id),
            _ => panic!("expected evicted thread to be missing, got {err:?}"),
        },
        Ok(_) => panic!("expected evicted thread to be missing"),
    }
    let second =
        spawn_v2_subagent(&control, &state, config.clone(), root.thread_id, "worker-2").await;
    second_slot.commit(second.thread_id);
    mark_thread_completed(second.thread.as_ref()).await;

    let err = control
        .ensure_v2_agent_loaded(config, first.thread_id, /*parent*/ None)
        .await
        .expect_err("evicted interrupted agent should stay lost");
    match err.details() {
        CodexErrorDetails::ThreadNotFound(thread_id) => assert_eq!(*thread_id, first.thread_id),
        _ => panic!("expected ThreadNotFound, got {err:?}"),
    }

    assert!(manager.get_thread(root.thread_id).await.is_ok());
    assert!(manager.get_thread(second.thread_id).await.is_ok());
    match manager.get_thread(first.thread_id).await {
        Err(err) => match err.details() {
            CodexErrorDetails::ThreadNotFound(thread_id) => assert_eq!(*thread_id, first.thread_id),
            _ => panic!("expected evicted thread to be missing, got {err:?}"),
        },
        Ok(_) => panic!("expected evicted thread to be missing"),
    }
}

async fn spawn_v2_subagent(
    control: &LocalAgentControl,
    state: &Arc<ThreadManagerState>,
    config: Config,
    parent_thread_id: ThreadId,
    label: &str,
) -> crate::thread_manager::NewThread {
    state
        .spawn_child_thread(
            crate::thread_manager::StartThreadOptions {
                session_source: Some(SessionSource::SubAgent(SubAgentSource::Other(
                    label.to_string(),
                ))),
                thread_source: Some(ThreadSource::Subagent),
                ..crate::thread_manager::StartThreadOptions::new(config)
            },
            control.clone(),
            Some(parent_thread_id),
            /*inherited_exec_policy*/ None,
        )
        .await
        .expect("spawn v2 subagent")
}

async fn mark_thread_completed(thread: &CodexThread) {
    let turn = thread.session.new_default_turn().await;
    thread
        .session
        .send_event(
            turn.as_ref(),
            EventMsg::TurnComplete(TurnCompleteEvent {
                root_turn_id: None,
                turn_id: turn.sub_id.clone(),
                started_at: None,
                last_agent_message: Some("done".to_string()),
                error: None,
                completed_at: None,
                duration_ms: None,
                time_to_first_token_ms: None,
            }),
        )
        .await;
    clear_active_turn(thread).await;
}

async fn mark_thread_interrupted(thread: &CodexThread) {
    let turn = thread.session.new_default_turn().await;
    thread
        .session
        .send_event(
            turn.as_ref(),
            EventMsg::TurnAborted(TurnAbortedEvent {
                root_turn_id: None,
                turn_id: Some(turn.sub_id.clone()),
                started_at: None,
                reason: TurnAbortReason::Interrupted,
                error: None,
                completed_at: None,
                duration_ms: None,
            }),
        )
        .await;
    clear_active_turn(thread).await;
}

async fn clear_active_turn(thread: &CodexThread) {
    // The fixture has no task runner to clear the turn after the terminal event.
    *thread.session.active_turn.lock().await = None;
}
