//! Torn-tail salvage, sidecar backups, and repair idempotence.
//!
//! Fixtures reproduce the 0.1.0-0.1.3 behavior: a torn final frame followed by
//! acknowledged-looking frames appended at the physical EOF.

use liven::storage::{RecoveryMode, StorageEngine};
use liven::types::DataValue;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;

const SEG: u64 = 1024 * 1024;
const STREAM: &str = "s";

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

fn frame_bytes(seq_id: u64, key: &str, value: &str) -> Vec<u8> {
    let value = DataValue::String(value.to_string());
    let mut payload = Vec::new();
    liven::storage::serialize_payload_into(STREAM, key, &value, &mut payload);
    let crc = crc32fast::hash(&payload);
    let mut frame = Vec::with_capacity(26 + payload.len());
    frame.extend_from_slice(&seq_id.to_be_bytes());
    frame.extend_from_slice(&now_ms().to_be_bytes());
    frame.push(value.type_tag());
    frame.push(0x01);
    frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    frame.extend_from_slice(&crc.to_be_bytes());
    frame.extend_from_slice(&payload);
    frame
}

fn segment_files(dir: &Path) -> Vec<PathBuf> {
    let mut files: Vec<PathBuf> = fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().and_then(|s| s.to_str()) == Some("liven"))
        .collect();
    files.sort();
    files
}

fn last_frame_geometry(path: &Path) -> (u64, u64) {
    let bytes = fs::read(path).unwrap();
    let mut offset = 0usize;
    let mut last = (0u64, 0u64);
    while offset + 26 <= bytes.len() {
        let payload_len = u32::from_be_bytes([
            bytes[offset + 18],
            bytes[offset + 19],
            bytes[offset + 20],
            bytes[offset + 21],
        ]) as usize;
        let frame_len = 26 + payload_len;
        if offset + frame_len > bytes.len() {
            break;
        }
        last = (offset as u64, frame_len as u64);
        offset += frame_len;
    }
    last
}

fn setup_prefix(dir: &Path, n: u64) {
    let engine = StorageEngine::new(dir, SEG).unwrap();
    for i in 0..n {
        engine
            .append(
                STREAM,
                &format!("k{i}"),
                DataValue::String(format!("v{i}")),
                false,
            )
            .unwrap();
    }
}

/// Tear the last frame roughly in half and append `m` valid frames at EOF,
/// reproducing the 0.1.3 append-after-tear shape.
fn make_stranded_fixture(dir: &Path, n: u64, m: u64) -> (PathBuf, u64, Vec<u8>) {
    setup_prefix(dir, n);
    let seg = segment_files(dir).pop().unwrap();
    let (start, len) = last_frame_geometry(&seg);
    let cut = len / 2;
    let original_len = fs::metadata(&seg).unwrap().len();
    OpenOptions::new()
        .write(true)
        .open(&seg)
        .unwrap()
        .set_len(original_len - cut)
        .unwrap();
    let tear_offset = start;

    let mut file = OpenOptions::new().append(true).open(&seg).unwrap();
    for i in 0..m {
        file.write_all(&frame_bytes(
            n + 1 + i,
            &format!("stranded{i}"),
            &format!("sv{i}"),
        ))
        .unwrap();
    }
    file.sync_all().unwrap();
    let torn_region = fs::read(&seg).unwrap()[tear_offset as usize..].to_vec();
    (seg, tear_offset, torn_region)
}

fn assert_all_present(dir: &Path, n: u64, m: u64) {
    let engine = StorageEngine::new(dir, SEG).unwrap();
    for i in 0..n.saturating_sub(1) {
        assert!(
            engine.get(STREAM, &format!("k{i}")).unwrap().is_some(),
            "prefix key k{i} lost"
        );
    }
    for i in 0..m {
        let rec = engine
            .get(STREAM, &format!("stranded{i}"))
            .unwrap()
            .unwrap_or_else(|| panic!("stranded{i} lost"));
        assert_eq!(
            rec.value,
            DataValue::String(format!("sv{i}")),
            "stranded{i} changed"
        );
    }
}

#[test]
fn stranded_frames_are_salvaged_by_repair() {
    let dir = tempfile::tempdir().unwrap();
    let (seg, tear_offset, torn_region) = make_stranded_fixture(dir.path(), 6, 4);

    // Default recovery must refuse to truncate stranded writes.
    let err = StorageEngine::new(dir.path(), SEG)
        .err()
        .expect("default recovery must fail");
    let msg = err.to_string();
    assert!(
        msg.contains("stranded"),
        "expected RecoveryRequired, got: {msg}"
    );

    let report = StorageEngine::repair(dir.path(), SEG, RecoveryMode::Repair).unwrap();
    assert_eq!(report.salvaged_frames, 4);
    assert_eq!(report.salvage_segments.len(), 1);

    // Sidecar is byte-identical to the removed region.
    let backup = dir.path().join(format!(
        "{}.torn-{}.bak",
        seg.file_name().unwrap().to_string_lossy(),
        tear_offset
    ));
    assert_eq!(fs::read(&backup).unwrap(), torn_region);

    // The source is now truncated to the last complete prefix frame.
    let (_, len) = last_frame_geometry(&seg);
    let prefix_end = last_frame_geometry(&seg).0 + len;
    assert_eq!(fs::metadata(&seg).unwrap().len(), prefix_end);

    assert_all_present(dir.path(), 6, 4);
}

#[test]
fn plain_torn_tail_starts_in_default_mode_with_sidecar() {
    let dir = tempfile::tempdir().unwrap();
    setup_prefix(dir.path(), 5);
    let seg = segment_files(dir.path()).pop().unwrap();
    let (start, len) = last_frame_geometry(&seg);
    let original_len = fs::metadata(&seg).unwrap().len();
    let cut = len / 2;
    OpenOptions::new()
        .write(true)
        .open(&seg)
        .unwrap()
        .set_len(original_len - cut)
        .unwrap();
    let torn_region = fs::read(&seg).unwrap()[(start as usize)..].to_vec();

    let engine = StorageEngine::new(dir.path(), SEG).unwrap();
    for i in 0..4 {
        assert!(engine.get(STREAM, &format!("k{i}")).unwrap().is_some());
    }
    drop(engine);

    let backup = dir.path().join(format!(
        "{}.torn-{}.bak",
        seg.file_name().unwrap().to_string_lossy(),
        start
    ));
    assert_eq!(fs::read(&backup).unwrap(), torn_region);
}

#[test]
fn salvage_is_idempotent() {
    let dir = tempfile::tempdir().unwrap();
    make_stranded_fixture(dir.path(), 6, 3);
    StorageEngine::repair(dir.path(), SEG, RecoveryMode::Repair).unwrap();

    let snapshot = |dir: &Path| {
        let mut v: Vec<(String, u64)> = fs::read_dir(dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| {
                (
                    e.file_name().to_string_lossy().to_string(),
                    e.metadata().unwrap().len(),
                )
            })
            .collect();
        v.sort();
        v
    };
    let before = snapshot(dir.path());
    let report = StorageEngine::repair(dir.path(), SEG, RecoveryMode::Repair).unwrap();
    assert_eq!(report.salvaged_frames, 0);
    assert_eq!(snapshot(dir.path()), before);
    assert_all_present(dir.path(), 6, 3);
}

#[test]
fn garbage_that_resembles_one_frame_is_not_a_resync_point() {
    let dir = tempfile::tempdir().unwrap();
    setup_prefix(dir.path(), 4);
    let seg = segment_files(dir.path()).pop().unwrap();

    // A header + payload that fails CRC, then bytes that are not a frame.
    let mut fake = frame_bytes(99, "phantom", "nope");
    fake[22] ^= 0xFF;
    let mut file = OpenOptions::new().append(true).open(&seg).unwrap();
    file.write_all(&fake).unwrap();
    file.write_all(&[0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x11, 0x22])
        .unwrap();
    file.sync_all().unwrap();

    // No valid frame at all: recovery truncates instead of resyncing.
    let engine = StorageEngine::new(dir.path(), SEG).unwrap();
    assert!(engine.get(STREAM, "phantom").unwrap().is_none());
    drop(engine);
}

#[test]
fn single_stranded_frame_at_eof_is_salvaged() {
    let dir = tempfile::tempdir().unwrap();
    let (_seg, _offset, _torn) = make_stranded_fixture(dir.path(), 5, 1);

    let err = StorageEngine::new(dir.path(), SEG)
        .err()
        .expect("one stranded frame must still be reported");
    assert!(err.to_string().contains("stranded"), "got: {err}");

    let report = StorageEngine::repair(dir.path(), SEG, RecoveryMode::Repair).unwrap();
    assert_eq!(report.salvaged_frames, 1);
    assert_all_present(dir.path(), 5, 1);
}

#[test]
fn last_stranded_frame_may_itself_be_torn() {
    let dir = tempfile::tempdir().unwrap();
    let (seg, _offset, _torn) = make_stranded_fixture(dir.path(), 5, 1);

    // Append a second stranded frame, then tear it in half.
    let second = frame_bytes(7, "stranded1", &"y".repeat(200));
    let mut file = OpenOptions::new().append(true).open(&seg).unwrap();
    file.write_all(&second).unwrap();
    file.sync_all().unwrap();
    let len = fs::metadata(&seg).unwrap().len();
    OpenOptions::new()
        .write(true)
        .open(&seg)
        .unwrap()
        .set_len(len - second.len() as u64 / 2)
        .unwrap();

    let err = StorageEngine::new(dir.path(), SEG)
        .err()
        .expect("valid frames before a torn last frame must be salvaged");
    let msg = err.to_string();
    assert!(msg.contains("stranded"), "got: {msg}");
    assert!(
        msg.contains("s:stranded1"),
        "error should name the torn key: {msg}"
    );

    let report = StorageEngine::repair(dir.path(), SEG, RecoveryMode::Repair).unwrap();
    assert_eq!(report.salvaged_frames, 1);
    let engine = StorageEngine::new(dir.path(), SEG).unwrap();
    for i in 0..4 {
        assert!(engine.get(STREAM, &format!("k{i}")).unwrap().is_some());
    }
    assert!(engine.get(STREAM, "stranded0").unwrap().is_some());
    assert!(engine.get(STREAM, "stranded1").unwrap().is_none());
}

#[test]
fn recovery_required_names_unrecoverable_keys() {
    let dir = tempfile::tempdir().unwrap();
    // A long value lets the tear fall inside the value, leaving the key intact.
    let engine = StorageEngine::new(dir.path(), SEG).unwrap();
    for i in 0..5 {
        engine
            .append(
                STREAM,
                &format!("k{i}"),
                DataValue::String("v".into()),
                false,
            )
            .unwrap();
    }
    engine
        .append(STREAM, "k5", DataValue::String("x".repeat(200)), false)
        .unwrap();
    drop(engine);

    let seg = segment_files(dir.path()).pop().unwrap();
    let (_, len) = last_frame_geometry(&seg);
    let original_len = fs::metadata(&seg).unwrap().len();
    OpenOptions::new()
        .write(true)
        .open(&seg)
        .unwrap()
        .set_len(original_len - len / 4)
        .unwrap();
    let mut file = OpenOptions::new().append(true).open(&seg).unwrap();
    for i in 0..2 {
        file.write_all(&frame_bytes(100 + i, &format!("stranded{i}"), "sv"))
            .unwrap();
    }
    file.sync_all().unwrap();

    let err = StorageEngine::new(dir.path(), SEG)
        .err()
        .expect("default recovery must fail");
    let msg = err.to_string();
    assert!(
        msg.contains("s:k5"),
        "error should name the torn key: {msg}"
    );
}

#[test]
fn sidecar_and_repair_files_are_ignored_by_recovery() {
    let dir = tempfile::tempdir().unwrap();
    setup_prefix(dir.path(), 3);
    let seg = segment_files(dir.path()).pop().unwrap();
    let name = seg.file_name().unwrap().to_string_lossy().to_string();
    fs::write(
        dir.path().join(format!("{name}.torn-0.bak")),
        b"not a segment",
    )
    .unwrap();
    fs::write(
        dir.path().join(format!("{name}.repair-123")),
        b"not a segment",
    )
    .unwrap();
    fs::write(dir.path().join(format!("{name}.salvage-0")), b"3 12\n").unwrap();

    let engine = StorageEngine::new(dir.path(), SEG).unwrap();
    for i in 0..3 {
        assert!(engine.get(STREAM, &format!("k{i}")).unwrap().is_some());
    }
}

#[test]
fn post_repair_ids_stay_in_the_normal_range_and_repair_is_clean() {
    let dir = tempfile::tempdir().unwrap();
    make_stranded_fixture(dir.path(), 5, 2);
    let report = StorageEngine::repair(dir.path(), 120, RecoveryMode::Repair).unwrap();
    let salvage_id = report.salvage_segments[0].0;
    assert!(salvage_id < u64::MAX / 2);

    // A default-mode start is clean, and a rollover picks an ID above the
    // salvage segment without entering the compaction range.
    let engine = StorageEngine::new(dir.path(), 120).unwrap();
    for i in 0..6 {
        engine
            .append(
                STREAM,
                &format!("post{i}"),
                DataValue::String("x".into()),
                false,
            )
            .unwrap();
    }
    let active = engine.active_segment_id();
    assert!(active < u64::MAX / 2);
    assert!(
        active > salvage_id,
        "active {active} must follow salvage {salvage_id}"
    );
    drop(engine);

    let engine = StorageEngine::new(dir.path(), 120).unwrap();
    for i in 0..6 {
        assert!(engine.get(STREAM, &format!("post{i}")).unwrap().is_some());
    }
    drop(engine);

    let again = StorageEngine::repair(dir.path(), 120, RecoveryMode::Repair).unwrap();
    assert_eq!(again.salvaged_frames, 0);
}

#[test]
fn inactive_segment_stranded_frames_are_salvaged() {
    let dir = tempfile::tempdir().unwrap();
    // Small segments force k0.. into segment 1 and the rest into segment 2.
    {
        let engine = StorageEngine::new(dir.path(), 120).unwrap();
        for i in 0..6 {
            engine
                .append(
                    STREAM,
                    &format!("k{i}"),
                    DataValue::String("v".into()),
                    false,
                )
                .unwrap();
        }
    }
    let segs = segment_files(dir.path());
    assert!(segs.len() >= 2, "need an inactive segment");
    let seg = segs.first().unwrap().clone();
    let (_, len) = last_frame_geometry(&seg);
    let original_len = fs::metadata(&seg).unwrap().len();
    let cut = len / 2;
    OpenOptions::new()
        .write(true)
        .open(&seg)
        .unwrap()
        .set_len(original_len - cut)
        .unwrap();
    let mut file = OpenOptions::new().append(true).open(&seg).unwrap();
    for i in 0..3 {
        file.write_all(&frame_bytes(500 + i, &format!("inactive{i}"), "iv"))
            .unwrap();
    }
    file.sync_all().unwrap();

    let err = StorageEngine::new(dir.path(), 120)
        .err()
        .expect("default recovery must fail");
    assert!(err.to_string().contains("stranded"), "got: {err}");

    let report = StorageEngine::repair(dir.path(), 120, RecoveryMode::Repair).unwrap();
    assert_eq!(report.salvaged_frames, 3);
    let engine = StorageEngine::new(dir.path(), 120).unwrap();
    // The torn frame was the last live record in the inactive segment.
    let complete = (0..6)
        .filter(|i| engine.get(STREAM, &format!("k{i}")).unwrap().is_some())
        .count();
    assert_eq!(complete, 5, "complete inactive prefix frames must survive");
    for i in 0..3 {
        assert!(
            engine
                .get(STREAM, &format!("inactive{i}"))
                .unwrap()
                .is_some(),
            "inactive stranded {i} lost"
        );
    }
}

#[test]
fn compacted_segment_stranded_frames_are_salvaged() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = StorageEngine::new(dir.path(), 120).unwrap();
        for i in 0..6 {
            engine
                .append(
                    STREAM,
                    &format!("k{i}"),
                    DataValue::String("v".into()),
                    false,
                )
                .unwrap();
        }
        engine.compact().unwrap();
    }
    let compacted = segment_files(dir.path())
        .into_iter()
        .find(|p| {
            p.file_stem()
                .and_then(|s| s.to_str())
                .and_then(|s| s.parse::<u64>().ok())
                .is_some_and(|id| id >= u64::MAX / 2)
        })
        .expect("compacted segment");
    let (_, len) = last_frame_geometry(&compacted);
    let original_len = fs::metadata(&compacted).unwrap().len();
    OpenOptions::new()
        .write(true)
        .open(&compacted)
        .unwrap()
        .set_len(original_len - len / 2)
        .unwrap();
    let mut file = OpenOptions::new().append(true).open(&compacted).unwrap();
    for i in 0..2 {
        file.write_all(&frame_bytes(900 + i, &format!("c{i}"), "cv"))
            .unwrap();
    }
    file.sync_all().unwrap();

    let err = StorageEngine::new(dir.path(), 120)
        .err()
        .expect("default recovery must fail");
    assert!(err.to_string().contains("stranded"), "got: {err}");

    let report = StorageEngine::repair(dir.path(), 120, RecoveryMode::Repair).unwrap();
    assert_eq!(report.salvaged_frames, 2);

    let engine = StorageEngine::new(dir.path(), 120).unwrap();
    // The torn frame was the last live record in the compacted segment and is
    // unrecoverable; every complete frame before it plus both stranded frames
    // must survive.
    let complete = (0..6)
        .filter(|i| engine.get(STREAM, &format!("k{i}")).unwrap().is_some())
        .count();
    assert_eq!(complete, 5, "complete compacted prefix frames must survive");
    assert!(engine.get(STREAM, "c0").unwrap().is_some());
    assert!(engine.get(STREAM, "c1").unwrap().is_some());
}

const V013_SEGMENT: &[u8] = include_bytes!("fixtures/v0.1.3-stranded/00000000000000000001.liven");

/// Real directory produced by the tagged v0.1.3 binary: six records, the last
/// frame physically torn, then three more records ingested after the tear.
#[test]
fn real_v013_stranded_directory_is_salvaged() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(dir.path().join("00000000000000000001.liven"), V013_SEGMENT).unwrap();

    let err = StorageEngine::new(dir.path(), SEG)
        .err()
        .expect("v0.1.3 stranded frames must be reported");
    assert!(err.to_string().contains("stranded"), "got: {err}");

    let report = StorageEngine::repair(dir.path(), SEG, RecoveryMode::Repair).unwrap();
    assert_eq!(report.salvaged_frames, 3);

    let engine = StorageEngine::new(dir.path(), SEG).unwrap();
    for i in 0..5 {
        assert!(engine.get(STREAM, &format!("k{i}")).unwrap().is_some());
    }
    assert!(engine.get(STREAM, "k5").unwrap().is_none());
    for i in 0..3 {
        assert!(
            engine
                .get(STREAM, &format!("stranded{i}"))
                .unwrap()
                .is_some(),
            "real v0.1.3 stranded{i} lost"
        );
    }
}

#[test]
fn repair_mid_file_crc_keeps_later_frames_and_backs_up() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = StorageEngine::new(dir.path(), 120).unwrap();
        for i in 0..8 {
            engine
                .append(
                    STREAM,
                    &format!("k{i}"),
                    DataValue::String("v".into()),
                    false,
                )
                .unwrap();
        }
    }
    let segs = segment_files(dir.path());
    assert!(segs.len() >= 2, "need an inactive segment");
    let seg = segs.first().unwrap().clone();

    // Corrupt the first frame's CRC but leave later frames in the file intact.
    let mut data = fs::read(&seg).unwrap();
    data[22] ^= 0xFF;
    fs::write(&seg, &data).unwrap();
    let corrupted = fs::read(&seg).unwrap();

    let err = StorageEngine::new(dir.path(), 120)
        .err()
        .expect("valid frames after a mid-file CRC error must require repair");
    assert!(err.to_string().contains("stranded"), "got: {err}");

    let report = StorageEngine::repair(dir.path(), 120, RecoveryMode::Repair).unwrap();
    assert!(report.salvaged_frames > 0);
    assert!(!report.torn_backups.is_empty());
    assert!(!report.repair_backups.is_empty());

    // The sidecar is the whole corrupted region (the tear is at offset 0).
    let sidecar = fs::read(&report.torn_backups[0]).unwrap();
    assert_eq!(sidecar, corrupted);
    let full_backup = fs::read(&report.repair_backups[0]).unwrap();
    assert_eq!(full_backup, corrupted);

    // Later valid frames were kept; only the corrupted frame's key is gone.
    let engine = StorageEngine::new(dir.path(), 120).unwrap();
    for i in 1..8 {
        assert!(
            engine.get(STREAM, &format!("k{i}")).unwrap().is_some(),
            "k{i} lost after mid-file CRC repair"
        );
    }
}

// ── Crash during repair, at each durable step ────────────────────────────────

fn repair_child() {
    let Some(dir) = std::env::var_os("LIVEN_SALVAGE_DIR") else {
        return;
    };
    let _ = StorageEngine::repair(PathBuf::from(dir), SEG, RecoveryMode::Repair);
    std::process::exit(0);
}

#[test]
fn salvage_repair_child() {
    repair_child();
}

fn spawn_crash_child(dir: &Path, point: &str) {
    let exe = std::env::current_exe().unwrap();
    let status = Command::new(exe)
        .arg("--exact")
        .arg("salvage_repair_child")
        .env("LIVEN_SALVAGE_DIR", dir)
        .env("LIVEN_TEST_CRASH_POINT", point)
        .status()
        .expect("spawn repair child");
    assert!(!status.success(), "crash point {point} was not reached");
}

#[test]
fn repair_orders_salvage_dir_sidecar_before_truncate() {
    let has_sidecar = |dir: &Path| {
        fs::read_dir(dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .any(|e| e.file_name().to_string_lossy().contains(".torn-"))
    };
    let segment_count = |dir: &Path| segment_files(dir).len();

    // 1. After salvage + directory fsync, the salvage segment exists and the
    //    sidecar does not; the source is still torn.
    let dir = tempfile::tempdir().unwrap();
    let (seg, _offset, _torn) = make_stranded_fixture(dir.path(), 5, 3);
    let original_len = fs::metadata(&seg).unwrap().len();
    spawn_crash_child(dir.path(), "repair_after_salvage_dir_sync");
    assert!(segment_count(dir.path()) >= 2, "salvage segment missing");
    assert!(!has_sidecar(dir.path()), "sidecar written before salvage");
    assert_eq!(fs::metadata(&seg).unwrap().len(), original_len);

    // 2. After the sidecar (and its directory fsync), both exist and the
    //    source is still intact.
    let dir = tempfile::tempdir().unwrap();
    let (seg, _offset, _torn) = make_stranded_fixture(dir.path(), 5, 3);
    let original_len = fs::metadata(&seg).unwrap().len();
    spawn_crash_child(dir.path(), "repair_after_backup");
    assert!(segment_count(dir.path()) >= 2, "salvage segment missing");
    assert!(has_sidecar(dir.path()), "sidecar missing");
    assert_eq!(fs::metadata(&seg).unwrap().len(), original_len);

    // 3. Only after both are durable is the source truncated.
    let dir = tempfile::tempdir().unwrap();
    let (seg, _offset, _torn) = make_stranded_fixture(dir.path(), 5, 3);
    let original_len = fs::metadata(&seg).unwrap().len();
    spawn_crash_child(dir.path(), "repair_after_truncate");
    assert!(segment_count(dir.path()) >= 2);
    assert!(has_sidecar(dir.path()));
    assert!(
        fs::metadata(&seg).unwrap().len() < original_len,
        "source was not truncated after salvage + sidecar"
    );
}

#[test]
fn repair_recoverable_after_crash_at_each_step() {
    for point in [
        "repair_after_backup",
        "repair_after_salvage",
        "repair_after_truncate",
    ] {
        let dir = tempfile::tempdir().unwrap();
        make_stranded_fixture(dir.path(), 6, 3);
        spawn_crash_child(dir.path(), point);

        // Rerun repair; every acknowledged stranded write must survive exactly once.
        let report = StorageEngine::repair(dir.path(), SEG, RecoveryMode::Repair).unwrap();
        let expected = if point == "repair_after_truncate" {
            0
        } else {
            3
        };
        assert_eq!(
            report.salvaged_frames, expected,
            "point {point}: unexpected rerun salvage count, got {report:?}"
        );
        assert_all_present(dir.path(), 6, 3);

        // A second repair must be a no-op.
        let again = StorageEngine::repair(dir.path(), SEG, RecoveryMode::Repair).unwrap();
        assert_eq!(
            again.salvaged_frames, 0,
            "point {point}: repair not idempotent"
        );
    }
}
