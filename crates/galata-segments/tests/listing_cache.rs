//! A quiet directory is listed once, and a moved one is always listed again.
//!
//! A listing never opens a file, so these trees hold empty files with segment
//! names. Directory mtimes are set by hand, so that "long ago" and "within the
//! racy margin" are facts of the test, not of how fast it ran.

use std::fs::{self, File};
use std::path::Path;
use std::time::{Duration, SystemTime};

use galata_segments::{ListingCache, list_segments, partitions};

fn touch(path: &Path) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    File::create(path).unwrap();
}

fn age(dir: &Path, ago: Duration) {
    File::open(dir)
        .unwrap()
        .set_modified(SystemTime::now() - ago)
        .unwrap();
}

/// `kind=x/date=…` with two old partitions, a newer root, and a stray file.
fn tree() -> tempfile::TempDir {
    let t = tempfile::tempdir().unwrap();
    let root = t.path();
    touch(&root.join("date=2020-01-01/s-1_2.parquet"));
    touch(&root.join("date=2020-01-01/s-3_4.parquet"));
    touch(&root.join("date=2020-01-02/s-5_6.parquet"));
    touch(&root.join("date=2020-01-02/.tmp-s-7_8.parquet"));
    touch(&root.join("date=2020-01-03/nested=a/s-9_10.parquet"));
    touch(&root.join("notes.txt"));
    for d in [
        "date=2020-01-01",
        "date=2020-01-02",
        "date=2020-01-03/nested=a",
        "date=2020-01-03",
    ] {
        age(&root.join(d), Duration::from_secs(3_600));
    }
    age(root, Duration::from_secs(60));
    t
}

fn uncached(root: &Path) -> Vec<(std::path::PathBuf, galata_segments::SharedSegments)> {
    partitions(root)
        .into_iter()
        .map(|p| {
            let s = list_segments(&p).into();
            (p, s)
        })
        .filter(|(_, s): &(_, galata_segments::SharedSegments)| !s.is_empty())
        .collect()
}

#[test]
fn a_cached_listing_equals_the_uncached_one() {
    let t = tree();
    let cache = ListingCache::default();
    let first = cache.partitions_with_segments(t.path());
    assert_eq!(first, uncached(t.path()));
    assert_eq!(first.len(), 3, "three partitions hold a segment");
    assert_eq!(
        cache.partitions_with_segments(t.path()),
        first,
        "warm equals cold"
    );
}

#[test]
fn a_quiet_directory_is_not_read_twice() {
    let t = tree();
    let cache = ListingCache::default();
    cache.partitions_with_segments(t.path());
    let cold = cache.directory_reads();
    assert_eq!(cold, 5, "root, three dates, one nested");
    cache.partitions_with_segments(t.path());
    // Only the root is re-read: it is the newest directory in the walk, so it
    // sits inside the racy margin. The hour-old partitions are not.
    assert_eq!(cache.directory_reads() - cold, 1);
}

#[test]
fn a_segment_renamed_into_an_old_partition_is_seen() {
    let t = tree();
    let cache = ListingCache::default();
    cache.partitions_with_segments(t.path());
    cache.partitions_with_segments(t.path());
    let staged = t.path().join("date=2020-01-01/.tmp-s-50_60.parquet");
    touch(&staged);
    fs::rename(&staged, t.path().join("date=2020-01-01/s-50_60.parquet")).unwrap();
    let after = cache.partitions_with_segments(t.path());
    assert_eq!(after, uncached(t.path()));
    assert_eq!(after[0].1.len(), 3, "the renamed segment is listed");
}

#[test]
fn a_removed_segment_is_gone() {
    let t = tree();
    let cache = ListingCache::default();
    cache.partitions_with_segments(t.path());
    cache.partitions_with_segments(t.path());
    fs::remove_file(t.path().join("date=2020-01-02/s-5_6.parquet")).unwrap();
    let after = cache.partitions_with_segments(t.path());
    assert_eq!(after, uncached(t.path()));
    assert!(after.iter().all(|(p, _)| !p.ends_with("date=2020-01-02")));
}

#[test]
fn a_directory_inside_the_margin_is_read_again() {
    let t = tree();
    // One old partition's mtime one second behind the newest: inside the margin.
    age(t.path(), Duration::from_secs(60));
    age(&t.path().join("date=2020-01-02"), Duration::from_secs(61));
    let cache = ListingCache::default();
    cache.partitions_with_segments(t.path());
    let cold = cache.directory_reads();
    cache.partitions_with_segments(t.path());
    // The root and the partition within 2 s of it are both re-read.
    assert_eq!(cache.directory_reads() - cold, 2);
}

#[test]
fn prune_forgets_a_directory_that_is_gone() {
    let t = tree();
    let cache = ListingCache::default();
    cache.partitions_with_segments(t.path());
    fs::remove_dir_all(t.path().join("date=2020-01-03")).unwrap();
    cache.prune();
    assert_eq!(cache.partitions_with_segments(t.path()), uncached(t.path()));
}

#[test]
fn one_walk_lists_what_partitions_and_list_segments_list() {
    let t = tree();
    let listed: Vec<_> = galata_segments::partitions_listed(t.path())
        .into_iter()
        .map(|(p, s)| (p, galata_segments::SharedSegments::from(s)))
        .collect();
    assert_eq!(listed, uncached(t.path()));
}

#[test]
fn a_pruned_walk_lists_only_the_directories_it_descends() {
    let t = tree();
    let one_day = |name: &str| !name.starts_with("date=") || name == "date=2020-01-02";
    let listed = galata_segments::partitions_listed_where(t.path(), &one_day);
    let dirs: Vec<_> = listed.iter().map(|(d, _)| d.clone()).collect();
    assert_eq!(dirs, vec![t.path().join("date=2020-01-02")]);
    assert_eq!(listed[0].1.len(), 1, "the partial write is not a segment");
}

#[test]
fn the_newest_position_of_a_subtree_is_read_from_the_listing() {
    let t = tree();
    let listing = galata_segments::partitions_listed(t.path());
    assert_eq!(
        galata_segments::last_durable_in(&listing, t.path()),
        galata_segments::last_durable(t.path())
    );
    assert_eq!(
        galata_segments::last_durable_in(&listing, &t.path().join("date=2020-01-01")),
        galata_segments::last_durable(&t.path().join("date=2020-01-01"))
    );
    assert_eq!(
        galata_segments::last_durable_in(&listing, &t.path().join("date=1999-01-01")),
        None
    );
}
