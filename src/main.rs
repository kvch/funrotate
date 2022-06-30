use chrono::{DateTime, Duration, NaiveDate, NaiveDateTime, Utc};
use csv;
use glob::glob;
use log::{debug, error, info};
use phf::phf_map;
use serde::{Deserialize, Serialize};
use simplelog::{ColorChoice, Config, LevelFilter, TermLogger, TerminalMode};
use std::io::prelude::*;
use std::io::{BufReader, Read};
use std::{collections::HashMap, error::Error, fs, process};

// CONFIG_FILENAME is the filename of the configuration
const CONFIG_FILENAME: &str = "funrotate.toml";
// ROTATION_INFO_FILENAME is the name of the file that contains the
// last time each file was rotated.
const ROTATION_INFO_FILENAME: &str = ".last_rotation";
// ROTATION_TIME_FORMAT is the format string of the timestamps
const ROTATION_TIME_FORMAT: &str = "%Y-%m-%d %H:%M";
// INTERVAL contains the available rotation intervals.
const INTERVALS: phf::Map<&'static str, RotationInterval> = phf_map! {
    "hourly" => RotationInterval::Hourly,
    "daily" => RotationInterval::Daily,
    "weekly" => RotationInterval::Weekly,
    "monthly" => RotationInterval::Monthly,
};
// STRATEGY contains the available rotation strategies.
const STRATEGY: phf::Map<&'static str, RotationStrategy> = phf_map! {
    "copy" => RotationStrategy::CopyTruncate,
    "copytruncate" => RotationStrategy::CopyTruncate,
    "create" => RotationStrategy::CopyTruncate,
    "nocopytruncate" => RotationStrategy::Copy,
};

#[derive(Deserialize, Debug)]
struct Conf {
    files: Vec<RotatedFile>,
}

#[derive(Clone)]
enum RotationInterval {
    Hourly,
    Daily,
    Weekly,
    Monthly,
}

#[derive(Clone)]
enum RotationStrategy {
    CopyTruncate,
    Copy,
}

#[derive(Deserialize, Debug)]
struct RotatedFile {
    path: String,
    interval: String,
    strategy: String,
    max_files: usize,
    compress: bool,
    size: u64,
}

#[derive(Deserialize, Serialize, Debug)]
struct LastRotation {
    path: String,
    last_rotation: String,
}

// RotationRecorder is the data structure that maintains rotation
// times. It also reads and saves them to disk.
struct RotationRecorder {
    entries: HashMap<String, String>,
    updated: bool,
}

impl RotationRecorder {
    // new opens the path under ROTATION_INFO_FILENAME and parses its contents.
    // Then it returns a RotationRecorder instance, so we can use it to get rotation
    // time of files.
    fn new() -> Result<RotationRecorder, Box<dyn Error>> {
        debug!(
            "reading information about last rotation from {}",
            ROTATION_INFO_FILENAME
        );

        let mut rdr = csv::ReaderBuilder::new().from_path(ROTATION_INFO_FILENAME)?;

        let records = rdr
            .records()
            .collect::<Result<Vec<csv::StringRecord>, csv::Error>>()?;

        let mut entries: HashMap<String, String> = HashMap::new();
        for record in records {
            let last_rotation: LastRotation = record.deserialize(None)?;
            entries.insert(last_rotation.path, last_rotation.last_rotation);
        }
        let recorder = RotationRecorder {
            entries: entries,
            updated: false,
        };

        return Ok(recorder);
    }

    // last_rotation_time returns the last time the file
    // under the given path was rotated.
    fn last_rotation_time(&self, path: &String) -> NaiveDateTime {
        let last_rotation = self.entries.get(path);
        return match last_rotation {
            Some(time) => NaiveDateTime::parse_from_str(time, ROTATION_TIME_FORMAT).unwrap(),
            None => NaiveDate::from_ymd(1999, 1, 1).and_hms(1, 2, 3),
        };
    }

    // update_rotation_time updates the rotation time of a given path.
    fn update_rotation_time(&mut self, path: String, rotation_time: DateTime<Utc>) {
        self.entries
            .insert(path, rotation_time.format(ROTATION_TIME_FORMAT).to_string());
        self.updated = true;
    }

    // save persists the last rotation times if they were updated.
    fn save(&self) -> Result<(), Box<dyn Error>> {
        if self.updated {
            return Ok(());
        }

        let mut wtr = csv::WriterBuilder::new().from_path(ROTATION_INFO_FILENAME)?;
        for (path, rotation_time) in &self.entries {
            wtr.serialize(LastRotation {
                path: path.to_string(),
                last_rotation: rotation_time.to_string(),
            })?;
        }
        wtr.flush()?;
        Ok(())
    }
}

fn main() {
    setup_logging();

    let now = Utc::now();
    debug!("starting log rotation at {:?}", now);

    let rotate_config = get_config().unwrap_or_else(|err| {
        error!("cannot load configuration: {}", err);
        process::exit(1);
    });

    let mut last_rotation_info = RotationRecorder::new().unwrap_or_else(|err| {
        error!("cannot load info about last rotation: {}", err);
        process::exit(1);
    });

    for file in rotate_config.files {
        if is_rotation_triggered(&last_rotation_info, &file) {
            rotate(&mut last_rotation_info, file);
        }
    }

    last_rotation_info.save().unwrap_or_else(|err| {
        error!("failed to persist rotation times: {}", err);
        process::exit(1);
    });

    debug!("rotation done");
}

fn setup_logging() {
    TermLogger::init(
        LevelFilter::Debug,
        Config::default(),
        TerminalMode::Mixed,
        ColorChoice::Auto,
    )
    .unwrap();
}

// get_config reads the configuration from the file under CONFIG_FILENAME
fn get_config() -> Result<Conf, toml::de::Error> {
    debug!("reading config from {}", CONFIG_FILENAME);

    let config_contents =
        fs::read_to_string(CONFIG_FILENAME).expect("cannot read configuration file");

    debug!("configuration loaded from {}", CONFIG_FILENAME);

    return toml::from_str(&config_contents);
}

// is_rotation_triggered checks if a file has to be rotated either because of
// its size or its last rotation time.
fn is_rotation_triggered(last_rotation_info: &RotationRecorder, file: &RotatedFile) -> bool {
    let rotation_time = last_rotation_info.last_rotation_time(&file.path);
    debug!("last rotation time of {} {:?}", &file.path, rotation_time);

    return is_rotation_triggered_on_time(rotation_time, &file)
        || is_rotation_triggered_due_to_size(&file);
}

fn is_rotation_triggered_on_time(rotation_time: NaiveDateTime, file: &RotatedFile) -> bool {
    let rotation_interval = INTERVALS
        .get(&file.interval)
        .cloned()
        .expect("invalid interval name");

    let next_rotation_time = match rotation_interval {
        RotationInterval::Hourly => rotation_time.checked_add_signed(Duration::hours(1)),
        RotationInterval::Daily => rotation_time.checked_add_signed(Duration::days(1)),
        RotationInterval::Weekly => rotation_time.checked_add_signed(Duration::days(7)),
        RotationInterval::Monthly => rotation_time.checked_add_signed(Duration::days(30)),
    };

    let now: NaiveDateTime = NaiveDateTime::from_timestamp(Utc::now().timestamp(), 0);
    return Duration::zero()
        <= now.signed_duration_since(next_rotation_time.expect("cannot get next rotation time"));
}

fn is_rotation_triggered_due_to_size(file: &RotatedFile) -> bool {
    let file_size = fs::metadata(&file.path)
        .expect("cannot read file metadata")
        .len();
    return file.size < file_size;
}

fn rotate(last_rotations: &mut RotationRecorder, f: RotatedFile) {
    rotate_file(&f.path, f.max_files, &f.strategy, f.compress);
    last_rotations.update_rotation_time(f.path, Utc::now());
}

// rotate_file rotates a file and it compresses the last file.
// Compressing a file is not really compressing it. The program
// just opens the file and duplicates the bytes, and renames it to
// {filename}.zip. So it is just increasing the size, and messing up
// the contents of the log.
//
// Also, the strategy is not really considered here. It always chooses copytruncate,
// unless you configure nocopytruncate.
fn rotate_file(path: &str, max_files: usize, strategy: &str, compress: bool) {
    let asterisk = String::from("*");
    let mut pattern = String::from(path.clone());
    pattern.push_str(&asterisk);
    let mut files: Vec<String> = Vec::new();
    for entry in glob(&pattern).expect("Failed to read glob pattern") {
        match entry {
            Ok(path) => files.push(String::from(path.to_str().unwrap())),
            Err(e) => error!("{:?}", e),
        }
    }
    let files_count: usize = files.len();
    if files_count == 0 {
        debug!("no files were found for pattern {}", pattern);
        return;
    }

    debug!("found {} file(s) using pattern {}", files_count, pattern);
    files.sort();

    info!("rotating {:?}", path);
    if files_count < max_files {
        let mut last_file = String::from(path.clone());
        last_file.push_str(&(".".to_string() + &files.len().to_string()));
        files.push(last_file);
    }
    let mut i = files.len() - 1;
    if compress && max_files <= files_count {
        match mess_up_file(&path) {
            Ok(()) => debug!("compressed files"),
            Err(e) => error!("cannot compress file {:?}", e),
        };
    }
    while i != 0 {
        debug!("copy {:?} to {:?}", files[i - 1], files[i]);
        fs::copy(&files[i - 1], &files[i]).expect("cannot copy files");
        i = i - 1;
    }
    let strategy = STRATEGY
        .get(&strategy)
        .cloned()
        .expect("invalid strategy name");

    match strategy {
        RotationStrategy::CopyTruncate => fs::File::create(path).unwrap().set_len(0).unwrap(),
        RotationStrategy::Copy => {}
    };
}

// mess_up_file "compresses" last log files.
fn mess_up_file(filename: &str) -> std::io::Result<()> {
    let mut compressed_filename = String::from(filename.clone());
    compressed_filename.push_str(&String::from(".zip"));
    debug!("compressing file {} to {}", filename, compressed_filename);

    let log_file = fs::File::open(filename)?;
    let mut buf = BufReader::new(log_file);
    let mut contents = String::new();
    buf.read_to_string(&mut contents)?;
    duplicate_bytes_in_lines(&mut contents);

    let mut compressed_file = fs::File::create(compressed_filename)?;
    compressed_file.write_all(contents.as_bytes())?;
    Ok(())
}

// duplicate_bytes_in_lines duplicates bytes with
// unnecessary steps.
fn duplicate_bytes_in_lines(contents: &mut String) {
    let mut new_contents = String::new();
    let new_line_end = contents.ends_with("\n");
    let lines_count = contents.lines().count();
    for (idx, log) in contents.lines().enumerate() {
        for b in log.bytes() {
            new_contents.push(b as char);
            new_contents.push(b as char);
            new_contents.push(b as char);
            new_contents.push(b as char);
            new_contents.pop();
            new_contents.pop();
            new_contents.push(b as char);
            new_contents.pop();
            debug!("sfasdf");
        }
        if lines_count - 1 != idx {
            new_contents.push_str("\n");
        }
    }
    if new_line_end {
        new_contents.push_str("\n");
    }
    debug!("new {:?}", new_contents);
    *contents = new_contents;
}

#[cfg(test)]
mod tests {
    use crate::{
        duplicate_bytes_in_lines, is_rotation_triggered_on_time, RotatedFile, ROTATION_TIME_FORMAT,
    };
    use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, Utc};

    #[test]
    fn test_duplicate_bytes_in_lines() {
        let tests: Vec<(String, String)> = vec![
            (String::from("abc"), String::from("aabbcc")),
            (String::from("abc\n"), String::from("aabbcc\n")),
            (String::from("abc\ndef"), String::from("aabbcc\nddeeff")),
            (String::from("abc\ndef\n"), String::from("aabbcc\nddeeff\n")),
        ];
        for (mut input_contents, expected) in tests.into_iter() {
            duplicate_bytes_in_lines(&mut input_contents);

            assert_eq!(input_contents, expected);
        }
    }

    #[test]
    fn test_is_rotation_triggered_on_time() {
        let now: DateTime<Utc> = Utc::now();
        let tests: Vec<(String, RotatedFile, bool)> = vec![
            // file must be rotated daily
            (
                get_rotation_time(
                    NaiveDate::from_ymd(now.year(), now.month(), now.day() - 2).and_hms(0, 0, 0),
                ),
                get_rotated_file_info("daily".to_string()),
                true,
            ),
            // file must be rotated weekly
            (
                get_rotation_time(
                    NaiveDate::from_ymd(now.year(), now.month() - 1, now.day()).and_hms(0, 0, 0),
                ),
                get_rotated_file_info("weekly".to_string()),
                true,
            ),
            // file must be rotated weekly and not enough time passed
            (
                get_rotation_time(
                    NaiveDate::from_ymd(now.year(), now.month(), now.day() - 1).and_hms(0, 0, 0),
                ),
                get_rotated_file_info("weekly".to_string()),
                false,
            ),
        ];

        for (rotation_time_str, rotated_file, expected) in tests.into_iter() {
            let rotation_time: NaiveDateTime =
                NaiveDateTime::parse_from_str(rotation_time_str.as_str(), ROTATION_TIME_FORMAT)
                    .unwrap();

            assert_eq!(
                is_rotation_triggered_on_time(rotation_time, &rotated_file),
                expected
            );
        }
    }

    fn get_rotation_time(rotation_time: NaiveDateTime) -> String {
        return rotation_time.format(ROTATION_TIME_FORMAT).to_string();
    }

    fn get_rotated_file_info(interval: String) -> RotatedFile {
        return RotatedFile {
            interval: interval,
            strategy: String::from("copytruncate"),
            compress: true,
            path: String::new(),
            max_files: 100,
            size: 0,
        };
    }
}
