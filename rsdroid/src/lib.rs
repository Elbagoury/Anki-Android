use jni::objects::JClass;
use jni::objects::JString;
use jni::sys::jstring;
use jni::JNIEnv;
use serde_derive::{Deserialize, Serialize};
use serde_json;

use anki::sched::{local_minutes_west_for_stamp, sched_timing_today};
use std::time;

#[derive(Deserialize)]
struct TimingTodayIn {
    created_secs: i64,
    created_mins_west: i32,
    rollover_hour: i8,
}

#[derive(Serialize)]
struct TimingTodayOut {
    days_elapsed: u32,
    next_day_at: i64,
}

fn run_command(command: String, args: String) -> String {
    match command.as_str() {
        "timingToday" => {
            let i: TimingTodayIn = serde_json::from_str(&args).unwrap();
            let now_stamp = time::SystemTime::now()
                .duration_since(time::SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs();
            let now_mins_west = local_minutes_west_for_stamp(now_stamp as i64) as i32;
            let o = sched_timing_today(
                i.created_secs,
                i.created_mins_west,
                now_stamp as i64,
                now_mins_west,
                i.rollover_hour,
            );
            let out = TimingTodayOut {
                days_elapsed: o.days_elapsed,
                next_day_at: o.next_day_at,
            };
            serde_json::to_string(&out).unwrap()
        }
        _ => {
            panic!("unexpected command: {}", command);
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn Java_net_ankiweb_rsdroid_RSDroid_request(
    env: JNIEnv,
    _: JClass,
    command: JString,
    args: JString,
) -> jstring {
    let command_str: String = env.get_string(command).expect("invalid command").into();
    let args_str: String = env.get_string(args).expect("invalid args").into();

    let output_str = run_command(command_str, args_str);

    let output = env
        .new_string(output_str)
        .expect("failed to convert string");

    output.into_inner()
}
