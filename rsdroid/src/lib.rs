use jni::objects::JClass;
use jni::objects::JString;
use jni::sys::jstring;
use jni::JNIEnv;

fn transform_string(s: String) -> String {
    format!("hello {}", s)
}

#[no_mangle]
pub unsafe extern "C" fn Java_net_ankiweb_rsdroid_RSDroid_request(
    env: JNIEnv,
    _: JClass,
    java_pattern: JString,
) -> jstring {
    let input_str: String = env
        .get_string(java_pattern)
        .expect("invalid pattern")
        .into();

    let output_str = transform_string(input_str);

    let output = env
        .new_string(output_str)
        .expect("failed to convert string");

    output.into_inner()
}
