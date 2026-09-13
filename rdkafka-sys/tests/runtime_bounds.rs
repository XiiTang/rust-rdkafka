use std::{ffi::{c_void,c_char},io::Write};
extern "C" {
 fn rd_gz_decompress_bounded(input:*const c_void,len:i32,out:*mut u64,maximum:u64)->*mut c_void;
 fn rd_kafka_snappy_java_uncompress_bounded(input:*const c_char,len:usize,out:*mut usize,error:*mut c_char,error_len:usize,maximum:usize)->*mut c_char;
}
#[test] fn gzip_output_limit_precedes_allocation(){
 let input=vec![b'x';1024*1024];let mut encoder=flate2::write::GzEncoder::new(Vec::new(),flate2::Compression::best());encoder.write_all(&input).unwrap();let compressed=encoder.finish().unwrap();
 // Force linkage to the same vendored engine as the embedding application.
 assert!(!unsafe{rdkafka_sys::rd_kafka_version_str()}.is_null());
 let mut length=0;let rejected=unsafe{rd_gz_decompress_bounded(compressed.as_ptr().cast(),compressed.len() as i32,&mut length,1024)};assert!(rejected.is_null());
 let mut length=0;let decoded=unsafe{rd_gz_decompress_bounded(compressed.as_ptr().cast(),compressed.len() as i32,&mut length,input.len() as u64)};assert!(!decoded.is_null());assert_eq!(length,input.len() as u64);assert_eq!(unsafe{std::slice::from_raw_parts(decoded.cast::<u8>(),length as usize)},input);unsafe{libc::free(decoded);}
}
#[test] fn snappy_java_limits_the_sum_of_chunks_before_allocation(){
 let compressed=snap::raw::Encoder::new().compress_vec(&vec![b'x';4096]).unwrap();let mut framed=Vec::new();for _ in 0..4{framed.extend_from_slice(&(compressed.len() as u32).to_be_bytes());framed.extend_from_slice(&compressed);}
 assert!(!unsafe{rdkafka_sys::rd_kafka_version_str()}.is_null());
 let mut length=0;let mut error=[0i8;128];let decoded=unsafe{rd_kafka_snappy_java_uncompress_bounded(framed.as_ptr().cast(),framed.len(),&mut length,error.as_mut_ptr(),error.len(),8192)};assert!(decoded.is_null());
 let decoded=unsafe{rd_kafka_snappy_java_uncompress_bounded(framed.as_ptr().cast(),framed.len(),&mut length,error.as_mut_ptr(),error.len(),16384)};assert!(!decoded.is_null());assert_eq!(length,16384);unsafe{libc::free(decoded.cast());}
}
