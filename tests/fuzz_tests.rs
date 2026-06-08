use konda::parser::parse_pipeline;
use konda::storage::deserialize_payload_fuzz;
use konda::storage::key::StreamKey;
use proptest::prelude::*;

proptest! {
    // Configured to execute 500 randomized iterations per test to search for edge case violations
    #![proptest_config(ProptestConfig::with_cases(500))]

    #[test]
    fn fuzz_parser_unicode_strings(s in "\\PC*") {
        // Proptest generator for arbitrary unicode printable character blocks
        let _ = parse_pipeline(&s);
    }

    #[test]
    fn fuzz_parser_any_string(s in any::<String>()) {
        // Proptest generator for completely unrestricted raw string allocations
        let _ = parse_pipeline(&s);
    }

    #[test]
    fn fuzz_decoder_arbitrary_payloads(bytes in any::<Vec<u8>>()) {
        // Proptest generator for arbitrary raw binary payloads of varying lengths.
        // The decoder must return an error gracefully and must never panic.
        let _ = deserialize_payload_fuzz(&bytes);
    }

    /// 1. Key Boundary Protection Fuzzing:
    /// Generates unrestricted raw string allocations to assert that
    /// StreamKey::try_new never panics under any unicode boundaries or layout constraints.
    #[test]
    fn fuzz_stream_key_creation(s in any::<String>()) {
        // Assert that StreamKey::try_new never panics and returns Ok or clean KeyTooLong error
        let result: Result<StreamKey, String> = StreamKey::try_new(&s);
        if let Ok(key) = result {
            // Ensure display and string conversion does not panic
            let _ = key.to_string();
            let _ = key.as_str();
            let _ = key.as_bytes();
        }

        // Assert that UTF-8 aware safe truncation never panics
        let truncated_key = StreamKey::from_str_truncated(&s);
        let _ = truncated_key.to_string();
    }


    /// 2. Ingestion Stream Input Fuzzing:
    /// Uses printable unicode generators to simulate malformed lines hitting
    /// JSONL & CSV parsers, checking nested pipe operations, trailing quotes, etc.
    #[test]
    fn fuzz_importer_line_boundaries(s in "\\PC{0,2048}") {
        // Feed into JSON parsing
        let _ = serde_json::from_str::<serde_json::Value>(&s);

        // Feed into CSV reader tokenization to check for unclosed quotes and escaping bugs
        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_reader(s.as_bytes());
        for result in rdr.records() {
            let _ = result;
        }

        // Stress-test the pipeline parser with complex constructs like recursive pipeline operators or massive strings
        let _ = parse_pipeline(&s);
    }

    /// 3. Decoder Hardening (Structural Poisoning):
    /// Generates randomized payload bytes, but manually overrides the initial stream/key lengths
    /// and optionally MessagePack prefix markers. This forces the decoder past initial validation
    /// loops into deep deserialization paths to verify panic safety.
    #[test]
    fn fuzz_decoder_with_partially_valid_headers(mut bytes in any::<Vec<u8>>()) {
        if bytes.len() >= 10 {
            // Override first 2 bytes to represent stream length of 3 bytes
            bytes[0] = 0;
            bytes[1] = 3;
            // Write valid UTF-8 stream name bytes "str"
            bytes[2] = b's';
            bytes[3] = b't';
            bytes[4] = b'r';

            // Override next 2 bytes for key length of 3 bytes
            bytes[5] = 0;
            bytes[6] = 3;
            // Write valid UTF-8 key bytes "key"
            bytes[7] = b'k';
            bytes[8] = b'e';
            bytes[9] = b'y';

            // Seeding value bytes with valid/poisoned MessagePack markers
            if bytes.len() > 10 {
                let marker = match bytes.len() % 6 {
                    0 => 0xc0, // Nil
                    1 => 0xc2, // False
                    2 => 0xc3, // True
                    3 => 0x90, // Fixarray (0 elements)
                    4 => 0xa0, // Fixstr (0 elements)
                    _ => 0xcc, // Positive fixint
                };
                bytes[10] = marker;
            }
        }

        let _ = deserialize_payload_fuzz(&bytes);
    }
}
