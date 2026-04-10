use clap::Parser;
use rand::Rng;
use std::{
    collections::HashSet,
    fs::File,
    io::{BufWriter, Write},
};

/// UK-style vehicle registration plate format: AB12 CDE
/// Area codes (first 2 letters) use a restricted set matching real DVLA area codes.
/// Age identifiers (2 digits) use valid UK plate year/half-year codes.
/// Random suffix (3 letters) excludes I, Q and Z as per DVLA rules.

const AREA_LETTERS: &[u8] = b"ABCDEFGHJKLMNOPRSTUVWXY"; // no I, Q, Z
const SUFFIX_LETTERS: &[u8] = b"ABCDEFGHJKLMNOPRSTUVWXY"; // no I, Q, Z

/// Valid UK age identifier digits (year/half codes 02..24 and 52..74)
fn valid_age_identifiers() -> Vec<u8> {
    let mut ids = Vec::new();
    // March registrations: 02-24 (representing year 2002-2024)
    for y in 2..=24u8 {
        ids.push(y);
    }
    // September registrations: 52-74
    for y in 52..=74u8 {
        ids.push(y);
    }
    ids
}

fn random_plate(rng: &mut impl Rng, age_ids: &[u8]) -> String {
    let a1 = AREA_LETTERS[rng.random_range(0..AREA_LETTERS.len())] as char;
    let a2 = AREA_LETTERS[rng.random_range(0..AREA_LETTERS.len())] as char;

    let age = age_ids[rng.random_range(0..age_ids.len())];

    let s1 = SUFFIX_LETTERS[rng.random_range(0..SUFFIX_LETTERS.len())] as char;
    let s2 = SUFFIX_LETTERS[rng.random_range(0..SUFFIX_LETTERS.len())] as char;
    let s3 = SUFFIX_LETTERS[rng.random_range(0..SUFFIX_LETTERS.len())] as char;

    format!("{}{}{:02} {}{}{}", a1, a2, age, s1, s2, s3)
}

/// Returns true if the plate conforms to the UK current-style format AA00 AAA.
fn is_valid_plate(plate: &str) -> bool {
    let bytes = plate.as_bytes();
    if bytes.len() != 8 {
        return false;
    }
    // positions 0,1: area letters
    // position 2,3: age digits
    // position 4: space
    // positions 5,6,7: suffix letters
    let area_ok = AREA_LETTERS.contains(&bytes[0]) && AREA_LETTERS.contains(&bytes[1]);
    let digits_ok = bytes[2].is_ascii_digit() && bytes[3].is_ascii_digit();
    let space_ok = bytes[4] == b' ';
    let suffix_ok = SUFFIX_LETTERS.contains(&bytes[5])
        && SUFFIX_LETTERS.contains(&bytes[6])
        && SUFFIX_LETTERS.contains(&bytes[7]);

    if !(area_ok && digits_ok && space_ok && suffix_ok) {
        return false;
    }

    let age: u8 = (bytes[2] - b'0') * 10 + (bytes[3] - b'0');
    let age_ids = valid_age_identifiers();
    age_ids.contains(&age)
}

#[derive(Parser)]
#[command(about = "Generate unique random UK-style vehicle registration plates")]
struct Args {
    /// Number of unique plates to generate
    #[arg(short, long, default_value_t = 400_000)]
    count: usize,

    /// Output CSV file path
    #[arg(short, long, default_value = "vrm.csv")]
    output: String,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let age_ids = valid_age_identifiers();
    let mut rng = rand::rng();
    let mut seen = HashSet::with_capacity(args.count);

    let file = File::create(&args.output)?;
    let mut writer = BufWriter::new(file);
    writeln!(writer, "vrm")?;

    while seen.len() < args.count {
        let plate = random_plate(&mut rng, &age_ids);
        if seen.insert(plate.clone()) {
            writeln!(writer, "{}", plate)?;
        }
    }

    eprintln!("Written {} plates to {}", args.count, args.output);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;
    use rand_chacha::ChaCha8Rng;

    fn seeded_rng() -> ChaCha8Rng {
        ChaCha8Rng::seed_from_u64(42)
    }

    // --- valid_age_identifiers ---

    #[test]
    fn age_ids_contains_march_range() {
        let ids = valid_age_identifiers();
        for y in 2u8..=24 {
            assert!(ids.contains(&y), "missing March code {y:02}");
        }
    }

    #[test]
    fn age_ids_contains_september_range() {
        let ids = valid_age_identifiers();
        for y in 52u8..=74 {
            assert!(ids.contains(&y), "missing September code {y:02}");
        }
    }

    #[test]
    fn age_ids_excludes_invalid_codes() {
        let ids = valid_age_identifiers();
        // 00 and 01 were pre-current-style
        assert!(!ids.contains(&0));
        assert!(!ids.contains(&1));
        // 25-51 are not used
        for y in 25u8..=51 {
            assert!(!ids.contains(&y), "unexpected code {y}");
        }
        // 75-99 are not used
        for y in 75u8..=99 {
            assert!(!ids.contains(&y), "unexpected code {y}");
        }
    }

    #[test]
    fn age_ids_count_is_correct() {
        // 02-24 = 23 codes; 52-74 = 23 codes
        assert_eq!(valid_age_identifiers().len(), 46);
    }

    // --- random_plate structure ---

    #[test]
    fn random_plate_has_correct_length() {
        let age_ids = valid_age_identifiers();
        let mut rng = seeded_rng();
        for _ in 0..1_000 {
            let plate = random_plate(&mut rng, &age_ids);
            assert_eq!(plate.len(), 8, "plate '{plate}' is not 8 chars");
        }
    }

    #[test]
    fn random_plate_has_space_at_position_4() {
        let age_ids = valid_age_identifiers();
        let mut rng = seeded_rng();
        for _ in 0..1_000 {
            let plate = random_plate(&mut rng, &age_ids);
            assert_eq!(plate.as_bytes()[4], b' ', "no space in '{plate}'");
        }
    }

    #[test]
    fn random_plate_area_letters_are_valid() {
        let age_ids = valid_age_identifiers();
        let mut rng = seeded_rng();
        for _ in 0..1_000 {
            let plate = random_plate(&mut rng, &age_ids);
            let b = plate.as_bytes();
            assert!(
                AREA_LETTERS.contains(&b[0]) && AREA_LETTERS.contains(&b[1]),
                "invalid area letters in '{plate}'"
            );
        }
    }

    #[test]
    fn random_plate_age_digits_are_valid() {
        let age_ids = valid_age_identifiers();
        let mut rng = seeded_rng();
        for _ in 0..1_000 {
            let plate = random_plate(&mut rng, &age_ids);
            assert!(is_valid_plate(&plate), "invalid plate '{plate}'");
        }
    }

    #[test]
    fn random_plate_suffix_letters_are_valid() {
        let age_ids = valid_age_identifiers();
        let mut rng = seeded_rng();
        for _ in 0..1_000 {
            let plate = random_plate(&mut rng, &age_ids);
            let b = plate.as_bytes();
            assert!(
                SUFFIX_LETTERS.contains(&b[5])
                    && SUFFIX_LETTERS.contains(&b[6])
                    && SUFFIX_LETTERS.contains(&b[7]),
                "invalid suffix letters in '{plate}'"
            );
        }
    }

    #[test]
    fn random_plate_excludes_forbidden_letters() {
        let age_ids = valid_age_identifiers();
        let mut rng = seeded_rng();
        for _ in 0..10_000 {
            let plate = random_plate(&mut rng, &age_ids);
            let letter_positions = [0, 1, 5, 6, 7];
            for &pos in &letter_positions {
                let ch = plate.as_bytes()[pos];
                assert!(ch != b'I', "forbidden letter I in '{plate}'");
                assert!(ch != b'Q', "forbidden letter Q in '{plate}'");
                assert!(ch != b'Z', "forbidden letter Z in '{plate}'");
            }
        }
    }

    // --- is_valid_plate ---

    #[test]
    fn is_valid_plate_accepts_known_good_plates() {
        let good = ["AB02 ABC", "SN65 XYW", "LK24 MNP", "BX52 WVU"];
        for plate in good {
            assert!(is_valid_plate(plate), "expected valid: '{plate}'");
        }
    }

    #[test]
    fn is_valid_plate_rejects_wrong_length() {
        assert!(!is_valid_plate("AB02ABC"));   // no space, 7 chars
        assert!(!is_valid_plate("AB02 ABCD")); // 9 chars
        assert!(!is_valid_plate(""));
    }

    #[test]
    fn is_valid_plate_rejects_forbidden_letters() {
        assert!(!is_valid_plate("IB02 ABC")); // I in area
        assert!(!is_valid_plate("AQ02 ABC")); // Q in area
        assert!(!is_valid_plate("AB02 IBC")); // I in suffix
        assert!(!is_valid_plate("AB02 AQC")); // Q in suffix
        assert!(!is_valid_plate("AB02 ABZ")); // Z in suffix
    }

    #[test]
    fn is_valid_plate_rejects_invalid_age_codes() {
        assert!(!is_valid_plate("AB00 ABC")); // 00 not valid
        assert!(!is_valid_plate("AB01 ABC")); // 01 not valid
        assert!(!is_valid_plate("AB25 ABC")); // 25 not valid
        assert!(!is_valid_plate("AB51 ABC")); // 51 not valid
        assert!(!is_valid_plate("AB75 ABC")); // 75 not valid
        assert!(!is_valid_plate("AB99 ABC")); // 99 not valid
    }

    #[test]
    fn is_valid_plate_rejects_non_digit_age() {
        assert!(!is_valid_plate("ABXX ABC"));
    }

    // --- uniqueness at scale ---

    #[test]
    fn generated_plates_are_unique() {
        let age_ids = valid_age_identifiers();
        let mut rng = seeded_rng();
        let mut seen = HashSet::new();
        for _ in 0..10_000 {
            let plate = random_plate(&mut rng, &age_ids);
            assert!(seen.insert(plate.clone()), "duplicate plate '{plate}'");
        }
    }
}
