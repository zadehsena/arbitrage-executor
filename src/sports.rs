//! Supported cross-venue sports and their venue-native identifiers.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Sport {
    pub label: &'static str,
    pub kalshi_series: &'static str,
    pub polymarket_league: &'static str,
}

const CFB: [Sport; 1] = [Sport {
    label: "CFB",
    kalshi_series: "KXNCAAFGAME",
    polymarket_league: "cfb",
}];

const NFL: [Sport; 1] = [Sport {
    label: "NFL",
    kalshi_series: "KXNFLGAME",
    polymarket_league: "nfl",
}];

const MLB: [Sport; 1] = [Sport {
    label: "MLB",
    kalshi_series: "KXMLBGAME",
    polymarket_league: "mlb",
}];

// Tennis is divided by tour at both venues. The `tennis` selection intentionally
// includes both so users do not need to run separate ATP and WTA scanners.
const TENNIS: [Sport; 2] = [
    Sport {
        label: "ATP",
        kalshi_series: "KXATPMATCH",
        polymarket_league: "atp",
    },
    Sport {
        label: "WTA",
        kalshi_series: "KXWTAMATCH",
        polymarket_league: "wta",
    },
];

pub fn selected_sports(selection: &str) -> Option<&'static [Sport]> {
    match selection {
        "cfb" => Some(&CFB),
        "nfl" => Some(&NFL),
        "mlb" => Some(&MLB),
        "tennis" => Some(&TENNIS),
        _ => None,
    }
}

pub const SPORTS_USAGE: &str = "[cfb|nfl|mlb|tennis]";
pub const SCANNER_USAGE: &str = "[cfb|nfl|mlb|tennis|all]";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tennis_includes_both_tours() {
        let tennis = selected_sports("tennis").expect("tennis is supported");
        assert_eq!(tennis.len(), 2);
        assert_eq!(tennis[0].kalshi_series, "KXATPMATCH");
        assert_eq!(tennis[1].kalshi_series, "KXWTAMATCH");
    }

    #[test]
    fn baseball_uses_mlb_game_series() {
        let mlb = selected_sports("mlb").expect("mlb is supported");
        assert_eq!(mlb[0].kalshi_series, "KXMLBGAME");
        assert_eq!(mlb[0].polymarket_league, "mlb");
    }
}
