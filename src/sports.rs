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

const CS2: [Sport; 1] = [Sport {
    label: "CS2",
    kalshi_series: "KXCS2GAME",
    polymarket_league: "cs2",
}];

const VALORANT: [Sport; 1] = [Sport {
    label: "VALORANT",
    kalshi_series: "KXVALORANTGAME",
    polymarket_league: "valorant",
}];

const DOTA2: [Sport; 1] = [Sport {
    label: "DOTA2",
    kalshi_series: "KXDOTA2GAME",
    polymarket_league: "dota2",
}];

const LOL: [Sport; 1] = [Sport {
    label: "LOL",
    kalshi_series: "KXLOLGAME",
    polymarket_league: "lol",
}];

const R6: [Sport; 1] = [Sport {
    label: "R6",
    kalshi_series: "KXR6GAME",
    polymarket_league: "r6",
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
        "cs2" => Some(&CS2),
        "valorant" => Some(&VALORANT),
        "dota2" => Some(&DOTA2),
        "lol" => Some(&LOL),
        "r6" => Some(&R6),
        "tennis" => Some(&TENNIS),
        _ => None,
    }
}

pub const SPORTS_USAGE: &str = "[cfb|nfl|mlb|tennis|cs2|valorant|dota2|lol|r6]";
pub const SCANNER_USAGE: &str = "[cfb|nfl|mlb|tennis|cs2|valorant|dota2|lol|r6|all]";

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

    #[test]
    fn esports_games_use_match_winner_series() {
        for (selection, kalshi_series, polymarket_league) in [
            ("cs2", "KXCS2GAME", "cs2"),
            ("valorant", "KXVALORANTGAME", "valorant"),
            ("dota2", "KXDOTA2GAME", "dota2"),
            ("lol", "KXLOLGAME", "lol"),
            ("r6", "KXR6GAME", "r6"),
        ] {
            let sport = selected_sports(selection).expect("esports game is supported");
            assert_eq!(sport[0].kalshi_series, kalshi_series);
            assert_eq!(sport[0].polymarket_league, polymarket_league);
        }
    }
}
