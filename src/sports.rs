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

const NBA: [Sport; 1] = [Sport {
    label: "NBA",
    kalshi_series: "KXNBAGAME",
    polymarket_league: "nba",
}];

const CBB: [Sport; 1] = [Sport {
    label: "CBB",
    kalshi_series: "KXNCAAMBGAME",
    polymarket_league: "cbb",
}];

const WCBB: [Sport; 1] = [Sport {
    label: "WCBB",
    kalshi_series: "KXNCAAWBGAME",
    polymarket_league: "wcbb",
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

const ATP_SPORT: Sport = Sport {
    label: "ATP",
    kalshi_series: "KXATPMATCH",
    polymarket_league: "atp",
};
const WTA_SPORT: Sport = Sport {
    label: "WTA",
    kalshi_series: "KXWTAMATCH",
    polymarket_league: "wta",
};
const ATP: [Sport; 1] = [ATP_SPORT];
const WTA: [Sport; 1] = [WTA_SPORT];
const TENNIS: [Sport; 2] = [ATP_SPORT, WTA_SPORT];

pub fn selected_sports(selection: &str) -> Option<&'static [Sport]> {
    match selection {
        "cfb" => Some(&CFB),
        "nfl" => Some(&NFL),
        "mlb" => Some(&MLB),
        "nba" => Some(&NBA),
        "cbb" | "ncaab" | "ncaamb" => Some(&CBB),
        "wcbb" | "ncaawb" => Some(&WCBB),
        "cs2" => Some(&CS2),
        "valorant" => Some(&VALORANT),
        "dota2" => Some(&DOTA2),
        "lol" => Some(&LOL),
        "r6" => Some(&R6),
        "atp" => Some(&ATP),
        "wta" => Some(&WTA),
        "tennis" => Some(&TENNIS),
        _ => None,
    }
}

pub const SPORTS_USAGE: &str =
    "[cfb|nfl|nba|cbb|wcbb|mlb|atp|wta|tennis|cs2|valorant|dota2|lol|r6]";
pub const SCANNER_USAGE: &str =
    "[cfb|nfl|nba|cbb|wcbb|mlb|atp|wta|tennis|cs2|valorant|dota2|lol|r6|all]";

/// NBA-only aliases prevent city names from resolving to another sport's team.
pub fn nba_team_key(value: &str) -> Option<&'static str> {
    let normalized: String = value
        .to_lowercase()
        .chars()
        .filter(char::is_ascii_alphanumeric)
        .collect();
    match normalized.as_str() {
        "atlanta" | "atl" | "hawks" | "atlhawks" | "atlantahawks" => Some("atlantahawks"),
        "boston" | "bos" | "celtics" | "bosceltics" | "bostonceltics" => Some("bostonceltics"),
        "brooklyn" | "bkn" | "nets" | "bknnets" | "brooklynnets" | "brk" | "brknets" => {
            Some("brooklynnets")
        }
        "charlotte" | "cha" | "hornets" | "chahornets" | "charlottehornets" => {
            Some("charlottehornets")
        }
        "chicago" | "chi" | "bulls" | "chibulls" | "chicagobulls" => Some("chicagobulls"),
        "cleveland" | "cle" | "cavaliers" | "clecavaliers" | "clevelandcavaliers" => {
            Some("clevelandcavaliers")
        }
        "dallas" | "dal" | "mavericks" | "dalmavericks" | "dallasmavericks" => {
            Some("dallasmavericks")
        }
        "denver" | "den" | "nuggets" | "dennuggets" | "denvernuggets" => Some("denvernuggets"),
        "detroit" | "det" | "pistons" | "detpistons" | "detroitpistons" => Some("detroitpistons"),
        "goldenstate" | "gsw" | "warriors" | "gswwarriors" | "goldenstatewarriors" => {
            Some("goldenstatewarriors")
        }
        "houston" | "hou" | "rockets" | "hourockets" | "houstonrockets" => Some("houstonrockets"),
        "indiana" | "ind" | "pacers" | "indpacers" | "indianapacers" => Some("indianapacers"),
        "losangelesc" | "lac" | "clippers" | "lacclippers" | "losangelesclippers"
        | "laclippers" => Some("losangelesclippers"),
        "losangelesl" | "lal" | "lakers" | "lallakers" | "losangeleslakers" | "lalakers" => {
            Some("losangeleslakers")
        }
        "memphis" | "mem" | "grizzlies" | "memgrizzlies" | "memphisgrizzlies" => {
            Some("memphisgrizzlies")
        }
        "miami" | "mia" | "heat" | "miaheat" | "miamiheat" => Some("miamiheat"),
        "milwaukee" | "mil" | "bucks" | "milbucks" | "milwaukeebucks" => Some("milwaukeebucks"),
        "minnesota" | "min" | "timberwolves" | "mintimberwolves" | "minnesotatimberwolves" => {
            Some("minnesotatimberwolves")
        }
        "neworleans" | "nop" | "pelicans" | "noppelicans" | "neworleanspelicans" | "no"
        | "nopelicans" => Some("neworleanspelicans"),
        "newyork" | "nyk" | "knicks" | "nyknicks" | "ny" | "nykknicks" | "newyorkknicks" => {
            Some("newyorkknicks")
        }
        "oklahomacity" | "okc" | "thunder" | "okcthunder" | "oklahomacitythunder" => {
            Some("oklahomacitythunder")
        }
        "orlando" | "orl" | "magic" | "orlmagic" | "orlandomagic" => Some("orlandomagic"),
        "philadelphia" | "phi" | "76ers" | "phi76ers" | "philadelphia76ers" => {
            Some("philadelphia76ers")
        }
        "phoenix" | "phx" | "suns" | "phxsuns" | "phoenixsuns" | "pho" | "phosuns" => {
            Some("phoenixsuns")
        }
        "portland"
        | "por"
        | "trailblazers"
        | "portrailblazers"
        | "portlandtrailblazers"
        | "blazers"
        | "porblazers" => Some("portlandtrailblazers"),
        "sacramento" | "sac" | "kings" | "sackings" | "sacramentokings" => Some("sacramentokings"),
        "sanantonio" | "sas" | "spurs" | "saspurs" | "sa" | "sasspurs" | "sanantoniospurs" => {
            Some("sanantoniospurs")
        }
        "toronto" | "tor" | "raptors" | "torraptors" | "torontoraptors" => Some("torontoraptors"),
        "utah" | "uta" | "jazz" | "utajazz" | "utahjazz" => Some("utahjazz"),
        "washington" | "was" | "wizards" | "waswizards" | "washingtonwizards" => {
            Some("washingtonwizards")
        }
        _ => None,
    }
}

pub fn same_nba_team(left: &str, right: &str) -> bool {
    nba_team_key(left).is_some_and(|key| Some(key) == nba_team_key(right))
}

/// School aliases are shared only within the separately discovered college leagues.
pub fn college_team_key(value: &str) -> String {
    let value = value.trim().strip_suffix(" (W)").unwrap_or(value.trim());
    let mut key: String = value
        .to_lowercase()
        .chars()
        .filter(char::is_ascii_alphanumeric)
        .collect();
    // A trailing St. means State; a leading St. means Saint.
    let lower = value.to_lowercase();
    if lower.ends_with(" st.") || lower.ends_with(" st") {
        key.truncate(key.len() - 2);
        key.push_str("state");
    }
    if value.to_lowercase().starts_with("st. ") || value.to_lowercase().starts_with("st ") {
        key = format!("saint{}", &key[2..]);
    }
    match key.as_str() {
        "kstate" => "kansasstate",
        "latech" => "louisianatech",
        "nichollsstate" => "nicholls",
        "southernuniversity" => "southern",
        "saintthomasmn" => "saintthomas",
        "nmstate" => "newmexicostate",
        "universityatalbany" => "ualbany",
        _ => &key,
    }
    .to_owned()
}

pub fn same_college_team(left: &str, right: &str) -> bool {
    let left = college_team_key(left);
    !left.is_empty() && left == college_team_key(right)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn college_leagues_and_school_aliases_are_distinct() {
        assert_eq!(
            selected_sports("cbb").unwrap()[0].kalshi_series,
            "KXNCAAMBGAME"
        );
        assert_eq!(
            selected_sports("wcbb").unwrap()[0].kalshi_series,
            "KXNCAAWBGAME"
        );
        assert_eq!(selected_sports("ncaab"), selected_sports("cbb"));
        assert_eq!(selected_sports("ncaawb"), selected_sports("wcbb"));
        for (a, b) in [
            ("Alcorn St.", "Alcorn State"),
            ("Ohio St.", "Ohio State"),
            ("St. John's", "Saint Johns"),
            ("Kansas St.", "K-State (W)"),
            ("St. Thomas", "St. Thomas (MN) (W)"),
            ("Southern University", "Southern (W)"),
            ("Louisiana Tech", "LA Tech"),
            ("University at Albany", "UAlbany (W)"),
        ] {
            assert!(same_college_team(a, b), "{a} / {b}");
        }
        for (a, b) in [
            ("Michigan", "Michigan State"),
            ("Kansas", "Kansas State"),
            ("Miami (FL)", "Miami (OH)"),
            ("Saint Francis (PA)", "Saint Francis (NY)"),
            ("USC", "USC Upstate"),
            ("Stanford", "Samford"),
            ("", ""),
            ("Forest", "Fore State"),
        ] {
            assert!(!same_college_team(a, b), "{a} / {b}");
        }
    }

    #[test]
    fn nba_aliases_cover_all_teams_without_city_collisions() {
        let sport = selected_sports("nba").unwrap()[0];
        assert_eq!(sport.kalshi_series, "KXNBAGAME");
        assert_eq!(sport.polymarket_league, "nba");
        let teams = [
            ("Atlanta", "ATL Hawks"),
            ("Boston", "BOS Celtics"),
            ("Brooklyn", "BKN Nets"),
            ("Charlotte", "CHA Hornets"),
            ("Chicago", "CHI Bulls"),
            ("Cleveland", "CLE Cavaliers"),
            ("Dallas", "DAL Mavericks"),
            ("Denver", "DEN Nuggets"),
            ("Detroit", "DET Pistons"),
            ("Golden State", "GSW Warriors"),
            ("Houston", "HOU Rockets"),
            ("Indiana", "IND Pacers"),
            ("Los Angeles C", "LA Clippers"),
            ("Los Angeles L", "LAL Lakers"),
            ("Memphis", "MEM Grizzlies"),
            ("Miami", "MIA Heat"),
            ("Milwaukee", "MIL Bucks"),
            ("Minnesota", "MIN Timberwolves"),
            ("New Orleans", "NO Pelicans"),
            ("New York", "NY Knicks"),
            ("Oklahoma City", "OKC Thunder"),
            ("Orlando", "ORL Magic"),
            ("Philadelphia", "PHI 76ers"),
            ("Phoenix", "PHX Suns"),
            ("Portland", "POR Trail Blazers"),
            ("Sacramento", "SAC Kings"),
            ("San Antonio", "SA Spurs"),
            ("Toronto", "TOR Raptors"),
            ("Utah", "UTA Jazz"),
            ("Washington", "WAS Wizards"),
        ];
        for (i, (city, alias)) in teams.iter().enumerate() {
            assert!(same_nba_team(city, alias), "{city} / {alias}");
            for (j, (_, other)) in teams.iter().enumerate() {
                assert_eq!(same_nba_team(city, other), i == j, "{city} / {other}");
            }
        }
        for ambiguous in ["Los Angeles", "LA", "New York Nets", "", "Chicago Bears"] {
            assert!(nba_team_key(ambiguous).is_none(), "{ambiguous}");
        }
    }

    #[test]
    fn tennis_tours_can_run_separately_or_together() {
        let tennis = selected_sports("tennis").expect("tennis is supported");
        assert_eq!(tennis.len(), 2);
        assert_eq!(tennis[0].kalshi_series, "KXATPMATCH");
        assert_eq!(tennis[1].kalshi_series, "KXWTAMATCH");
        assert_eq!(selected_sports("atp"), Some(&tennis[..1]));
        assert_eq!(selected_sports("wta"), Some(&tennis[1..]));
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
